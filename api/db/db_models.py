#
#  Copyright 2024 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

'''
数据库模型

peewee 数据库 ORM
playhouse 数据库 ORM 相关拓展

'''

import inspect
import logging
import operator
import os
import sys
import typing
import time
from enum import Enum
from functools import wraps
import hashlib

from flask_login import UserMixin
from itsdangerous.url_safe import URLSafeTimedSerializer as Serializer
from peewee import BigIntegerField, BooleanField, CharField, CompositeKey, DateTimeField, Field, FloatField, IntegerField, Metadata, Model, TextField
from playhouse.migrate import MySQLMigrator, PostgresqlMigrator, migrate
from playhouse.pool import PooledMySQLDatabase, PooledPostgresqlDatabase

from api import settings, utils
from api.db import ParserType, SerializedType


def singleton(cls, *args, **kw):
    instances = {}

    def _singleton():
        key = str(cls) + str(os.getpid())
        if key not in instances:
            instances[key] = cls(*args, **kw)
        return instances[key]

    return _singleton


CONTINUOUS_FIELD_TYPE = {IntegerField, FloatField, DateTimeField} # 连续值类型
AUTO_DATE_TIMESTAMP_FIELD_PREFIX = {"create", "start", "end", "update", "read_access", "write_access"} # 时间戳字段前缀


''' 定义 TextField 拓展字段类型 '''
class TextFieldType(Enum):
    MYSQL = "LONGTEXT"
    POSTGRES = "TEXT"


class LongTextField(TextField):
    ''' 根据数据库类型选择 TEXT 类型映射 '''
    field_type = TextFieldType[settings.DATABASE_TYPE.upper()].value


class JSONField(LongTextField):
    ''' JSON 类型字段序列化和反序列化 '''
    default_value = {}

    def __init__(self, object_hook=None, object_pairs_hook=None, **kwargs):
        self._object_hook = object_hook  # 序列化钩子函数 str -> dict -> 打印 hook 处理的 dict 
        self._object_pairs_hook = object_pairs_hook # 输入是键值对的钩子函数
        super().__init__(**kwargs)

    def db_value(self, value):
        if value is None:
            value = self.default_value
        return utils.json_dumps(value)

    def python_value(self, value):
        if not value:
            return self.default_value
        return utils.json_loads(value, object_hook=self._object_hook, object_pairs_hook=self._object_pairs_hook)


class ListField(JSONField):
    default_value = []


class SerializedField(LongTextField):
    ''' 可序列化字段类型 '''
    def __init__(self, serialized_type=SerializedType.PICKLE, object_hook=None, object_pairs_hook=None, **kwargs):
        self._serialized_type = serialized_type
        self._object_hook = object_hook
        self._object_pairs_hook = object_pairs_hook
        super().__init__(**kwargs)

    def db_value(self, value):
        if self._serialized_type == SerializedType.PICKLE:
            return utils.serialize_b64(value, to_str=True)
        elif self._serialized_type == SerializedType.JSON:
            if value is None:
                return None
            return utils.json_dumps(value, with_type=True)
        else:
            raise ValueError(f"the serialized type {self._serialized_type} is not supported")

    def python_value(self, value):
        if self._serialized_type == SerializedType.PICKLE:
            return utils.deserialize_b64(value)
        elif self._serialized_type == SerializedType.JSON:
            if value is None:
                return {}
            return utils.json_loads(value, object_hook=self._object_hook, object_pairs_hook=self._object_pairs_hook)
        else:
            raise ValueError(f"the serialized type {self._serialized_type} is not supported")


def is_continuous_field(cls: typing.Type) -> bool:
    '''  判断字段是否是连续字段(非离散值) '''
    if cls in CONTINUOUS_FIELD_TYPE:
        return True
    for p in cls.__bases__: # __bases__负类
        if p in CONTINUOUS_FIELD_TYPE:
            return True
        elif p is not Field and p is not object: # 递归父类
            if is_continuous_field(p):
                return True
    else:
        return False


def auto_date_timestamp_field():
    ''' 自动生成各类阶段的时间戳字段 '''
    return {f"{f}_time" for f in AUTO_DATE_TIMESTAMP_FIELD_PREFIX}


def auto_date_timestamp_db_field():
    ''' 自动生成各类时间戳数据库字段规范 '''
    return {f"f_{f}_time" for f in AUTO_DATE_TIMESTAMP_FIELD_PREFIX}


def remove_field_name_prefix(field_name):
    ''' 移除字段名前缀 f_ '''
    return field_name[2:] if field_name.startswith("f_") else field_name


''' 基本模型 '''
class BaseModel(Model):
    create_time = BigIntegerField(null=True, index=True)
    create_date = DateTimeField(null=True, index=True)
    update_time = BigIntegerField(null=True, index=True)
    update_date = DateTimeField(null=True, index=True)

    def to_json(self):
        # This function is obsolete
        return self.to_dict()

    def to_dict(self):
        return self.__dict__["__data__"]

    def to_human_model_dict(self, only_primary_with: list = None):
        '''
        only_primary_with: List 去除前缀的字段，若为 None 则全部去除
        '''
        model_dict = self.__dict__["__data__"]

        if not only_primary_with:
            return {remove_field_name_prefix(k): v for k, v in model_dict.items()}

        human_model_dict = {}
        ''' 主键 '''
        for k in self._meta.primary_key.field_names:
            human_model_dict[remove_field_name_prefix(k)] = model_dict[k]
        for k in only_primary_with:
            human_model_dict[k] = model_dict[f"f_{k}"]
        return human_model_dict

    @property
    def meta(self) -> Metadata:
        return self._meta

    @classmethod
    def get_primary_keys_name(cls):
        ''' 获得复合主键字段名称 '''
        return cls._meta.primary_key.field_names if isinstance(cls._meta.primary_key, CompositeKey) else [cls._meta.primary_key.name]

    @classmethod
    def getter_by(cls, attr):
        return operator.attrgetter(attr)(cls)  # operator.attrgetter(attr) 获取一个可调用函数，调用返回对象的 attr 属性值，可以是多个属性

    @classmethod
    def query(cls, reverse=None, order_by=None, **kwargs):
        ''' 
        
        表查询
        :param reverse: 是否倒序
        :param order_by: 排序字段
        :param kwargs: 查询条件

        '''
        filters = []
        for f_n, f_v in kwargs.items():
            attr_name = "%s" % f_n
            if not hasattr(cls, attr_name) or f_v is None:
                continue
            if type(f_v) in {list, set}:
                f_v = list(f_v)
                # 连续字段类型
                if is_continuous_field(type(getattr(cls, attr_name))):
                    # 处理特殊时间段的时间戳范围字段
                    if len(f_v) == 2:
                        for i, v in enumerate(f_v):
                            if isinstance(v, str) and f_n in auto_date_timestamp_field():
                                # time type: %Y-%m-%d %H:%M:%S
                                f_v[i] = utils.date_string_to_timestamp(v)
                        lt_value = f_v[0]
                        gt_value = f_v[1]
                        if lt_value is not None and gt_value is not None:
                            filters.append(cls.getter_by(attr_name).between(lt_value, gt_value))
                        elif lt_value is not None:
                            filters.append(operator.attrgetter(attr_name)(cls) >= lt_value)
                        elif gt_value is not None:
                            filters.append(operator.attrgetter(attr_name)(cls) <= gt_value)
                # 非连续数值类型
                else:
                    filters.append(operator.attrgetter(attr_name)(cls) << f_v)
            # 非序列类型
            else:
                filters.append(operator.attrgetter(attr_name)(cls) == f_v)
        if filters:
            query_records = cls.select().where(*filters)
            if reverse is not None:
                if not order_by or not hasattr(cls, f"{order_by}"):
                    # 默认排序字段
                    order_by = "create_time"
                if reverse is True:
                    query_records = query_records.order_by(cls.getter_by(f"{order_by}").desc())
                elif reverse is False:
                    query_records = query_records.order_by(cls.getter_by(f"{order_by}").asc())
            return [query_record for query_record in query_records]
        else:
            return []

    @classmethod
    def insert(cls, __data=None, **insert):
        ''' 插入数据,并在数据中添加创建时间字段和数据 '''
        if isinstance(__data, dict) and __data:
            __data[cls._meta.combined["create_time"]] = utils.current_timestamp()
        if insert:
            insert["create_time"] = utils.current_timestamp()

        return super().insert(__data, **insert)

    # update and insert will call this method
    @classmethod
    def _normalize_data(cls, data, kwargs):
        ''' insert和update都使用这个接口，将 '''
        normalized = super()._normalize_data(data, kwargs)
        if not normalized:
            return {}

        normalized[cls._meta.combined["update_time"]] = utils.current_timestamp()

        for f_n in AUTO_DATE_TIMESTAMP_FIELD_PREFIX:
            if {f"{f_n}_time", f"{f_n}_date"}.issubset(cls._meta.combined.keys()) and cls._meta.combined[f"{f_n}_time"] in normalized and normalized[cls._meta.combined[f"{f_n}_time"]] is not None:
                normalized[cls._meta.combined[f"{f_n}_date"]] = utils.timestamp_to_date(normalized[cls._meta.combined[f"{f_n}_time"]])

        return normalized


class JsonSerializedField(SerializedField):
    def __init__(self, object_hook=utils.from_dict_hook, object_pairs_hook=None, **kwargs):
        super(JsonSerializedField, self).__init__(serialized_type=SerializedType.JSON, object_hook=object_hook, object_pairs_hook=object_pairs_hook, **kwargs)


class PooledDatabase(Enum):
    ''' 数据库连接池 '''
    MYSQL = PooledMySQLDatabase
    POSTGRES = PooledPostgresqlDatabase


class DatabaseMigrator(Enum):
    ''' 数据库迁移 '''
    MYSQL = MySQLMigrator
    POSTGRES = PostgresqlMigrator


@singleton
class BaseDataBase:
    ''' 单例模式根据 config 初始化数据库连接池 '''
    def __init__(self):
        database_config = settings.DATABASE.copy()
        db_name = database_config.pop("name")
        self.database_connection = PooledDatabase[settings.DATABASE_TYPE.upper()].value(db_name, **database_config) # .value 取枚举的值 数据库连接池，并实例化
        logging.info("init database on cluster mode successfully")


def with_retry(max_retries=3, retry_delay=1.0):
    """Decorator: Add retry mechanism to database operations
    
    Args:
        max_retries (int): maximum number of retries
        retry_delay (float): initial retry delay (seconds), will increase exponentially
        
    Returns:
        decorated function
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for retry in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    # get self and method name for logging
                    self_obj = args[0] if args else None
                    func_name = func.__name__
                    lock_name = getattr(self_obj, 'lock_name', 'unknown') if self_obj else 'unknown'
                    
                    if retry < max_retries - 1:
                        current_delay = retry_delay * (2 ** retry) # 2 ** n * retry_delay 递增重试
                        logging.warning(f"{func_name} {lock_name} failed: {str(e)}, retrying ({retry+1}/{max_retries})")
                        time.sleep(current_delay)
                    else:
                        logging.error(f"{func_name} {lock_name} failed after all attempts: {str(e)}")
            
            if last_exception:
                raise last_exception
            return False
        return wrapper
    return decorator


class PostgresDatabaseLock:
    ''' postgres 咨询锁，默认会话级别 pg_try_advisory_xact_lock 是事务级别 '''
    def __init__(self, lock_name, timeout=10, db=None):
        self.lock_name = lock_name
        self.lock_id = int(hashlib.md5(lock_name.encode()).hexdigest(), 16) % (2**31-1) # int(a , 16) 16进制转10进制
        self.timeout = int(timeout)
        self.db = db if db else DB

    @with_retry(max_retries=3, retry_delay=1.0)
    def lock(self):
        cursor = self.db.execute_sql("SELECT pg_try_advisory_lock(%s)", (self.lock_id,))
        ret = cursor.fetchone()
        if ret[0] == 0:
            raise Exception(f"acquire postgres lock {self.lock_name} timeout")
        elif ret[0] == 1:
            return True
        else:
            raise Exception(f"failed to acquire lock {self.lock_name}")

    @with_retry(max_retries=3, retry_delay=1.0)
    def unlock(self):
        cursor = self.db.execute_sql("SELECT pg_advisory_unlock(%s)", (self.lock_id,))
        ret = cursor.fetchone()
        if ret[0] == 0:
            raise Exception(f"postgres lock {self.lock_name} was not established by this thread")
        elif ret[0] == 1:
            return True
        else:
            raise Exception(f"postgres lock {self.lock_name} does not exist")

    def __enter__(self):
        if isinstance(self.db, PooledPostgresqlDatabase):
            self.lock()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if isinstance(self.db, PooledPostgresqlDatabase):
            self.unlock()

    def __call__(self, func):
        ''' 锁装饰器 '''
        @wraps(func)
        def magic(*args, **kwargs):
            with self:
                return func(*args, **kwargs)

        return magic


class MysqlDatabaseLock:
    ''' mysql 用户级咨询锁 '''
    def __init__(self, lock_name, timeout=10, db=None):
        self.lock_name = lock_name
        self.timeout = int(timeout)
        self.db = db if db else DB

    @with_retry(max_retries=3, retry_delay=1.0)
    def lock(self):
        # SQL parameters only support %s format placeholders
        cursor = self.db.execute_sql("SELECT GET_LOCK(%s, %s)", (self.lock_name, self.timeout))
        ret = cursor.fetchone()
        if ret[0] == 0:
            raise Exception(f"acquire mysql lock {self.lock_name} timeout")
        elif ret[0] == 1:
            return True
        else:
            raise Exception(f"failed to acquire lock {self.lock_name}")

    @with_retry(max_retries=3, retry_delay=1.0)
    def unlock(self):
        cursor = self.db.execute_sql("SELECT RELEASE_LOCK(%s)", (self.lock_name,))
        ret = cursor.fetchone()
        if ret[0] == 0:
            raise Exception(f"mysql lock {self.lock_name} was not established by this thread")
        elif ret[0] == 1:
            return True
        else:
            raise Exception(f"mysql lock {self.lock_name} does not exist")

    def __enter__(self):
        if isinstance(self.db, PooledMySQLDatabase):
            self.lock()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if isinstance(self.db, PooledMySQLDatabase):
            self.unlock()

    def __call__(self, func):
        @wraps(func)
        def magic(*args, **kwargs):
            with self:
                return func(*args, **kwargs)

        return magic


class DatabaseLock(Enum):
    MYSQL = MysqlDatabaseLock
    POSTGRES = PostgresDatabaseLock

''' 创建数据库池连接实例和锁 '''
DB = BaseDataBase().database_connection
DB.lock = DatabaseLock[settings.DATABASE_TYPE.upper()].value


def close_connection():
    try:
        if DB:
            DB.close_stale(age=30) # 关闭所有过时的连接
    except Exception as e:
        logging.exception(e)


class DataBaseModel(BaseModel):
    ''' 使用 Meta 绑定数据库连接实例 '''
    class Meta:
        database = DB


@DB.connection_context()
def init_database_tables(alter_fields=[]):
    ''' 初始化数据库 '''
    members = inspect.getmembers(sys.modules[__name__], inspect.isclass) # 获取当前模块的类 例如 [('Test', <class '__main__.Test'>)]
    table_objs = []
    create_failed_list = []
    for name, obj in members:
        ''' 创建所有派生类 '''
        if obj != DataBaseModel and issubclass(obj, DataBaseModel):
            table_objs.append(obj)

            if not obj.table_exists():
                logging.debug(f"start create table {obj.__name__}")
                try:
                    obj.create_table()
                    logging.debug(f"create table success: {obj.__name__}")
                except Exception as e:
                    logging.exception(e)
                    create_failed_list.append(obj.__name__)
            else:
                logging.debug(f"table {obj.__name__} already exists, skip creation.")

    if create_failed_list:
        logging.error(f"create tables failed: {create_failed_list}")
        raise Exception(f"create tables failed: {create_failed_list}")
    migrate_db()


def fill_db_model_object(model_object, human_model_dict):
    ''' 使用字典填充模型对象  '''
    for k, v in human_model_dict.items():
        attr_name = "%s" % k
        if hasattr(model_object.__class__, attr_name):
            setattr(model_object, attr_name, v)
    return model_object


class User(DataBaseModel, UserMixin):
    '''
    用户表 user

    id: 唯一id
    access_token: 登录凭证
    nickname: 用户昵称
    email: 邮箱
    avatar: 头像
    language: 语言
    color_scheme: 背景颜色
    timezone: 时区
    last_login_time: 最后登录时间
    is_authenticated: 认证状态
    is_active: 活动状态
    is_anonymous: 匿名状态
    login_channel: 登录渠道
    status: 状态
    is_superuser: 超级用户
    '''
    id = CharField(max_length=32, primary_key=True)
    access_token = CharField(max_length=255, null=True, index=True)
    nickname = CharField(max_length=100, null=False, help_text="nicky name", index=True)
    password = CharField(max_length=255, null=True, help_text="password", index=True)
    email = CharField(max_length=255, null=False, help_text="email", index=True)
    avatar = TextField(null=True, help_text="avatar base64 string")
    language = CharField(max_length=32, null=True, help_text="English|Chinese", default="Chinese" if "zh_CN" in os.getenv("LANG", "") else "English", index=True)
    color_schema = CharField(max_length=32, null=True, help_text="Bright|Dark", default="Bright", index=True)
    timezone = CharField(max_length=64, null=True, help_text="Timezone", default="UTC+8\tAsia/Shanghai", index=True)
    last_login_time = DateTimeField(null=True, index=True)
    is_authenticated = CharField(max_length=1, null=False, default="1", index=True)
    is_active = CharField(max_length=1, null=False, default="1", index=True)
    is_anonymous = CharField(max_length=1, null=False, default="0", index=True)
    login_channel = CharField(null=True, help_text="from which user login", index=True)
    status = CharField(max_length=1, null=True, help_text="is it validate(0: wasted, 1: validate)", default="1", index=True)
    is_superuser = BooleanField(null=True, help_text="is root", default=False, index=True)

    def __str__(self):
        return self.email

    def get_id(self):
        jwt = Serializer(secret_key=settings.SECRET_KEY)
        return jwt.dumps(str(self.access_token))

    class Meta:
        db_table = "user"


class Tenant(DataBaseModel):
    '''
    租户表 tenant

    id: 唯一标识符
    name: 租户名称
    public_key: 公钥
    llm_id: LLM ID
    embd_id: embed ID
    asr_id: asr 模型id
    img2txt_id: 图片转文本模型id
    rerank_id: rerank 模型id
    tts_id: tts 模型id
    parser_id: parser 模型id
    credits: 剩余额度
    status: 状态
    '''
    id = CharField(max_length=32, primary_key=True)
    name = CharField(max_length=100, null=True, help_text="Tenant name", index=True)
    public_key = CharField(max_length=255, null=True, index=True)
    llm_id = CharField(max_length=128, null=False, help_text="default llm ID", index=True)
    embd_id = CharField(max_length=128, null=False, help_text="default embedding model ID", index=True)
    asr_id = CharField(max_length=128, null=False, help_text="default ASR model ID", index=True)
    img2txt_id = CharField(max_length=128, null=False, help_text="default image to text model ID", index=True)
    rerank_id = CharField(max_length=128, null=False, help_text="default rerank model ID", index=True)
    tts_id = CharField(max_length=256, null=True, help_text="default tts model ID", index=True)
    parser_ids = CharField(max_length=256, null=False, help_text="document processors", index=True)
    credit = IntegerField(default=512, index=True)
    status = CharField(max_length=1, null=True, help_text="is it validate(0: wasted, 1: validate)", default="1", index=True)

    class Meta:
        db_table = "tenant"


class UserTenant(DataBaseModel):
    '''
    用户租户关联表 user_tenant

    id: 唯一标识
    user_id: 用户id
    tenant_id: 租户id
    role: 用户租户角色
    invite_by: 邀请人id
    status:状态

    '''
    id = CharField(max_length=32, primary_key=True)
    user_id = CharField(max_length=32, null=False, index=True)
    tenant_id = CharField(max_length=32, null=False, index=True)
    role = CharField(max_length=32, null=False, help_text="UserTenantRole", index=True)
    invited_by = CharField(max_length=32, null=False, index=True)
    status = CharField(max_length=1, null=True, help_text="is it validate(0: wasted, 1: validate)", default="1", index=True)

    class Meta:
        db_table = "user_tenant"


class InvitationCode(DataBaseModel):
    '''
    邀请码 invitation_code

    id: 唯一标识
    code: 邀请码
    visit_time: 访问时间
    user_id: 邀请码对应的用户id
    tenant_id: 邀请码对应的租户id
    status: 邀请码状态
    '''
    id = CharField(max_length=32, primary_key=True)
    code = CharField(max_length=32, null=False, index=True)
    visit_time = DateTimeField(null=True, index=True)
    user_id = CharField(max_length=32, null=True, index=True)
    tenant_id = CharField(max_length=32, null=True, index=True)
    status = CharField(max_length=1, null=True, help_text="is it validate(0: wasted, 1: validate)", default="1", index=True)

    class Meta:
        db_table = "invitation_code"


class LLMFactories(DataBaseModel):
    '''
    模型供应商注册 llm_factories

    name: 模型名称
    logo: logo_base64
    tags: 类型
    status: 状态
    '''
    name = CharField(max_length=128, null=False, help_text="LLM factory name", primary_key=True)
    logo = TextField(null=True, help_text="llm logo base64")
    tags = CharField(max_length=255, null=False, help_text="LLM, Text Embedding, Image2Text, ASR", index=True)
    status = CharField(max_length=1, null=True, help_text="is it validate(0: wasted, 1: validate)", default="1", index=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = "llm_factories"


class LLM(DataBaseModel):
    '''
    模型注册 llm

    llm_name: 模型名称
    model_type: 模型类型
    fid: 模型工厂id
    max_tokens: 最大token数
    tags: 模型类型
    is_tools: 是否为支持工具
    status: 状态
    '''
    # LLMs dictionary
    llm_name = CharField(max_length=128, null=False, help_text="LLM name", index=True)
    model_type = CharField(max_length=128, null=False, help_text="LLM, Text Embedding, Image2Text, ASR", index=True)
    fid = CharField(max_length=128, null=False, help_text="LLM factory id", index=True)
    max_tokens = IntegerField(default=0)

    tags = CharField(max_length=255, null=False, help_text="LLM, Text Embedding, Image2Text, Chat, 32k...", index=True)
    is_tools =  BooleanField(null=False, help_text="support tools", default=False)
    status = CharField(max_length=1, null=True, help_text="is it validate(0: wasted, 1: validate)", default="1", index=True)

    def __str__(self):
        return self.llm_name

    class Meta:
        primary_key = CompositeKey("fid", "llm_name")
        db_table = "llm"


class TenantLLM(DataBaseModel):
    '''
    租户提供的模型 tenat_llm

    tenant_id: 租户 id
    llm_factory: 模型供应商
    model_type: 模型类型
    llm_name: 模型名称
    api_key: key
    api_base: base url
    max_tokens: 最大 token 数
    use_count: 使用次数
    
    '''
    tenant_id = CharField(max_length=32, null=False, index=True)
    llm_factory = CharField(max_length=128, null=False, help_text="LLM factory name", index=True)
    model_type = CharField(max_length=128, null=True, help_text="LLM, Text Embedding, Image2Text, ASR", index=True)
    llm_name = CharField(max_length=128, null=True, help_text="LLM name", default="", index=True)
    api_key = CharField(max_length=2048, null=True, help_text="API KEY", index=True)
    api_base = CharField(max_length=255, null=True, help_text="API Base")
    max_tokens = IntegerField(default=8192, index=True)
    used_tokens = IntegerField(default=0, index=True)

    def __str__(self):
        return self.llm_name

    class Meta:
        db_table = "tenant_llm"
        primary_key = CompositeKey("tenant_id", "llm_factory", "llm_name")


class TenantLangfuse(DataBaseModel):
    '''
    租户模型 Langfuse 观测  tenant_langfuse

    tenant_id: 租户 ID
    secret_key: Langfuse 密钥
    public_key: Langfuse 公钥
    host: Langfuse 域名
    '''
    tenant_id = CharField(max_length=32, null=False, primary_key=True)
    secret_key = CharField(max_length=2048, null=False, help_text="SECRET KEY", index=True)
    public_key = CharField(max_length=2048, null=False, help_text="PUBLIC KEY", index=True)
    host = CharField(max_length=128, null=False, help_text="HOST", index=True)

    def __str__(self):
        return "Langfuse host" + self.host

    class Meta:
        db_table = "tenant_langfuse"


class Knowledgebase(DataBaseModel):
    '''
    知识库  knowledgebase

    id: 唯一标识
    avatar: b64 头像
    namme: 知识库名称
    language: 语言
    description: 描述
    embd_id: embedding model id
    permission: 权限 个人或者团队
    created_by: 创建者
    doc_num: 文档数量
    token_num: token数量
    chunk_num: 分块数量
    similarity_threshold: 相似度阈值
    vector_similarity_weight: 向量相似度权重

    parser_id: 解析器 id
    parser_config: 解析器配置
    pagerank: 页面排名
    status: 状态
    '''
    id = CharField(max_length=32, primary_key=True)
    avatar = TextField(null=True, help_text="avatar base64 string")
    tenant_id = CharField(max_length=32, null=False, index=True)
    name = CharField(max_length=128, null=False, help_text="KB name", index=True)
    language = CharField(max_length=32, null=True, default="Chinese" if "zh_CN" in os.getenv("LANG", "") else "English", help_text="English|Chinese", index=True)
    description = TextField(null=True, help_text="KB description")
    embd_id = CharField(max_length=128, null=False, help_text="default embedding model ID", index=True)
    permission = CharField(max_length=16, null=False, help_text="me|team", default="me", index=True)
    created_by = CharField(max_length=32, null=False, index=True)
    doc_num = IntegerField(default=0, index=True)
    token_num = IntegerField(default=0, index=True)
    chunk_num = IntegerField(default=0, index=True)
    similarity_threshold = FloatField(default=0.2, index=True)
    vector_similarity_weight = FloatField(default=0.3, index=True)

    parser_id = CharField(max_length=32, null=False, help_text="default parser ID", default=ParserType.NAIVE.value, index=True)
    parser_config = JSONField(null=False, default={"pages": [[1, 1000000]]})
    pagerank = IntegerField(default=0, index=False)
    status = CharField(max_length=1, null=True, help_text="is it validate(0: wasted, 1: validate)", default="1", index=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = "knowledgebase"


class Document(DataBaseModel):
    '''
    文档表 document

    id: 文档唯一标识 id
    thumbnail: 文档缩略图 base64
    kb_id: 关联知识库 id
    parser_id: 文档解析器 id
    parser_config: 文档解析器配置
    source_type: 文档来源 默认为 local
    type: 文档拓展名
    create_by: 文档创建者
    name: 文档名称
    location: 文档存储位置
    size: 文档大小
    token_num: 文档 token 数
    chunk_num: 文档 chunk 数
    progress: 文档解析进度
    progress_msg: 文档解析进度信息
    progress_begin_at: 文档解析开始时间
    process_duation: 文档解析耗时
    meta_fields: 文档元数据

    run: 文档运行状态 1 表示正在运行，2表示被取消
    status: 状态
    '''
    id = CharField(max_length=32, primary_key=True)
    thumbnail = TextField(null=True, help_text="thumbnail base64 string")
    kb_id = CharField(max_length=256, null=False, index=True)
    parser_id = CharField(max_length=32, null=False, help_text="default parser ID", index=True)
    parser_config = JSONField(null=False, default={"pages": [[1, 1000000]]})
    source_type = CharField(max_length=128, null=False, default="local", help_text="where dose this document come from", index=True)
    type = CharField(max_length=32, null=False, help_text="file extension", index=True)
    created_by = CharField(max_length=32, null=False, help_text="who created it", index=True)
    name = CharField(max_length=255, null=True, help_text="file name", index=True)
    location = CharField(max_length=255, null=True, help_text="where dose it store", index=True)
    size = IntegerField(default=0, index=True)
    token_num = IntegerField(default=0, index=True)
    chunk_num = IntegerField(default=0, index=True)
    progress = FloatField(default=0, index=True)
    progress_msg = TextField(null=True, help_text="process message", default="")
    process_begin_at = DateTimeField(null=True, index=True)
    process_duation = FloatField(default=0)
    meta_fields = JSONField(null=True, default={})

    run = CharField(max_length=1, null=True, help_text="start to run processing or cancel.(1: run it; 2: cancel)", default="0", index=True)
    status = CharField(max_length=1, null=True, help_text="is it validate(0: wasted, 1: validate)", default="1", index=True)

    class Meta:
        db_table = "document"


class File(DataBaseModel):
    '''
    文件表 file

    id: 文件唯一标识
    parent_id: 父级文件id
    tenant_id: 租户id
    create_by: 创建人
    name: 文件名
    location: 文件存储位置
    size: 文件大小
    type: 文件拓展名
    status: 文件状态
    '''
    id = CharField(max_length=32, primary_key=True)
    parent_id = CharField(max_length=32, null=False, help_text="parent folder id", index=True)
    tenant_id = CharField(max_length=32, null=False, help_text="tenant id", index=True)
    created_by = CharField(max_length=32, null=False, help_text="who created it", index=True)
    name = CharField(max_length=255, null=False, help_text="file name or folder name", index=True)
    location = CharField(max_length=255, null=True, help_text="where dose it store", index=True)
    size = IntegerField(default=0, index=True)
    type = CharField(max_length=32, null=False, help_text="file extension", index=True)
    source_type = CharField(max_length=128, null=False, default="", help_text="where dose this document come from", index=True)

    class Meta:
        db_table = "file"


class File2Document(DataBaseModel):
    '''
    文件文档关联表

    id: 唯一标识
    file_id: 文件id
    document_id: 文档id

    '''
    id = CharField(max_length=32, primary_key=True)
    file_id = CharField(max_length=32, null=True, help_text="file id", index=True)
    document_id = CharField(max_length=32, null=True, help_text="document id", index=True)

    class Meta:
        db_table = "file2document"


class Task(DataBaseModel):
    '''
    任务表

    id: 唯一标识
    doc_id: 任务处理的文档id
    from_page: 任务处理的开始页码
    to_page: 任务处理的结束页码
    task_type: 任务类型
    priority: 优先级

    begin_at: 任务开始时间
    process_duation: 任务处理时长
    
    progress: 任务进度
    progress_msg: 任务进度信息
    retry_times: 任务重试次数
    digest: 任务摘要
    chunk_id: 块ids
    '''
    id = CharField(max_length=32, primary_key=True)
    doc_id = CharField(max_length=32, null=False, index=True)
    from_page = IntegerField(default=0)
    to_page = IntegerField(default=100000000)
    task_type = CharField(max_length=32, null=False, default="")
    priority = IntegerField(default=0)

    begin_at = DateTimeField(null=True, index=True)
    process_duation = FloatField(default=0)

    progress = FloatField(default=0, index=True)
    progress_msg = TextField(null=True, help_text="process message", default="")
    retry_count = IntegerField(default=0)
    digest = TextField(null=True, help_text="task digest", default="")
    chunk_ids = LongTextField(null=True, help_text="chunk ids", default="")


class Dialog(DataBaseModel):
    '''
    会话记录 dialog
    
    id: 唯一标识符
    tenant_id: 租户id
    name: 对话应用名称
    description: 对话应用描述
    icon: 对话应用图标 base64
    language: 语言
    llm_id: 模型id

    llm_setting: 模型设置
    prompt_type: 提示词类型
    prompt_config: 提示词配置

    similarity_threshold: 相似度阈值
    vector_similarity_weight: 向量相似度权重

    top_n: 引用数量
    top_k: 粗排数量
    do_refer: 是否需要在答案中插入参考索引 默认为是
    rerank_id: 默认重排模型

    kb_ids: 关联知识库id
    status: 状态
    '''
    id = CharField(max_length=32, primary_key=True)
    tenant_id = CharField(max_length=32, null=False, index=True)
    name = CharField(max_length=255, null=True, help_text="dialog application name", index=True)
    description = TextField(null=True, help_text="Dialog description")
    icon = TextField(null=True, help_text="icon base64 string")
    language = CharField(max_length=32, null=True, default="Chinese" if "zh_CN" in os.getenv("LANG", "") else "English", help_text="English|Chinese", index=True)
    llm_id = CharField(max_length=128, null=False, help_text="default llm ID")

    llm_setting = JSONField(null=False, default={"temperature": 0.1, "top_p": 0.3, "frequency_penalty": 0.7, "presence_penalty": 0.4, "max_tokens": 512})
    prompt_type = CharField(max_length=16, null=False, default="simple", help_text="simple|advanced", index=True)
    prompt_config = JSONField(
        null=False,
        default={"system": "", "prologue": "Hi! I'm your assistant, what can I do for you?", "parameters": [], "empty_response": "Sorry! No relevant content was found in the knowledge base!"},
    )

    similarity_threshold = FloatField(default=0.2)
    vector_similarity_weight = FloatField(default=0.3)

    top_n = IntegerField(default=6)

    top_k = IntegerField(default=1024)

    do_refer = CharField(max_length=1, null=False, default="1", help_text="it needs to insert reference index into answer or not")

    rerank_id = CharField(max_length=128, null=False, help_text="default rerank model ID")

    kb_ids = JSONField(null=False, default=[])
    status = CharField(max_length=1, null=True, help_text="is it validate(0: wasted, 1: validate)", default="1", index=True)

    class Meta:
        db_table = "dialog"


class Conversation(DataBaseModel):
    '''
    对话消息记录 conversation
    
    id: 唯一标识符
    dialog_id: 会话id
    name: 会话名称
    message: 会话消息
    reference: 引用
    user_id: 用户id
    '''

    id = CharField(max_length=32, primary_key=True)
    dialog_id = CharField(max_length=32, null=False, index=True)
    name = CharField(max_length=255, null=True, help_text="converastion name", index=True)
    message = JSONField(null=True)
    reference = JSONField(null=True, default=[])
    user_id = CharField(max_length=255, null=True, help_text="user_id", index=True)

    class Meta:
        db_table = "conversation"


class APIToken(DataBaseModel):
    '''
    api token记录表 api_token
    
    tenant_id: 租户id
    token: api token
    dialog_id: 会话id
    source: 来源类型
    beta: 是否为测试
    '''

    tenant_id = CharField(max_length=32, null=False, index=True)
    token = CharField(max_length=255, null=False, index=True)
    dialog_id = CharField(max_length=32, null=True, index=True)
    source = CharField(max_length=16, null=True, help_text="none|agent|dialog", index=True)
    beta = CharField(max_length=255, null=True, index=True)

    class Meta:
        db_table = "api_token"
        primary_key = CompositeKey("tenant_id", "token")


class API4Conversation(DataBaseModel):
    '''
    工作流调用时每条记录的详细记录 api_4_conversation

    id: 唯一标识符
    dialog_id: 会话 id
    user_id: 用户 id
    message: 消息
    reference: 参考
    tokens: 使用tokens数量
    source: 会话来源
    dsl: 会话 dsl
    duration: 执行时间
    round: 轮次
    thumb_up: 用户反馈
    '''
    id = CharField(max_length=32, primary_key=True)
    dialog_id = CharField(max_length=32, null=False, index=True)
    user_id = CharField(max_length=255, null=False, help_text="user_id", index=True)
    message = JSONField(null=True)
    reference = JSONField(null=True, default=[])
    tokens = IntegerField(default=0)
    source = CharField(max_length=16, null=True, help_text="none|agent|dialog", index=True)
    dsl = JSONField(null=True, default={})
    duration = FloatField(default=0, index=True)
    round = IntegerField(default=0, index=True)
    thumb_up = IntegerField(default=0, index=True)

    class Meta:
        db_table = "api_4_conversation"


class UserCanvas(DataBaseModel):
    '''
    用户工作流 user_canvas

    id: 唯一标识
    avatar: 头像 base64
    user_id: 用户 id
    title: 画板 标题

    permission: 权限
    description: 描述
    canvas_type: 画板类型
    dsl: dsl
    
    '''

    id = CharField(max_length=32, primary_key=True)
    avatar = TextField(null=True, help_text="avatar base64 string")
    user_id = CharField(max_length=255, null=False, help_text="user_id", index=True)
    title = CharField(max_length=255, null=True, help_text="Canvas title")

    permission = CharField(max_length=16, null=False, help_text="me|team", default="me", index=True)
    description = TextField(null=True, help_text="Canvas description")
    canvas_type = CharField(max_length=32, null=True, help_text="Canvas type", index=True)
    dsl = JSONField(null=True, default={})

    class Meta:
        db_table = "user_canvas"


class CanvasTemplate(DataBaseModel):
    '''
    工作流模板 canvas_template

    id: 唯一标识符
    avatar: 模板图标
    title: 工作流
    description: 描述
    canvas_type: 工作流类型
    dsl: 工作流模板
    '''
    id = CharField(max_length=32, primary_key=True)
    avatar = TextField(null=True, help_text="avatar base64 string")
    title = CharField(max_length=255, null=True, help_text="Canvas title")

    description = TextField(null=True, help_text="Canvas description")
    canvas_type = CharField(max_length=32, null=True, help_text="Canvas type", index=True)
    dsl = JSONField(null=True, default={})

    class Meta:
        db_table = "canvas_template"


class UserCanvasVersion(DataBaseModel):
    '''
    用户工作流版本 user_canvas_version

    id: 唯一标识符
    user_canvas_id: 工作流id

    title: 标题
    description: 描述
    dsl: 工作流 dsl
    '''
    id = CharField(max_length=32, primary_key=True)
    user_canvas_id = CharField(max_length=255, null=False, help_text="user_canvas_id", index=True)

    title = CharField(max_length=255, null=True, help_text="Canvas title")
    description = TextField(null=True, help_text="Canvas description")
    dsl = JSONField(null=True, default={})

    class Meta:
        db_table = "user_canvas_version"


def migrate_db():
    migrator = DatabaseMigrator[settings.DATABASE_TYPE.upper()].value(DB)  # 获取数据库迁移器  MySQLMigrator | PostgresqlMigrator
    ''' 对各种表格添加字段 '''
    try:
        migrate(migrator.add_column("file", "source_type", CharField(max_length=128, null=False, default="", help_text="where dose this document come from", index=True)))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("tenant", "rerank_id", CharField(max_length=128, null=False, default="BAAI/bge-reranker-v2-m3", help_text="default rerank model ID")))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("dialog", "rerank_id", CharField(max_length=128, null=False, default="", help_text="default rerank model ID")))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("dialog", "top_k", IntegerField(default=1024)))
    except Exception:
        pass
    try:
        migrate(migrator.alter_column_type("tenant_llm", "api_key", CharField(max_length=2048, null=True, help_text="API KEY", index=True)))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("api_token", "source", CharField(max_length=16, null=True, help_text="none|agent|dialog", index=True)))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("tenant", "tts_id", CharField(max_length=256, null=True, help_text="default tts model ID", index=True)))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("api_4_conversation", "source", CharField(max_length=16, null=True, help_text="none|agent|dialog", index=True)))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("task", "retry_count", IntegerField(default=0)))
    except Exception:
        pass
    try:
        migrate(migrator.alter_column_type("api_token", "dialog_id", CharField(max_length=32, null=True, index=True)))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("tenant_llm", "max_tokens", IntegerField(default=8192, index=True)))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("api_4_conversation", "dsl", JSONField(null=True, default={})))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("knowledgebase", "pagerank", IntegerField(default=0, index=False)))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("api_token", "beta", CharField(max_length=255, null=True, index=True)))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("task", "digest", TextField(null=True, help_text="task digest", default="")))
    except Exception:
        pass

    try:
        migrate(migrator.add_column("task", "chunk_ids", LongTextField(null=True, help_text="chunk ids", default="")))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("conversation", "user_id", CharField(max_length=255, null=True, help_text="user_id", index=True)))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("document", "meta_fields", JSONField(null=True, default={})))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("task", "task_type", CharField(max_length=32, null=False, default="")))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("task", "priority", IntegerField(default=0)))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("user_canvas", "permission", CharField(max_length=16, null=False, help_text="me|team", default="me", index=True)))
    except Exception:
        pass
    try:
        migrate(migrator.add_column("llm", "is_tools", BooleanField(null=False, help_text="support tools", default=False)))
    except Exception:
        pass
