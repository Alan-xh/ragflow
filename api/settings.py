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
import json
import os
import secrets
from datetime import date
from enum import Enum, IntEnum

import rag.utils
import rag.utils.es_conn
import rag.utils.infinity_conn
import rag.utils.opensearch_coon
from api.constants import RAG_FLOW_SERVICE_NAME
from api.utils import decrypt_database_config, get_base_config
from api.utils.file_utils import get_project_base_directory
from graphrag import search as kg_search
from rag.nlp import search

# 轻量模式
LIGHTEN = int(os.environ.get("LIGHTEN", "0"))

# 服务基本配置
LLM = None
LLM_FACTORY = None
LLM_BASE_URL = None
CHAT_MDL = ""
EMBEDDING_MDL = ""
RERANK_MDL = ""
ASR_MDL = ""
IMAGE2TEXT_MDL = ""
API_KEY = None
PARSERS = None
HOST_IP = None
HOST_PORT = None
SECRET_KEY = None
FACTORY_LLM_INFOS = None

# 数据库配置
DATABASE_TYPE = os.getenv("DB_TYPE", "mysql")
DATABASE = decrypt_database_config(name=DATABASE_TYPE)

# authentication
AUTHENTICATION_CONF = None

# client
CLIENT_AUTHENTICATION = None
HTTP_APP_KEY = None
GITHUB_OAUTH = None
FEISHU_OAUTH = None
OAUTH_CONFIG = None
DOC_ENGINE = None
docStoreConn = None

retrievaler = None
kg_retrievaler = None

# user registration switch
REGISTER_ENABLED = 1 # 用户注册开关


# sandbox-executor-manager
SANDBOX_ENABLED = 0
SANDBOX_HOST = None

BUILTIN_EMBEDDING_MODELS = ["BAAI/bge-large-zh-v1.5@BAAI", "maidalun1020/bce-embedding-base_v1@Youdao"]

def get_or_create_secret_key():
    # 从环境变量中获取 RAGFLOW_SECRET_KEY
    secret_key = os.environ.get("RAGFLOW_SECRET_KEY")
    if secret_key and len(secret_key) >= 32:
        return secret_key
    
    # 从环境变量的 ragflow 的 secret_key 获取 RAGFLOW_SECRET_KEY
    configured_key = get_base_config(RAG_FLOW_SERVICE_NAME, {}).get("secret_key")
    if configured_key and configured_key != str(date.today()) and len(configured_key) >= 32:
        return configured_key
    
    # 随机生成一个密钥 Generate a new secure key and warn about it
    import logging
    new_key = secrets.token_hex(32)
    logging.warning(
        "SECURITY WARNING: Using auto-generated SECRET_KEY. "
        f"Generated key: {new_key}"
    )
    return new_key


def init_settings():
    global LLM, LLM_FACTORY, LLM_BASE_URL, LIGHTEN, DATABASE_TYPE, DATABASE, FACTORY_LLM_INFOS, REGISTER_ENABLED
    LIGHTEN = int(os.environ.get("LIGHTEN", "0")) # 轻量
    DATABASE_TYPE = os.getenv("DB_TYPE", "mysql") # 数据库类型
    DATABASE = decrypt_database_config(name=DATABASE_TYPE) # 数据库配置
    LLM = get_base_config("user_default_llm", {}) # 默认 LLM 配置
    LLM_DEFAULT_MODELS = LLM.get("default_models", {}) # 默认模型
    LLM_FACTORY = LLM.get("factory")
    try:
        REGISTER_ENABLED = int(os.environ.get("REGISTER_ENABLED", "1")) # 用户可否注册
    except Exception:
        pass

    try:
        ''' 导入部分 LLM 的基础信息 '''
        with open(os.path.join(get_project_base_directory(), "conf", "llm_factories.json"), "r") as f:
            FACTORY_LLM_INFOS = json.load(f)["factory_llm_infos"]
    except Exception:
        FACTORY_LLM_INFOS = []

    # 所有模型
    global CHAT_MDL, EMBEDDING_MDL, RERANK_MDL, ASR_MDL, IMAGE2TEXT_MDL
    if not LIGHTEN:
        EMBEDDING_MDL = BUILTIN_EMBEDDING_MODELS[0]

    if LLM_DEFAULT_MODELS:
        CHAT_MDL = LLM_DEFAULT_MODELS.get("chat_model", CHAT_MDL)
        EMBEDDING_MDL = LLM_DEFAULT_MODELS.get("embedding_model", EMBEDDING_MDL)
        RERANK_MDL = LLM_DEFAULT_MODELS.get("rerank_model", RERANK_MDL)
        ASR_MDL = LLM_DEFAULT_MODELS.get("asr_model", ASR_MDL)
        IMAGE2TEXT_MDL = LLM_DEFAULT_MODELS.get("image2text_model", IMAGE2TEXT_MDL)

        # factory can be specified in the config name with "@". LLM_FACTORY will be used if not specified
        CHAT_MDL = CHAT_MDL + (f"@{LLM_FACTORY}" if "@" not in CHAT_MDL and CHAT_MDL != "" else "")
        EMBEDDING_MDL = EMBEDDING_MDL + (f"@{LLM_FACTORY}" if "@" not in EMBEDDING_MDL and EMBEDDING_MDL != "" else "")
        RERANK_MDL = RERANK_MDL + (f"@{LLM_FACTORY}" if "@" not in RERANK_MDL and RERANK_MDL != "" else "")
        ASR_MDL = ASR_MDL + (f"@{LLM_FACTORY}" if "@" not in ASR_MDL and ASR_MDL != "" else "")
        IMAGE2TEXT_MDL = IMAGE2TEXT_MDL + (f"@{LLM_FACTORY}" if "@" not in IMAGE2TEXT_MDL and IMAGE2TEXT_MDL != "" else "")

    # 定义服务基础信息
    global API_KEY, PARSERS, HOST_IP, HOST_PORT, SECRET_KEY
    API_KEY = LLM.get("api_key")
    PARSERS = LLM.get(
        "parsers", "naive:General,qa:Q&A,resume:Resume,manual:Manual,table:Table,paper:Paper,book:Book,laws:Laws,presentation:Presentation,picture:Picture,one:One,audio:Audio,email:Email,tag:Tag"
    )

    HOST_IP = get_base_config(RAG_FLOW_SERVICE_NAME, {}).get("host", "127.0.0.1")
    HOST_PORT = get_base_config(RAG_FLOW_SERVICE_NAME, {}).get("http_port")

    SECRET_KEY = get_or_create_secret_key()

    # 定义认证配置
    global AUTHENTICATION_CONF, CLIENT_AUTHENTICATION, HTTP_APP_KEY, GITHUB_OAUTH, FEISHU_OAUTH, OAUTH_CONFIG
    # authentication
    AUTHENTICATION_CONF = get_base_config("authentication", {})

    # client
    CLIENT_AUTHENTICATION = AUTHENTICATION_CONF.get("client", {}).get("switch", False)
    HTTP_APP_KEY = AUTHENTICATION_CONF.get("client", {}).get("http_app_key")
    GITHUB_OAUTH = get_base_config("oauth", {}).get("github")
    FEISHU_OAUTH = get_base_config("oauth", {}).get("feishu")

    OAUTH_CONFIG = get_base_config("oauth", {})

    # 定义文档引擎
    global DOC_ENGINE, docStoreConn, retrievaler, kg_retrievaler
    DOC_ENGINE = os.environ.get("DOC_ENGINE", "elasticsearch")
    # DOC_ENGINE = os.environ.get('DOC_ENGINE', "opensearch")
    lower_case_doc_engine = DOC_ENGINE.lower()
    if lower_case_doc_engine == "elasticsearch":
        docStoreConn = rag.utils.es_conn.ESConnection()
    elif lower_case_doc_engine == "infinity":
        docStoreConn = rag.utils.infinity_conn.InfinityConnection()
    elif lower_case_doc_engine == "opensearch":
        docStoreConn = rag.utils.opensearch_coon.OSConnection()
    else:
        raise Exception(f"Not supported doc engine: {DOC_ENGINE}")

    # 实例化 nlp 和 graphrag 检索器
    retrievaler = search.Dealer(docStoreConn)
    kg_retrievaler = kg_search.KGSearch(docStoreConn)

    # 沙盒 host
    if int(os.environ.get("SANDBOX_ENABLED", "0")):
        global SANDBOX_HOST
        SANDBOX_HOST = os.environ.get("SANDBOX_HOST", "sandbox-executor-manager")


class CustomEnum(Enum):
    ''' 添加验证和取 names 和 values 的方法 '''
    @classmethod
    def valid(cls, value):
        try:
            cls(value)
            return True
        except BaseException:
            return False

    @classmethod
    def values(cls):
        return [member.value for member in cls.__members__.values()]

    @classmethod
    def names(cls):
        return [member.name for member in cls.__members__.values()]

# 常用结果码
class RetCode(IntEnum, CustomEnum):
    SUCCESS = 0
    NOT_EFFECTIVE = 10
    EXCEPTION_ERROR = 100
    ARGUMENT_ERROR = 101
    DATA_ERROR = 102
    OPERATING_ERROR = 103
    CONNECTION_ERROR = 105
    RUNNING = 106
    PERMISSION_ERROR = 108
    AUTHENTICATION_ERROR = 109
    UNAUTHORIZED = 401
    SERVER_ERROR = 500
    FORBIDDEN = 403
    NOT_FOUND = 404
