#
#  Copyright 2025 The InfiniFlow Authors. All Rights Reserved.
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

import logging
import json
import uuid

import valkey as redis
from rag import settings
from rag.utils import singleton
from valkey.lock import Lock
import trio

class RedisMsg:
    def __init__(self, consumer, queue_name, group_name, msg_id, message):
        self.__consumer = consumer
        self.__queue_name = queue_name
        self.__group_name = group_name
        self.__msg_id = msg_id
        self.__message = json.loads(message["message"])

    def ack(self):
        try:
            self.__consumer.xack(self.__queue_name, self.__group_name, self.__msg_id)
            return True
        except Exception as e:
            logging.warning("[EXCEPTION]ack" + str(self.__queue_name) + "||" + str(e))
        return False

    def get_message(self):
        return self.__message

    def get_msg_id(self):
        return self.__msg_id


@singleton
class RedisDB:
    lua_delete_if_equal = None

    ''' 获取 get键的值，如果存在且等于 ARGV[1] 则删除并返回 1'''
    LUA_DELETE_IF_EQUAL_SCRIPT = """
        local current_value = redis.call('get', KEYS[1])
        if current_value and current_value == ARGV[1] then
            redis.call('del', KEYS[1])
            return 1
        end
        return 0
    """

    def __init__(self):
        self.REDIS = None
        self.config = settings.REDIS
        self.__open__()

    def register_scripts(self) -> None:
        ''' 注册Lua脚本，获取 EVALSHA 自动处理脚本缓存，返回可调用对象 '''
        cls = self.__class__
        client = self.REDIS
        cls.lua_delete_if_equal = client.register_script(cls.LUA_DELETE_IF_EQUAL_SCRIPT) # 获取 EVALSHA 自动处理脚本缓存，返回可调用对象

    def __open__(self):
        try:
            ''' 创建 redis 连接对象，并注册脚本 '''
            self.REDIS = redis.StrictRedis(
                host=self.config["host"].split(":")[0],
                port=int(self.config.get("host", ":6379").split(":")[1]),
                db=int(self.config.get("db", 1)),
                password=self.config.get("password"),
                decode_responses=True,
            )
            self.register_scripts() # 注册所有必要的Lua脚本
        except Exception:
            logging.warning("Redis can't be connected.")
        return self.REDIS

    def health(self):
        self.REDIS.ping()
        a, b = "xx", "yy"
        self.REDIS.set(a, b, 3) # 设置 3 秒的过期时间的键值对

        if self.REDIS.get(a) == b:
            return True

    def is_alive(self):
        ''' 检查Redis连接对象是否已成功创建 '''
        return self.REDIS is not None

    def exist(self, k):
        ''' 检查Redis中是否存在指定的键 '''
        if not self.REDIS:
            return
        try:
            return self.REDIS.exists(k)
        except Exception as e:
            logging.warning("RedisDB.exist " + str(k) + " got exception: " + str(e))
            self.__open__()

    def get(self, k):
        ''' 获取Redis中指定键的值 '''
        if not self.REDIS:
            return
        try:
            return self.REDIS.get(k)
        except Exception as e:
            logging.warning("RedisDB.get " + str(k) + " got exception: " + str(e))
            self.__open__()

    def set_obj(self, k, obj, exp=3600):
        ''' 将Python对象序列化为JSON字符串后存储到Redis，并设置过期时间 '''
        try:
            self.REDIS.set(k, json.dumps(obj, ensure_ascii=False), exp)
            return True
        except Exception as e:
            logging.warning("RedisDB.set_obj " + str(k) + " got exception: " + str(e))
            self.__open__()
        return False

    def set(self, k, v, exp=3600):
        ''' 将键值对存储到Redis，并设置过期时间 '''
        try:
            self.REDIS.set(k, v, exp)
            return True
        except Exception as e:
            logging.warning("RedisDB.set " + str(k) + " got exception: " + str(e))
            self.__open__()
        return False

    def sadd(self, key: str, member: str):
        ''' 将一个或多个成员添加到集合中 '''
        try:
            self.REDIS.sadd(key, member)
            return True
        except Exception as e:
            logging.warning("RedisDB.sadd " + str(key) + " got exception: " + str(e))
            self.__open__()
        return False

    def srem(self, key: str, member: str):
        ''' 从集合中移除一个或多个成员 '''
        try:
            self.REDIS.srem(key, member)
            return True
        except Exception as e:
            logging.warning("RedisDB.srem " + str(key) + " got exception: " + str(e))
            self.__open__()
        return False

    def smembers(self, key: str):
        ''' 获取集合中的所有成员 '''
        try:
            res = self.REDIS.smembers(key)
            return res
        except Exception as e:
            logging.warning(
                "RedisDB.smembers " + str(key) + " got exception: " + str(e)
            )
            self.__open__()
        return None

    def zadd(self, key: str, member: str, score: float):
        ''' 将一个或多个成员及其分数添加到有序集合中 '''
        try:
            self.REDIS.zadd(key, {member: score})
            return True
        except Exception as e:
            logging.warning("RedisDB.zadd " + str(key) + " got exception: " + str(e))
            self.__open__()
        return False

    def zcount(self, key: str, min: float, max: float):
        ''' 计算有序集合中指定分数范围内的成员数量 '''
        try:
            res = self.REDIS.zcount(key, min, max)
            return res
        except Exception as e:
            logging.warning("RedisDB.zcount " + str(key) + " got exception: " + str(e))
            self.__open__()
        return 0

    def zpopmin(self, key: str, count: int):
        ''' 从有序集合中移除并返回分数最低的N个成员 '''
        try:
            res = self.REDIS.zpopmin(key, count)
            return res
        except Exception as e:
            logging.warning("RedisDB.zpopmin " + str(key) + " got exception: " + str(e))
            self.__open__()
        return None

    def zrangebyscore(self, key: str, min: float, max: float):
        ''' 获取有序集合中指定分数范围内的成员 '''
        try:
            res = self.REDIS.zrangebyscore(key, min, max)
            return res
        except Exception as e:
            logging.warning(
                "RedisDB.zrangebyscore " + str(key) + " got exception: " + str(e)
            )
            self.__open__()
        return None

    def transaction(self, key, value, exp=3600):
        ''' 使用Redis事务以原子方式设置键值对，如果键不存在则设置 '''
        try:
            pipeline = self.REDIS.pipeline(transaction=True)
            pipeline.set(key, value, exp, nx=True)
            pipeline.execute()
            return True
        except Exception as e:
            logging.warning(
                "RedisDB.transaction " + str(key) + " got exception: " + str(e)
            )
            self.__open__()
        return False

    def queue_product(self, queue, message) -> bool:
        ''' 将消息添加到Redis Stream队列中 '''
        for _ in range(3):
            try:
                payload = {"message": json.dumps(message)}
                self.REDIS.xadd(queue, payload)
                return True
            except Exception as e:
                logging.exception(
                    "RedisDB.queue_product " + str(queue) + " got exception: " + str(e)
                )
        return False

    def queue_consumer(self, queue_name, group_name, consumer_name, msg_id=b">") -> RedisMsg:
        """
        从Redis Stream中消费消息，如果消费者组不存在则创建。
        参考: https://redis.io/docs/latest/commands/xreadgroup/
        """
        try:
            group_info = self.REDIS.xinfo_groups(queue_name)
            if not any(gi["name"] == group_name for gi in group_info):
                self.REDIS.xgroup_create(queue_name, group_name, id="0", mkstream=True)
            args = {
                "groupname": group_name,
                "consumername": consumer_name,
                "count": 1,
                "block": 5,
                "streams": {queue_name: msg_id},
            }
            messages = self.REDIS.xreadgroup(**args)
            if not messages:
                return None
            stream, element_list = messages[0]
            if not element_list:
                return None
            msg_id, payload = element_list[0]
            res = RedisMsg(self.REDIS, queue_name, group_name, msg_id, payload)
            return res
        except Exception as e:
            if str(e) == 'no such key':
                pass
            else:
                logging.exception(
                    "RedisDB.queue_consumer "
                    + str(queue_name)
                    + " got exception: "
                    + str(e)
                )
        return None

    def get_unacked_iterator(self, queue_names: list[str], group_name, consumer_name):
        '''
        获取未确认消息的迭代器，遍历指定队列中属于某个消费者组和消费者的未确认消息。
        '''
        try:
            for queue_name in queue_names:
                try:
                    group_info = self.REDIS.xinfo_groups(queue_name)
                except Exception as e:
                    if str(e) == 'no such key':
                        logging.warning(f"RedisDB.get_unacked_iterator queue {queue_name} doesn't exist")
                        continue
                if not any(gi["name"] == group_name for gi in group_info):
                    logging.warning(f"RedisDB.get_unacked_iterator queue {queue_name} group {group_name} doesn't exist")
                    continue
                current_min = 0
                while True:
                    payload = self.queue_consumer(queue_name, group_name, consumer_name, current_min)
                    if not payload:
                        break
                    current_min = payload.get_msg_id()
                    logging.info(f"RedisDB.get_unacked_iterator {queue_name} {consumer_name} {current_min}")
                    yield payload
        except Exception:
            logging.exception(
                "RedisDB.get_unacked_iterator got exception: "
            )
            self.__open__()

    def get_pending_msg(self, queue, group_name):
        ''' 获取指定队列和消费者组的待处理消息列表 '''
        try:
            messages = self.REDIS.xpending_range(queue, group_name, '-', '+', 10)
            return messages
        except Exception as e:
            if 'No such key' not in (str(e) or ''):
                logging.warning(
                    "RedisDB.get_pending_msg " + str(queue) + " got exception: " + str(e)
                )
        return []

    def requeue_msg(self, queue: str, group_name: str, msg_id: str):
        '''
        重新排队一个消息：从队列中读取指定ID的消息内容，重新添加到队列中，并确认原消息。
        这通常用于处理失败的消息，将其重新放入队列以便稍后再次处理。
        '''
        try:
            messages = self.REDIS.xrange(queue, msg_id, msg_id)
            if messages:
                self.REDIS.xadd(queue, messages[0][1])
                self.REDIS.xack(queue, group_name, msg_id)
        except Exception as e:
            logging.warning(
                "RedisDB.get_pending_msg " + str(queue) + " got exception: " + str(e)
            )

    def queue_info(self, queue, group_name) -> dict | None:
        ''' 获取指定队列和消费者组的信息 '''
        try:
            groups = self.REDIS.xinfo_groups(queue)
            for group in groups:
                if group["name"] == group_name:
                    return group
        except Exception as e:
            logging.warning(
                "RedisDB.queue_info " + str(queue) + " got exception: " + str(e)
            )
        return None

    def delete_if_equal(self, key: str, expected_value: str) -> bool:
        '''
        原子性操作：如果Redis中指定键的值等于给定值，则删除该键，否则不做任何操作。
        此操作通过Lua脚本实现，保证原子性。

        Do follwing atomically:
        Delete a key if its value is equals to the given one, do nothing otherwise.
        '''
        return bool(self.lua_delete_if_equal(keys=[key], args=[expected_value], client=self.REDIS))

    def delete(self, key) -> bool:
        ''' 从Redis中删除指定的键 '''
        try:
            self.REDIS.delete(key)
            return True
        except Exception as e:
            logging.warning("RedisDB.delete " + str(key) + " got exception: " + str(e))
            self.__open__()
        return False
    
    
REDIS_CONN = RedisDB()


class RedisDistributedLock:
    def __init__(self, lock_key, lock_value=None, timeout=10, blocking_timeout=1):
        """
        初始化分布式锁。
        :param lock_key: 锁的键名，用于在 Redis 中标识锁。
        :param lock_value: 锁的值，默认为 None，如果未提供则生成一个 UUID。这个值用于“所有者”的概念，确保只有持有锁的进程才能释放它。
        :param timeout: 锁的过期时间（秒），防止死锁。
        :param blocking_timeout: 尝试获取锁时的阻塞超时时间（秒）。
        """
        self.lock_key = lock_key
        if lock_value:
            self.lock_value = lock_value
        else:
            self.lock_value = str(uuid.uuid4())
        self.timeout = timeout
        self.lock = Lock(REDIS_CONN.REDIS, lock_key, timeout=timeout, blocking_timeout=blocking_timeout)

    def acquire(self):
        """
        尝试获取分布式锁。
        在尝试获取锁之前，会先尝试删除与当前 lock_key 和 lock_value 匹配的旧锁，
        这有助于清理可能由于客户端崩溃而遗留的锁（通常在实际应用中会更谨慎处理）。
        :return: 如果成功获取锁则返回 True，否则返回 False。
        """
        # 在尝试获取锁之前，先删除可能存在的旧锁，确保幂等性或清理。
        REDIS_CONN.delete_if_equal(self.lock_key, self.lock_value)
        # 调用 Redis-Py Lock 对象的 acquire 方法来获取锁。
        return self.lock.acquire(token=self.lock_value)

    async def spin_acquire(self):
        """
        自旋尝试获取分布式锁，直到成功获取为止。
        这是一个异步方法，当无法立即获取锁时，会等待一段时间后重试，
        避免长时间阻塞。
        """
        REDIS_CONN.delete_if_equal(self.lock_key, self.lock_value)
        while True:
            if self.lock.acquire(token=self.lock_value):
                break
            await trio.sleep(10)

    def release(self):
        """
        释放分布式锁。
        只有当存储在 Redis 中的锁的值与当前实例的 lock_value 匹配时，才会成功释放锁。
        这确保了只有锁的持有者才能释放它。
        """
        # 调用 REDIS_CONN 的 delete_if_equal 方法来原子性地删除锁。
        # 确保只有当 Redis 中的锁值与当前的 self.lock_value 相等时才删除，
        # 避免误删其他进程持有的锁。
        REDIS_CONN.delete_if_equal(self.lock_key, self.lock_value)
