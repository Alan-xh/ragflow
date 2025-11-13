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
import os
import sys
import logging
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from flask import Blueprint, Flask
from werkzeug.wrappers.request import Request
from flask_cors import CORS  # 跨域插件
from flasgger import Swagger # swagger 插件
from itsdangerous.url_safe import URLSafeTimedSerializer as Serializer

from common.constants import StatusEnum
from api.db.db_models import close_connection
from api.db.services import UserService
from api.utils.json_encode import CustomJSONEncoder
from api.utils import commands

from flask_mail import Mail
from flask_session import Session
from flask_login import LoginManager
from api import settings
from api.utils.api_utils import server_error_response
from api.constants import API_VERSION

__all__ = ["app"]

Request.json = property(lambda self: self.get_json(force=True, silent=True)) # 过滤请求

app = Flask(__name__)
smtp_mail_server = Mail()

# Add this at the beginning of your file to configure Swagger UI
# 在文件开头添加此内容以配置 Swagger UI
swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": "apispec", # apispec 规约
            "route": "/apispec.json", # 生成的API规范JSON文件在Web服务器上的路径 127.0.0.1::8000/apispec.json
            "rule_filter": lambda rule: True,  # Include all endpoints
            "model_filter": lambda tag: True,  # Include all models
        }
    ],
    "static_url_path": "/flasgger_static", # 静态文件路径
    "swagger_ui": True, # 使用Swagger UI
    "specs_route": "/apidocs/", # API文档路径
}


swagger = Swagger(
    app,
    config=swagger_config,
    template={
        "swagger": "2.0",
        "info": {
            "title": "RAGFlow API",
            "description": "",
            "version": "1.0.0",
        },
        # 访问 api doc 的安全认证
        "securityDefinitions": {
            "ApiKeyAuth": {"type": "apiKey", "name": "Authorization", "in": "header"}
        },
    },
)

CORS(
    app, supports_credentials=True, max_age=2592000
)  # 允许携带凭证跨域请求 预检请求的结果缓存 30 天
app.url_map.strict_slashes = False  # 设置末尾带不带 /为等价的
app.json_encoder = (
    CustomJSONEncoder  # 定制编码器，处理标准编码器无法处理的自定义数据类型
)
app.errorhandler(Exception)(server_error_response)  # 注册了全局的错误处理函数

## convince for dev and debug
# 定义特定的服务选项
# app.config["LOGIN_DISABLED"] = True
app.config["SESSION_PERMANENT"] = False  # 会话非永久，重启浏览器需要重新登入
app.config["SESSION_TYPE"] = "filesystem"  # 会话数据存在文件系统里
app.config["MAX_CONTENT_LENGTH"] = int(
    os.environ.get(
        "MAX_CONTENT_LENGTH", 1024 * 1024 * 1024
    )  # 请求体（request body）的最大字节数。这主要用于限制上传文件或POST请求数据的大小。
)

# Flask 默认的会话机制是将会话数据加密后直接存储在客户端的 cookie 中,不安全且性能差，集成 Flask-Session拓展
# 启用并配置 Flask-Session 扩展，从而允许你在服务器端存储用户会话数据，cookie 只存放一个会话 ID
Session(app)
# 配置 app登入管理器
login_manager = LoginManager()
login_manager.init_app(app)

# app 注册命令行
commands.register_commands(app)

def search_pages_path(pages_dir):
    '''
    搜索页面路径中所有 _app.py 和 *sdk/*.py 的文件
    '''
    app_path_list = [
        path for path in pages_dir.glob("*_app.py") if not path.name.startswith(".")
    ]
    api_path_list = [
        path for path in pages_dir.glob("*sdk/*.py") if not path.name.startswith(".")
    ]
    app_path_list.extend(api_path_list)
    return app_path_list


def register_page(page_path):
    path = f"{page_path}"
    '''
    导入模块，将 *_app 重命名, 并且注册蓝图 https://www.cnblogs.com/poloyy/p/15004389.html
    
    # pathlib.Path.stem 文件名部分，不包括后缀
    # pathlib.Path.parts 将路径分解成各个组成部分，并返回一个元组

    根据 page_path动态导入模块
    spec_from_file_location 根据模块的文件路径创建一个模块的规范 (spec), 返回 importlib.machinery.ModuleSpec
    module_from_spec 根据一个模块规范 (spec) 创建一个空的模块对象, 返回 types.ModuleType实例
    '''

    page_name = page_path.stem.removesuffix("_app")  # .stem 返回路径的最后一个组件（文件名或目录名） .removesuffix() python3.9 字符串末尾移除指定后缀, 这里删除 _app
    module_name = ".".join(
        page_path.parts[page_path.parts.index("api"): -1] + (page_name,)  # api.apps.* / api.apps.sdk.*
    )

    spec = spec_from_file_location(module_name, page_path)  # 规约，将从命名的模块指定原路径
    page = module_from_spec(spec) # 创建空的模块，指定路径和对应的模块名 -> types.ModuleType
    # 给创建的模块添加 flask 和 蓝图， 并且注册到系统模块中
    page.app = app
    page.manager = Blueprint(name=page_name, import_name=module_name) # 添加蓝图实例属性 .manager, 一般 import_name = __name__ (模块名)
    sys.modules[module_name] = page
    spec.loader.exec_module(page) # 加载原路径的文件中的属性和方法进入模块
    page_name = getattr(page, "page_name", page_name)
    sdk_path = "\\sdk\\" if sys.platform.startswith("win") else "/sdk/"
    url_prefix = (
        f"/api/{API_VERSION}" if sdk_path in path else f"/{API_VERSION}/{page_name}" # api 使用 /api/API_VERSION 前缀, 非 api 文件下的路由使用 /API_VERSION//page_name 前缀
    )

    app.register_blueprint(page.manager, url_prefix=url_prefix)  # 添加前缀
    return url_prefix


# 注册 apps 和 sdk 下所有应用，有重复，但是不遗漏
pages_dir = [
    Path(__file__).parent,
    Path(__file__).parent.parent / "api" / "apps",
    Path(__file__).parent.parent / "api" / "apps" / "sdk",
]

client_urls_prefix = [
    register_page(path) for dir in pages_dir for path in search_pages_path(dir)
]


# 用户登录认证
@login_manager.request_loader
def load_user(web_request):
    jwt = Serializer(secret_key=settings.SECRET_KEY)
    # 获取 Authorization 凭证
    authorization = web_request.headers.get("Authorization")
    if authorization:
        try:
            # 解密
            access_token = str(jwt.loads(authorization))

            if not access_token or not access_token.strip():
                logging.warning("Authentication attempt with empty access token")
                return None

            # Access tokens should be UUIDs (32 hex characters)
            if len(access_token.strip()) < 32:
                logging.warning(f"Authentication attempt with invalid token format: {len(access_token)} chars")
                return None

            user = UserService.query(
                access_token=access_token, status=StatusEnum.VALID.value
            )
            if user:
                if not user[0].access_token or not user[0].access_token.strip():
                    logging.warning(f"User {user[0].email} has empty access_token in database")
                    return None
                return user[0]
            else:
                return None
        except Exception as e:
            logging.warning(f"load_user got exception {e}")
            return None
    else:
        return None


# 每次请求结束，关闭数据库连接
@app.teardown_request
def _db_close(exc):
    close_connection()
