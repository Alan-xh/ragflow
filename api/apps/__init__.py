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
from flask_cors import CORS
from flasgger import Swagger
from itsdangerous.url_safe import URLSafeTimedSerializer as Serializer

from api.db import StatusEnum
from api.db.db_models import close_connection
from api.db.services import UserService
from api.utils import CustomJSONEncoder, commands

from flask_session import Session
from flask_login import LoginManager
from api import settings
from api.utils.api_utils import server_error_response
from api.constants import API_VERSION

__all__ = ["app"]

Request.json = property(lambda self: self.get_json(force=True, silent=True))

app = Flask(__name__)

# Add this at the beginning of your file to configure Swagger UI
# 在文件开头添加此内容以配置 Swagger UI
swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": "apispec",
            "route": "/apispec.json",
            "rule_filter": lambda rule: True,  # Include all endpoints
            "model_filter": lambda tag: True,  # Include all models
        }
    ],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/apidocs/",
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
        "securityDefinitions": {
            "ApiKeyAuth": {"type": "apiKey", "name": "Authorization", "in": "header"}
        },
    },
)

CORS(
    app, supports_credentials=True, max_age=2592000
)  # 允许跨域请求时携带凭证 预检请求的结果缓存 30 天
app.url_map.strict_slashes = False  # 设置末尾带不带 /为等价的
app.json_encoder = (
    CustomJSONEncoder  # 定制编码器，处理标准编码器无法处理的自定义数据类型
)
app.errorhandler(Exception)(server_error_response)  # 注册了全局的错误处理函数

## convince for dev and debug
# 配置开发和调试
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

# 注册命令行
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
    导入模块并且注册蓝图
    
    # pathlib.Path.stem 文件名部分，不包括后缀
    # pathlib.Path.parts 将路径分解成各个组成部分，并返回一个元组

    根据 page_path动态导入模块
    spec_from_file_location 根据模块的文件路径创建一个模块的规范 (spec), 返回 importlib.machinery.ModuleSpec
    module_from_spec 根据一个模块规范 (spec) 创建一个空的模块对象, 返回 types.ModuleType实例
    '''

    page_name = page_path.stem.removesuffix("_app")
    module_name = ".".join(
        page_path.parts[page_path.parts.index("api"): -1] + (page_name,)
    )

    spec = spec_from_file_location(module_name, page_path)
    page = module_from_spec(spec)
    page.app = app
    page.manager = Blueprint(page_name, module_name)
    sys.modules[module_name] = page
    spec.loader.exec_module(page)
    page_name = getattr(page, "page_name", page_name)
    sdk_path = "\\sdk\\" if sys.platform.startswith("win") else "/sdk/"
    url_prefix = (
        f"/api/{API_VERSION}" if sdk_path in path else f"/{API_VERSION}/{page_name}"
    )

    app.register_blueprint(page.manager, url_prefix=url_prefix)
    return url_prefix


# 注册 apps和 sdk下所有页面
pages_dir = [
    Path(__file__).parent,
    Path(__file__).parent.parent / "api" / "apps",
    Path(__file__).parent.parent / "api" / "apps" / "sdk",
]

client_urls_prefix = [
    register_page(path) for dir in pages_dir for path in search_pages_path(dir)
]


@login_manager.request_loader
def load_user(web_request):
    jwt = Serializer(secret_key=settings.SECRET_KEY)
    authorization = web_request.headers.get("Authorization")
    if authorization:
        try:
            access_token = str(jwt.loads(authorization))
            user = UserService.query(
                access_token=access_token, status=StatusEnum.VALID.value
            )
            if user:
                return user[0]
            else:
                return None
        except Exception as e:
            logging.warning(f"load_user got exception {e}")
            return None
    else:
        return None


@app.teardown_request
def _db_close(exc):
    close_connection()
