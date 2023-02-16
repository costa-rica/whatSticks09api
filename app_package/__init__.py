from flask import Flask
from ws09_config import ConfigLocal, ConfigDev, ConfigProd
from ws09_config import config
import os
import logging
from logging.handlers import RotatingFileHandler
from pytz import timezone
from datetime import datetime


if os.environ.get('CONFIG_TYPE')=='local':
    config = ConfigLocal()
    print('- whatSticks09/__init__: Development - Local')
elif os.environ.get('CONFIG_TYPE')=='dev':
    config = ConfigDev()
    print('- whatSticks09/__init__: Development')
elif os.environ.get('CONFIG_TYPE')=='prod':
    config = ConfigProd()
    print('- whatSticks09/__init__: Configured for Production')
# print("---- whatSticks09api/app_package/__init__.py ----")
# print("config.GUEST_EMAIL")
# print(config.GUEST_EMAIL)


if not os.path.exists(os.path.join(os.environ.get('WS_ROOT_API'),'logs')):
    os.makedirs(os.path.join(os.environ.get('WS_ROOT_API'), 'logs'))

# timezone 
def timetz(*args):
    return datetime.now(timezone('Europe/Paris') ).timetuple()

logging.Formatter.converter = timetz

formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
formatter_terminal = logging.Formatter('%(asctime)s:%(filename)s:%(name)s:%(message)s')

logger_init = logging.getLogger('__init__')
logger_init.setLevel(logging.DEBUG)

file_handler = RotatingFileHandler(os.path.join(os.environ.get('WS_ROOT_API'),'logs','__init__.log'), mode='a', maxBytes=5*1024*1024,backupCount=2)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter_terminal)

stream_handler_tz = logging.StreamHandler()

logger_init.addHandler(file_handler)
logger_init.addHandler(stream_handler)

logging.getLogger('werkzeug').setLevel(logging.DEBUG)
logging.getLogger('werkzeug').addHandler(file_handler)

logger_init.info(f'--- Starting whatSticks09 API ---')


def create_app(config_for_flask = config):
    print("---- whatSticks09api/app_package/__init__.py create_app() ----")
    print("config.GUEST_EMAIL")
    print(config.WS_API_PASSWORD)
    app = Flask(__name__)   
    app.config.from_object(config_for_flask)

    # print(f"- app.config: {dir(app.config)} -")
    # print(f"- app.config: {app.config.items()} -")

    from app_package.users.routes import users
    # from app_package.api.routes import api

    app.register_blueprint(users)
    # app.register_blueprint(api)

    return app