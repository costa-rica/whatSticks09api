from flask import Blueprint
from flask import request, jsonify, make_response, current_app
from ws09_models import sess, Users, Oura_token, Oura_sleep_descriptions,\
    Locations, Weather_history, User_location_day
from werkzeug.security import generate_password_hash, check_password_hash #password hashing
import bcrypt
#import jwt #token creating thing
import datetime
# import base64
# from functools import wraps

from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
from app_package.utilsDecorators import token_required
import logging
import os
from logging.handlers import RotatingFileHandler
import json


formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
formatter_terminal = logging.Formatter('%(asctime)s:%(filename)s:%(name)s:%(message)s')

logger_users = logging.getLogger(__name__)
logger_users.setLevel(logging.DEBUG)

file_handler = RotatingFileHandler(os.path.join(os.environ.get('WS_ROOT_API'),'logs','users_routes.log'), mode='a', maxBytes=5*1024*1024,backupCount=2)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter_terminal)

logger_users.addHandler(file_handler)
logger_users.addHandler(stream_handler)


users = Blueprint('users',__name__)
logger_users.info(f'- whatSticks09 API users Bluprints initialized')

salt = bcrypt.gensalt()

@users.route('/are_we_working', methods=['GET'])
def are_we_working():
    logger_users.info(f"are_we_working endpoint pinged")

    logger_users.info(f"{current_app.config.get('WS_API_PASSWORD')}")

    # print(dir(current_app.config))
    # print(current_app.config.items())

    return jsonify("Yes! We're up! in the utilties02 machine")

@users.route('/diagnostics', methods=['GET'])
def diagnostics():
    logger_users.info(f"- ws09api diagnostics pinged")
    if current_app.config.get('WS_API_PASSWORD') == request.headers.get('password'):

        logger_users.info(f"{current_app.config.get('WS_API_PASSWORD')}")

        diagnostics_dict = {}

        #remove any current_app.config.items NOT json serializable
        for key, value in current_app.config.items():
            try:
                json.dumps(value)
                diagnostics_dict[key]=value
            except (TypeError, OverflowError):
                pass

        diagnostics_dict_json = json.dumps(diagnostics_dict)


        return diagnostics_dict_json
    else:
        return make_response('Could note verify sender', 401)


@users.route('/add_user', methods=['POST'])
def add_users():
    logger_users.info(f"- add_user endpoint pinged -")

    if current_app.config.get('WS_API_PASSWORD') == request.headers.get('password'):

        request_data = request.get_json()

        if request_data.get('email') in ("", None) or request_data.get('password') in ("" , None):
            return make_response('User must have email and password', 409)

        user_exists = sess.query(Users).filter_by(email= request_data.get('email')).first()

        if user_exists:
            return make_response('User already exists', 409)

        hash_pw = bcrypt.hashpw(request_data.get('password').encode(), salt)
        new_user = Users()

        for key, value in request_data.items():
            if key == "password":
                setattr(new_user, "password", hash_pw)
            elif key in Users.__table__.columns.keys():
                setattr(new_user, key, value)

        sess.add(new_user)
        sess.commit()
        return jsonify({"message": f"new user created: {request_data.get('email')}"})
    else:
        return make_response('Could note verify sender', 401)



@users.route('/login',methods=['GET'])
def login():
    logger_users.info(f"- login endpoint pinged -")

    if current_app.config.get('WS_API_PASSWORD') == request.headers.get('password'):
        logger_users.info(f"- sender password verified -")

        auth = request.authorization
        logger_users.info(f"- auth.username: {auth.username} -")

        if not auth or not auth.username or not auth.password:
            return make_response('Could note verify', 401)

        user = sess.query(Users).filter_by(email= auth.username).first()

        if not user:
            return make_response('Could note verify - user not found', 401)

        logger_users.info(f"- checking password -")

        if bcrypt.checkpw(auth.password.encode(), user.password):
            logger_users.info(f"- password checks out! -")

            expires_sec=60*20#set to 20 minutes
            s=Serializer(current_app.config['SECRET_KEY'], expires_sec)
            token = s.dumps({'user_id': user.id}).decode('utf-8')
            return jsonify({'token': token})

        return make_response('Could not verify', 401)
    else:
        return make_response('Could note verify sender', 401)

@users.route('/user_account_data',methods=['GET'])
@token_required
def get_user_data(current_user):#current user is passed in by @token_required
    # print('in get_user_data')


    logger_users.info(f"- /user_account_data endpoint pinged: get this users data -")

    # if current_app.config.get('WS_API_PASSWORD') == request.headers.get('password'):
    #     logger_users.info(f"- sender password verified -")

    print(current_user)
    print(dir(current_user))
    print(current_user.id)

    # print(type(user_id), user_id)
    # if current_user.id != int(user_id):
    #     return jsonify({'message': 'Cannot access user data'})

    # user = Users.query.get(user_id)
    # user = Users.query.filter_by(id = user_id).first()

    user_data={}
    user_data['id'] = current_user.id
    # user_data['username'] = current_user.username
    user_data['email'] = current_user.email
    # user_data['password'] = current_user.password
    # user_data['notes'] = current_user.notes
    # user_data['lat'] = current_user.lat
    # user_data['height_feet'] = current_user.height_feet
    # user_data['password'] = user.password

    return(user_data)


