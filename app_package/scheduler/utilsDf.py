from flask import current_app
from datetime import datetime, timedelta
import os
from ws09_models import sess, text, engine, Oura_sleep_descriptions, Weather_history, \
    User_location_day, Apple_health_export
import pandas as pd
import json
import numpy as np
import logging
from logging.handlers import RotatingFileHandler
# from ws_config01 import ConfigDev, ConfigProd, ConfigLocal


# if os.uname()[1] == 'Nicks-Mac-mini.lan' or os.uname()[1] == 'NICKSURFACEPRO4':
#     config = ConfigLocal()
#     # testing = True
# elif 'dev' in os.uname()[1]:
#     config = ConfigDev()
#     # testing = False
# elif 'prod' in os.uname()[1] or os.uname()[1] == 'speedy100':
#     config = ConfigProd()
#     # testing = False
# if os.environ.get('CONFIG_TYPE')=='local':
#     config_context = ConfigLocal()
# elif os.environ.get('CONFIG_TYPE')=='dev':
#     config_context = ConfigDev()
# elif os.environ.get('CONFIG_TYPE')=='prod':
#     config_context = ConfigProd()



#Setting up Logger
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
formatter_terminal = logging.Formatter('%(asctime)s:%(filename)s:%(name)s:%(message)s')

#initialize a logger
logger_sched = logging.getLogger(__name__)
logger_sched.setLevel(logging.DEBUG)
# logger_terminal = logging.getLogger('terminal logger')
# logger_terminal.setLevel(logging.DEBUG)

#where do we store logging information
# file_handler = RotatingFileHandler(os.path.join(config_context.API_LOGS_DIR,'schd_routes.log'), mode='a', maxBytes=5*1024*1024,backupCount=2)
file_handler = RotatingFileHandler(os.path.join(os.environ.get('API_ROOT'),'logs','schd_routes.log'), mode='a', maxBytes=5*1024*1024,backupCount=2)
file_handler.setFormatter(formatter)

#where the stream_handler will print
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter_terminal)

# logger_sched.handlers.clear() #<--- This was useful somewhere for duplicate logs
logger_sched.addHandler(file_handler)
logger_sched.addHandler(stream_handler)

#This function is the same as in dashboard/utilsChart
def create_raw_df(USER_ID, table, table_name):

    if table_name != "weather_history_":
        base_query = sess.query(table).filter_by(user_id = 1)
        df = pd.read_sql(text(str(base_query)[:-1] + str(USER_ID)), engine.connect())
    else:
        base_query = sess.query(table)
        df = pd.read_sql(text(str(base_query)), engine.connect())
    if len(df) == 0:
        return False
    
    cols = list(df.columns)
    for col in cols:
        if col[:len(table_name)] == table_name:
            df = df.rename(columns=({col: col[len(table_name):]}))
    
    return df



def apple_hist_util(USER_ID, data_item_list, data_item_name_show, method, data_item_apple_type_name):
    print('--  IN apple_hist_util ---')
    df = create_raw_df(USER_ID, Apple_health_export, 'apple_health_export_')
    if isinstance(df,bool):
        return df
    df=df[df['type']==data_item_apple_type_name]
    df['date']=df['creationDate'].str[:10]
    df=df[['date', 'value']].copy()
    df['value']=df['value'].astype(float)

    df = df.rename(columns=({'value': data_item_list[0]}))
    if method == 'sum':
        df = df.groupby('date').sum()
    elif method == 'average':
        df = df.groupby('date').mean()
    df[data_item_list[0] + '-ln'] = np.log(df[data_item_list[0]])
    df.reset_index(inplace = True)
    print(df.head())
    return df



def apple_hist_steps(USER_ID):
    df = create_raw_df(USER_ID, Apple_health_export, 'apple_health_export_')
    if isinstance(df,bool):
        return df

    df=df[df['type']=='HKQuantityTypeIdentifierStepCount']
    df['date']=df['creationDate'].str[:10]
    df=df[['date', 'value']].copy()
    df['value']=df['value'].astype(int)
    
    df = df.rename(columns=({'value': 'steps'}))
    df = df.groupby('date').sum()
    df['steps-ln'] = np.log(df.steps)
    df.reset_index(inplace = True)
    
    return df

# def browse_apple_data(USER_ID):
#     table_name = 'apple_health_export_'
#     file_name = f'user{USER_ID}_df_browse_apple.pkl'
#     file_path = os.path.join(current_app.config.get('DF_FILES_DIR, file_name)

#     if os.path.exists(file_path):
#         os.remove(file_path)

#     df = create_raw_df(USER_ID, Apple_health_export, table_name)
#     if not isinstance(df, bool):
#         series_type = df[['type']].copy()
#         series_type = series_type.groupby(['type'])['type'].count()

#         df_type = series_type.to_frame()
#         df_type.rename(columns = {list(df_type)[0]:'record_count'}, inplace=True)
#         df_type.reset_index(inplace=True)

#         df_type.to_pickle(file_path)
#         count = "{:,}".format(df_type.record_count.sum())
#         return count_of_apple_records
#     return False

def browse_apple_data(USER_ID):
    table_name = 'apple_health_export_'
    file_name = f'user{USER_ID}_df_browse_apple.pkl'
    file_path = os.path.join(current_app.config.get('DF_FILES_DIR'), file_name)

    if os.path.exists(file_path):
        os.remove(file_path)

    df = create_raw_df(USER_ID, Apple_health_export, table_name)
    if not isinstance(df, bool):
        series_type = df[['type']].copy()
        series_type = series_type.groupby(['type'])['type'].count()

        df_type = series_type.to_frame()
        df_type.rename(columns = {list(df_type)[0]:'record_count'}, inplace=True)
        count_of_apple_records = "{:,}".format(df_type.record_count.sum())

        # Try add new columns 
        df_type['index'] = range(1,len(df_type)+1)
        df_type.reset_index(inplace=True)
        df_type.set_index('index', inplace=True)
        df_type['type_formatted'] = df_type['type'].map(lambda cell_value: format_item_name(cell_value) )

        df_type['df_file_created']=''
        df_type.to_pickle(file_path)
        
        return count_of_apple_records
    return False


def oura_hist_util(USER_ID, data_item):

    df = create_raw_df(USER_ID, Oura_sleep_descriptions, 'oura_sleep_descriptions_')
    if isinstance(df,bool):
        return df

    logger_sched.info(f'--  making {data_item} --')
    df = df[['summary_date', 'score']].copy()
    # Remove duplicates keeping the last entryget latest date
    df = df.drop_duplicates(subset='summary_date', keep='last')

    if data_item == 'oura_sleep_tonight':
        df.rename(columns=({'summary_date':'date', 'score':'oura_sleep_tonight'}), inplace= True)
        df['oura_sleep_tonight-ln'] = np.log(df.oura_sleep_tonight)

    elif data_item == 'oura_sleep_last_night':
        df.rename(columns=({'summary_date':'date', 'score':'oura_sleep_last_night'}), inplace= True)
        df['date']=pd.to_datetime(df['date'],format='%Y-%m-%d')
        df['date'] = df['date'].apply(lambda d: d-timedelta(1))
        df['date'] = df['date'].astype(str)
        df['oura_sleep_last_night-ln'] = np.log(df.oura_sleep_last_night)

    return df

def user_loc_day_util(USER_ID):
    df = create_raw_df(USER_ID, User_location_day, 'user_location_day_')

    if isinstance(df,bool):
        return False, False
    # 1) get make df of user_day_location
    df = df[['date', 'location_id']]
    df= df.drop_duplicates(subset='date', keep='last')

    #2) make df of all weather [location_id, date, avg temp, cloudcover]
    df_weath_hist = create_raw_df(USER_ID, Weather_history, 'weather_history_')

    if isinstance(df_weath_hist,bool):
        return False, False

    df_weath_hist = df_weath_hist[['date_time','temp','location_id', 'cloudcover']].copy()
    df_weath_hist = df_weath_hist.rename(columns=({'date_time': 'date'}))
    
    # 3) merge on location_id and date
    df_user_date_temp = pd.merge(df, df_weath_hist, 
        how='left', left_on=['date', 'location_id'], right_on=['date', 'location_id'])
    df_user_date_temp = df_user_date_temp[df_user_date_temp['temp'].notna()]
    df_user_date_temp= df_user_date_temp[['date', 'temp', 'cloudcover']].copy()
    df_user_date_temp['cloudcover'] = df_user_date_temp['cloudcover'].astype(float)
    df_user_date_temp['temp-ln']=df_user_date_temp['temp'].apply(
                                        lambda x: np.log(.01) if x==0 else np.log(x))
    df_user_date_temp['cloudcover-ln']=df_user_date_temp['cloudcover'].apply(
                                        lambda x: np.log(.01) if x==0 else np.log(x))

    df_temp = df_user_date_temp[['date', 'temp', 'temp-ln']].copy()
    df_cloud = df_user_date_temp[['date', 'cloudcover', 'cloudcover-ln']].copy()

    return df_temp, df_cloud



def create_df_files(USER_ID, data_item_list , data_item_name_show='',
    method='', data_item_apple_type_name=''):
    logger_sched.info('-- In scheduler/create_df_files --')

    # Items names in data_item_list must match column name
    # print(USER_ID, data_item_list , data_item_name_show, method)
    # print('data_item_apple_type_name::: ', data_item_apple_type_name)

    # create file dictionary {data_item_name: path_to_df (but no df yet)}
    file_dict = {}
    for data_item in data_item_list:
        # temp_file_name = f'user{USER_ID}_df_{data_item}.json'
        temp_file_name = f'user{USER_ID}_df_{data_item}.pkl'
        file_dict[data_item] = os.path.join(current_app.config.get('DF_FILES_DIR'), temp_file_name)

    # Remove any existing df for user
    for _, f in file_dict.items():
        if os.path.exists(f):
            os.remove(f)

    df_dict = {}
    # Make DF for each in database/df_files/
    for data_item, file_path in file_dict.items():
        print(f'data_item: {data_item}')
        if not os.path.exists(file_path):
            # print('data_item: ', data_item)
            if data_item == 'steps':
                df_dict[data_item] = apple_hist_steps(USER_ID)
                # if not isinstance(df_dict['steps'], bool): df_dict['steps'].to_pickle(file_path)
                if not isinstance(df_dict[data_item], bool): df_dict[data_item].to_pickle(file_path)
                #create brows_data_df
                browse_apple_data(USER_ID)
            # elif data_item == 'oura_sleep_tonight':
            elif data_item[:5] == 'oura_':
                logger_sched.info(f'-- data_item: {data_item} fired --')
                df_dict[data_item] = oura_hist_util(USER_ID, data_item)
                if not isinstance(df_dict[data_item], bool):df_dict[data_item].to_pickle(file_path)
            elif data_item =='temp':
                df_dict['temp'], _ = user_loc_day_util(USER_ID)
                # if not isinstance(df_dict['temp'] , bool): df_dict['temp'] .to_pickle(file_path)
                if not isinstance(df_dict[data_item] , bool): df_dict[data_item] .to_pickle(file_path)
            elif data_item == 'cloudcover':
                _, df_dict['cloudcover'] = user_loc_day_util(USER_ID)
                # if not isinstance(df_dict['cloudcover'] , bool): df_dict['cloudcover'] .to_pickle(file_path)
                if not isinstance(df_dict[data_item] , bool): df_dict[data_item] .to_pickle(file_path)
            else:
                print('-- else apple_hist_util --')
                df_dict[data_item_list[0]] = apple_hist_util(USER_ID, data_item_list, data_item_name_show, method, data_item_apple_type_name)
                if not isinstance(df_dict[data_item_list[0]], bool): df_dict[data_item_list[0]].to_pickle(file_path)
        else:
            df_dict[data_item] = pd.read_pickle(file_path)
            logger_sched.info(f'- catchall for future data_item(s) -')

    return df_dict

