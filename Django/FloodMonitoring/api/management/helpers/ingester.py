
from collections import deque
from datetime import datetime
import queue

from .preprocessor import engineer_features_for_sensor, engineer_features_for_weather_api
from .weather_fetcher import get_weather_from_api

WL_LOG_QUEUE_SIZE = 11
RF_LOG_QUEUE_SIZE = 24

#CACHE OBJECTS, CREATE A SEPARATE DATABASE TABLES FOR THIS IN THE FUTURE AS RAILWAY CAN CLEAR THIS UPON BACKEND REFRESH
water_level_readings = {}
rainfall_readings = {}
last_logged_hour = {}
sensor_weather_info = {}

def cache_flood_data(msg_queue: queue.Queue):

    print(f"[FLOOD_CACHE] Storing flood measurements...")

    while True:
        try:
            sensors = msg_queue.get_nowait()

        except queue.Empty:
            break

        for sensor_connected in sensors:
            wlvl_now = sensor_connected['wlvl_now']
            sensor_id = sensor_connected['sensor_id']

            if sensor_id not in water_level_readings:
                water_level_readings[sensor_id] = deque(maxlen=WL_LOG_QUEUE_SIZE)
            water_level_readings[sensor_id].append(wlvl_now)

            print(f"[INGESTER] Sensor {sensor_id} measurement stored...")

        print(f"[FLOOD_CACHE] Sensor measurement storing completed...")


def ingest_datapoints_from_queue(msg_queue: queue.Queue):

    processed_batch = []

    print(f"[INGESTER] Processing each sensor reading...")

    while True:

        try:
            datapoint = msg_queue.get_nowait()
        except queue.Empty:
            break

        current_hour = datetime.fromisoformat(datapoint['datetime'])
        current_hour = current_hour.replace(minute=0, second=0, microsecond=0)
        sensor_id = datapoint['sensor_id']
        wlvl_now = datapoint['wlvl_now']

        water_level_readings.setdefault(sensor_id, deque(maxlen=WL_LOG_QUEUE_SIZE))
        water_level_readings[sensor_id].append(wlvl_now)

        flood_features = engineer_features_for_sensor(water_level_readings[sensor_id])
    
        if sensor_id not in last_logged_hour or last_logged_hour[sensor_id] != current_hour:

            rainfall, weather_info = get_weather_from_api(datapoint['latlong'])

            rainfall_readings.setdefault(sensor_id, deque(maxlen=RF_LOG_QUEUE_SIZE))
            rainfall_readings[sensor_id].append((current_hour, rainfall))

            sensor_weather_info[sensor_id] = weather_info
            last_logged_hour[sensor_id] = current_hour

        weather_features = engineer_features_for_weather_api(rainfall_readings.get(sensor_id, []))
        
        processed_batch.append({

            #datapoint identifiers
            'timestamp': datapoint['datetime'],
            'sensor_id': sensor_id,

            #flood measurement details
            'wlvl_now': flood_features['wlvl_now'],
            'flood_cat_now': datapoint['flood_cat_now'],

            #engineered flood features
            'wlvl_lag_1': flood_features['wlvl_lag_1'],
            'wlvl_lag_2': flood_features['wlvl_lag_2'],
            'wlvl_lag_5': flood_features['wlvl_lag_5'],
            'wlvl_lag_10': flood_features['wlvl_lag_10'],
            'diff_lag_1': flood_features['diff_lag_1'],
            'pct_change_lag_1': flood_features['pct_change_lag_1'],
            'slope_lag_10': flood_features['slope_lag_10'],

            #engineered weather features
            'rainfall_hr1': weather_features['rainfall_hr1'],
            'rainfall_hr2': weather_features['rainfall_hr2'],
            'rainfall_hr12': weather_features['rainfall_hr12'],
            'rainfall_hr24': weather_features['rainfall_hr24'],

            #other supplementary weather information
            'weather_info': sensor_weather_info.get(sensor_id, {}),
        })

        print(f"[INGESTER] Sensor reading processed...")

    print(f"[INGESTER] Returning processed readings...")
    return processed_batch