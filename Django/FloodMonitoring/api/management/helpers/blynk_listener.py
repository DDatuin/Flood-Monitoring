from datetime import datetime, timezone
import time

from paho.mqtt import client as mqtt
from queue import Queue
import ssl, json, os

from dotenv import load_dotenv
import requests

from api.supabase.utils import get_sensor_details_from_supabase
from api.utils import find_category

load_dotenv()

def start_blynk_listener(msg_queue: Queue):

    supabase_response = get_sensor_details_from_supabase()

    while True:
        try:
            supabase_response = get_sensor_details_from_supabase()

            for sensor in supabase_response:
                if not sensor.get('token') or not sensor.get('pin'):
                    print(f"[BLYNK] SENSOR: {sensor['sensor_id']} does not have designated token and pin. Skipping...")
                    continue

                url = f"https://blynk.cloud/external/api/get?token={sensor['token']}&pin={sensor['pin']}"
                response = requests.get(url, timeout=10)

                if response.status_code == 200:
                    value = float(response.text.strip())

                    wlvl_now = sensor['ground_distance'] - value

                    payload = {
                        "sensor_id": sensor['sensor_id'],
                        "datetime": datetime.now(timezone.utc).isoformat(),
                        "wlvl_now": wlvl_now,
                        "flood_cat_now": find_category(wlvl_now),
                        "latlong": [sensor["latitude"], sensor["longitude"]],
                    }

                    msg_queue.put(payload)
                    print(f"[BLYNK] Payload inserted: {payload}")

        except Exception as e:
            print(f"[BLYNK] ERROR: {e}")

        time.sleep(300)