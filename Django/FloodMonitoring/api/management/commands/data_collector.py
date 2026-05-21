
import os

from django.core.management.base import BaseCommand

from api.management.helpers.blynk_listener import start_blynk_listener
from ..helpers.mqtt_listener import start_mqtt_listener
from ..helpers.ingester import ingest_datapoints_from_queue
from ..helpers.model_predictor.predictor import predict_batch
from ...supabase.utils import push_to_supabase
from queue import Queue
import threading, time

from dotenv import load_dotenv
load_dotenv()

class Command(BaseCommand):

    help = "Background process for acquiring HiveMQ/Blynk + OpenWeatherAPI data to log in Supabase and send to client/edge device"
    
    def handle(self, *args, **kwargs):

        msg_queue = Queue()
        
        self.stdout.write("[MAIN] Starting Sensor Listener thread...")

        LISTENER_TYPE = os.getenv('BLYNK_OR_MQTT').lower()

        if LISTENER_TYPE == 'mqtt':
            thread = threading.Thread(target=start_mqtt_listener, args=(msg_queue,), daemon=True)
        elif LISTENER_TYPE == 'blynk':
            thread = threading.Thread(target=start_blynk_listener, args=(msg_queue,), daemon=True)
        thread.start()

        while True:

            if not msg_queue.empty():
                print("[MAIN] Received data from queue")
                print("[MAIN] Processing batch...")
                new_data_batch = ingest_datapoints_from_queue(msg_queue)
                print(f"[MAIN] Acquired Batch: {new_data_batch}")
                
                if new_data_batch:
                    print("[MAIN] Running model prediction...")
                    forecast_data_batch = predict_batch(new_data_batch)
                    print(f"[MAIN] Prediction acquired: {forecast_data_batch}")
                    push_to_supabase(forecast_data_batch, new_data_batch)
            else:
                time.sleep(0.05)