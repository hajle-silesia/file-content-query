import base64
import copy
import datetime
import json
import os
import threading

import fastapi
import kafka
import pymongo
import requests


def load_events():
    consumer = kafka.KafkaConsumer(bootstrap_servers="kafka-cluster-kafka-bootstrap.event-streaming:9092",
                                   value_deserializer=lambda message: json.loads(base64.b64decode(message).decode()),
                                   )
    consumer.subscribe(topics=["file-content-processor-topic"])
    for event in consumer:
        mongo_client = pymongo.MongoClient(host="mongodb://file-content.default.svc.cluster.local",
                                           port=80,
                                           username=os.getenv('FILE_CONTENT_ROOT_USERNAME'),
                                           password=os.getenv('FILE_CONTENT_ROOT_PASSWORD'),
                                           )
        db = mongo_client["file-content"]
        collection = db["recipes"]

        utc_timezone_aware_timestamp = datetime.datetime.now(datetime.timezone.utc)
        locale_timezone_timestamp = utc_timezone_aware_timestamp.astimezone()
        record = copy.deepcopy(event.value)
        record.update({'timestamp': locale_timezone_timestamp.isoformat()})
        collection.insert_one(record)


def get_latest_recipe():
    mongo_client = pymongo.MongoClient(host="mongodb://file-content.default.svc.cluster.local",
                                       port=80,
                                       username=os.getenv('FILE_CONTENT_ROOT_USERNAME'),
                                       password=os.getenv('FILE_CONTENT_ROOT_PASSWORD'),
                                       )

    db = mongo_client["file-content"]
    collection = db["recipes"]

    results = collection.find().sort('_id', pymongo.DESCENDING).limit(1)
    for result in results:
        return result


app = fastapi.FastAPI()

events_thread = threading.Thread(target=load_events)
events_thread.start()


@app.get("/api/healthz")
async def healthz():
    return {'status': "ok"}


@app.post("/api/file-content-monitor/update")
async def file_content_monitor_update(request: fastapi.Request):
    url = "http://file-content-monitor-file-content-monitor-config.default.svc.cluster.local/update"
    response = requests.post(url=url, data=await request.body(), timeout=3)
    return response.status_code


@app.get("/api/file-content")
async def file_content():
    response = get_latest_recipe()
    return base64.b64encode(json.dumps(response, default=str).encode())


@app.get("/api/video-stream")
async def video_stream():
    response = requests.get(
        url="http://srs-server.default.svc.cluster.local:8080/live/livestream.flv",
        stream=True,
        timeout=3,
    )
    return response
