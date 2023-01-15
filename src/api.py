import base64
import copy
import datetime
import json
import os
import threading

import fastapi
import kafka
import pymongo


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


app = fastapi.FastAPI()

events_thread = threading.Thread(target=load_events)
events_thread.start()


@app.get("/healthz")
async def healthz():
    return {'status': "ok"}

# @app.get("/content")
# async def content():
#     return {"content": file_content_processor.content,
#             }
