from google.cloud import pubsub_v1
from google.cloud import storage
import json
from datetime import datetime
from random import randrange
from datetime import timedelta
import time


storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
project_id = "smartlive"
topic_name = "topic1"
topic_path = publisher.topic_path(project_id, topic_name)

# Get bucket with name 
bucket = storage_client.get_bucket('rim-bucket')
blob = bucket.get_blob('market.json')

stringData = blob.download_as_string()
# json.loads returns an object from a string representing a json object.
list = json.loads(stringData)
current = datetime.now()
for i in range(len(list)):
    item = list[i]
    item.update({'timestamp': ''}) # add a new field : timestamp 
    new = current + timedelta(seconds=randrange(20))
    # Object of type datetime is not JSON Serializable
    item['timestamp'] = str(new) # add a value to the key "timestamp"
    print('item   **   ', item)
    json_data = json.dumps(item).encode('utf-8')
    # bytestring
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data=json_data)
    # Sleep makes the program run slower 
    time.sleep(2) 
print('Published messages.')