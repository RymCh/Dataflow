from google.cloud import pubsub_v1
from google.cloud import storage
import json
import time


storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
project_id = "smartlive"
topic_name = "topic1"
topic_path = publisher.topic_path(project_id, topic_name)

bucket = storage_client.get_bucket('rim-bucket')
blob = bucket.get_blob('market.json')

stringData = blob.download_as_string()
list = json.loads(stringData)
# list of dictionaries 
for i in range(len(list)):
    item = list[i] 
    json_data = json.dumps(item).encode('utf-8')
    # bytestring
    future = publisher.publish(topic_path, data=json_data)
    time.sleep(0.1)

print('* published !*')
print(future)
#print('le message {} of message ID {}.'.format(json_data, future.result()))
print('Published messages.')