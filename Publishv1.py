from google.cloud import pubsub_v1
project_id = "smartlive"
topic_name = "topic1"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
data = "Hello world"
data = data.encode('utf-8')
for i in range(3):
    future = publisher.publish(topic_path, data=data)


print('le message {} of message ID {}.'.format(data, future.result())) # just write the last one
# print('le message {} of message ID {}.'.format(data, future.result()))
print('Published messages.')
