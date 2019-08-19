import json
import time
import random
import soundfile
import subprocess
from statistics import mode
from pydub import AudioSegment
import speech_recognition as sr
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud import speech_v1p1beta1 as speech

def IsOverWrote(attributes,description):
    if 'overwroteGeneration' in attributes:
        description += '\tOverwrote generation: %s\n' % (
            attributes['overwroteGeneration'])
    if 'overwrittenByGeneration' in attributes:
        description += '\tOverwritten by generation: %s\n' % (
            attributes['overwrittenByGeneration'])
    return description

def summarize(message):
    # [START parse_message]
    data = message.data.decode('utf-8')
    attributes = message.attributes

    event_type = attributes['eventType']
    bucket_id = attributes['bucketId']
    object_id = attributes['objectId']
    generation = attributes['objectGeneration']
    description = ('\tEvent type: {event_type}\n\tBucket ID: {bucket_id}\n\tObject ID: {object_id}\n'
        '\tGeneration: {generation}\n').format(event_type=event_type,bucket_id=bucket_id,object_id=object_id,generation=generation)
    description=IsOverWrote(attributes,description)

    payload_format = attributes['payloadFormat']
    if payload_format == 'JSON_API_V1':
        object_metadata = json.loads(data)
        size = object_metadata['size']
        content_type = object_metadata['contentType']
        metageneration = object_metadata['metageneration']
        description += (
            '\tContent type: {content_type}\n'
            '\tSize: {object_size}\n'
            '\tMetageneration: {metageneration}\n').format(
                content_type=content_type,
                object_size=size,
                metageneration=metageneration)
    if event_type=="OBJECT_FINALIZE":
		print("Object Uploaded...")
		destination_bucketName = "bkt-splitwav-destination-v7"
		destination_object_id = "destination-v7" + object_id
		print("****************Splitting Start****************")
		message.ack()
                try:
			print("Audio Segment Started")
			new = AudioSegment.from_wav(object_id)
			print("Audio Segment End")
                except:
		        print("Error Occured".sys.exc_inf()[0])
		print("****************Splitting End****************")
    else:
       description="OtherOperation"
    return description
    # [END parse_message]


def poll_notifications(project, subscription_name):
    # [BEGIN poll_notifications]
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project, subscription_name)

    def callback(message):
        print('Received message:\n{}'.format(summarize(message)))
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    print('Listening for messages on {}'.format(subscription_path))
    while True:
        time.sleep(60)
    # [END poll_notifications]


if __name__ == '__main__':
    print("Line1: Start")
    poll_notifications("ai-proj-v7","subs-v7-1000")
    print("LineN: End")
