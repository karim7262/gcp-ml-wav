import json
import time
import random
import soundfile
import subprocess
from statistics import mode
from pydub import AudioSegment
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
        #Start Copy Object to Destination in Spliting Format
        CpSrcTrgtSplits(bucket_id, object_id)

        #End Copy Object to Destination in Spliting Format
    else:
       description="OtherOperation"
    return description
    # [END parse_message]


def CpSrcTrgtSplits(bucket_id, object_id):
    client = speech.SpeechClient()
    lang_dict = {'en-us': '_ENGS', 'pt-br': '_BRPS', 'es-es': '_SPLS'}
    # bucket_name=bucket_id #"bkt-v5-628"
    # filename=object_id
    foldername = "language-detection-test"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_id)  # foldername+"/"+bucket_id)
    blob = bucket.blob(object_id)  # foldername+"/"+object_id) #foldername + '/' + filename)
    destination_object_id = "bkt-splitwav-destination"
    # blob.download_to_filename(destination_object_id)
    print("OBJECT ID IS: " + object_id + "a " + destination_object_id)
    # newData = AudioSegment.from_wav(foldername+"/"+object_id)
    # i = 0
    # while i < len(new):
    #    outfile = new[i:i + 15000]
    #    outfile.export('./splits/trimmed_' + str(i/15000) + '.wav', format='wav')
    #    data, samplerate = soundfile.read('./splits/trimmed_' + str(i/15000) + '.wav')
    #    soundfile.write('./splits/reduced_' + str(i/15000) + '.wav', data, samplerate, subtype='PCM_16')
    #    os.remove('./splits/trimmed_' + str(i/15000) + '.wav')
    #    i += 15000
    # print('Splitting Complete!')
    # description="Modified"


def poll_notifications(project, subscription_name):
    """Polls a Cloud Pub/Sub subscription for new GCS events for display."""
    # [BEGIN poll_notifications]
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project, subscription_name)

    def callback(message):
        print('Received message:\n{}'.format(summarize(message)))
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    # The subscriber is non-blocking, so we must keep the main thread from
    # exiting to allow it to process messages in the background.
    print('Listening for messages on {}'.format(subscription_path))
    while True:
        time.sleep(60)
    # [END poll_notifications]


if __name__ == '__main__':
    poll_notifications("ai-proj-v5","subs-v5-628")
