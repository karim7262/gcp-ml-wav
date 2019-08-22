import os
import json
import time
import random
import soundfile
import subprocess
from statistics import mode
from pydub import AudioSegment
import speech_recognition as sr
from google.cloud import storage
from google.cloud import pubsub
from google.cloud import speech_v1p1beta1 as speech

def IsOverWrote(attributes,description):
    if 'overwroteGeneration' in attributes:
        description += '\tOverwrote generation: %s\n' % (
            attributes['overwroteGeneration'])
    if 'overwrittenByGeneration' in attributes:
        description += '\tOverwritten by generation: %s\n' % (
            attributes['overwrittenByGeneration'])
    return description

def clear_folder(dir):
    if os.path.exists(dir):
        for the_file in os.listdir(dir):
            file_path = os.path.join(dir, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                else:
                    clear_folder(file_path)
                    os.rmdir(file_path)
            except Exception as e:
                print(e)


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
		print("****************Start****************")
		#message.ack()
                try:
			message.ack()
			
			print("1.If Not Exists then Create Temp Folder")
			strTempWavFldr="temp-wav"
			print("object_id is "+object_id)
			object_without_ext=os.path.splitext(str(object_id))[0]
			print("Object Without Ext..."+object_without_ext)
			strObjectURL="./"+strTempWavFldr+"/"+object_without_ext
			print("Complete URL "+strObjectURL)
			if os.path.exists(strObjectURL):
				print("Found some files and folders with newly uploaded object")
				clear_folder(strObjectURL)
			os.mkdir(strObjectURL)
			
			print("2.Copy From Bucket to Temp Folder")
			strBlobURL="gs://"+bucket_id+"/"+object_id
			subprocess.call(["gsutil",'cp',strBlobURL,strObjectURL+"/."])
			
			print("3.Create Multiple Files")
			strInputWavFile="./"+strObjectURL+"/"+object_id
			subprocess.call("ffmpeg -i "+strInputWavFile+" -f segment -segment_time 1 -c copy ./"+strObjectURL+"/out%03d.wav",shell=True)
			if os.path.exists(strInputWavFile):
				os.remove(strInputWavFile)
			print("4.Send wav audio part file to Speech API")
			first_lang = 'es-ES'
			second_lang = ['en-US', 'pt-BR']
			sample_size = 40
			#print("Variable Initialized")
			fileList = os.listdir('./'+strObjectURL)
			probable_languages_list = []
			print("fileList Prepared"+fileList)
			if sample_size > len(fileList):
				sample_size = len(fileList)
			#new = AudioSegment.from_wav(object_id)
			print("sample_size checked: "+sample_size)
			for speech_file in random.sample(fileList, sample_size):
				print("Speech File Loop Start")
				print("speech_file: "+speech_file)
				with open('./'+strObjectURL+'/' + speech_file, 'rb') as audio_file:
					content = audio_file.read()
				print("File Open with this Location")
				audio = speech.types.RecognitionAudio(content=content)
				print("Audio Object Prepared: ")
				config = speech.types.RecognitionConfig(encoding=speech.enums.RecognitionConfig.AudioEncoding.LINEAR16,audio_channel_count=2,language_code=first_lang,alternative_language_codes=second_lang,model='default')
				print('Waiting for operation to complete...')
				languageList = []
				response = client.recognize(config, audio)
				print("response object")

				for i in response.results:
					languageList.append(i.language_code)
				print("Language List are: "+languageList)
				if languageList:
					probable_languages_list.append(mode(languageList))
			print("Probable Language List are: "+probable_languages_list)
			detected_language = mode(probable_languages_list)
			print("Detected Language are: "+detected_language)
			#print("Audio Segment End")
                except:
		        print("Error Occured".sys.exc_inf()[0])
		print("****************End****************")
    else:
       description="OtherOperation"
    return description
    # [END parse_message]


def poll_notifications(project, subscription_name):
    # [BEGIN poll_notifications]
    subscriber = pubsub.SubscriberClient()
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
