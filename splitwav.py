import os
import subprocess
strSourceBucket='bkt-v7-1000'
strSourceBlobName='125536_SPLS_TestP2.wav'
strSourceBlobURL='gs://'+strSourceBucket+'/'+strSourceBlobName

strDestinationBucket='bkt-splitwav-destination-v7'
strDestinationBlobURL='gs://'+strDestinationBucket+'/'+strSourceBlobName
subprocess.call(['gsutil','cp',strSourceBlobURL,strDestinationBlobURL])
subprocess.call(['gsutil','cp',strDestinationBlobURL,'.'])
subprocess.call("ffmpeg -i "+strSourceBlobName+" -f segment -segment_time 1 -c copy out%03d.wav",shell=True)
#subprocess.call(['ffmpeg', '-i',strSourceBlobName,'-f','segment','-segment_time','1' ,'-c','copy', 'out%03d.wav'])
#subprocess.call(['ffmpeg', '-i','125536_SPLS_TestP2.wav','-f','segment','-segment_time','1' ,'-c','copy', 'out%03d.wav'])
