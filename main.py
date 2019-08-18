#!/usr/bin/env python 
import json
import time
import random
import webapp2
import soundfile
import subprocess
from statistics import mode
from pydub import AudioSegment
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud import speech_v1p1beta1 as speech


class MainHandler(webapp2.RequestHandler):
    def get(self):
        self.response.write('Welcome 545!')

app = webapp2.WSGIApplication([
    ('/', MainHandler)
], debug=True)
