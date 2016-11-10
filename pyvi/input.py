import os 
import re

class SentencesFromTextFile(object):
    def __init__(self,filename):
        self.filename=filename
    def __iter__(self):
        for line in open(self.filename):
            yield line 

class SentencesFromDirectory(object):
    def __init__(self,dirname):
        self.dirname = dirname
    def __iter__(self):
        for fname in os.listdir(self.dirname):
            for line in open(os.path.join(self.dirname, fname)):
                yield line 