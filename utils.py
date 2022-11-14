import logging
import os
import os.path as osp
import boto3
from botocore.exceptions import ClientError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S')

s3 = boto3.client('s3',
  endpoint_url = os.environ['S3_HOST_URL'],
  aws_access_key_id = os.environ['S3_ACCESS_KEY'],
  aws_secret_access_key = os.environ['S3_SECRET_KEY']
)


def getConfig(bucketName, remoteFile):
    localFile = osp.basename(remoteFile)
    s3.download_file(bucketName, remoteFile, localFile)
    return localFile

def save(bucketName, remotePath, file):
    if osp.isfile(file):
        try:
            s3.upload_file(file, bucketName, remotePath)
            return True
        except ClientError:
            logging.exception("message")
            return False
    return False

class EventHandler(FileSystemEventHandler):

    def __init__(self, bucketName, remoteDir, basePath, logger=None):
        super(EventHandler, self).__init__()
        self.logger = logger or logging.root
        self.bucketName = bucketName
        self.remoteDir = remoteDir
        self.basePath = basePath

    def on_moved(self, event):
        super(EventHandler, self).on_moved(event)

        if event.is_directory:
            return
        self.logger.info("Moved: from %s to %s", event.src_path,
                         event.dest_path)

    def on_created(self, event):
        super(EventHandler, self).on_created(event)

        if event.is_directory:
            return
        self.logger.info("Created: %s", event.src_path)
        
        remotePath = osp.join(self.remoteDir, event.src_path.replace(self.basePath, ""))
        if save(self.bucketName, remotePath, event.src_path):
            self.logger.info("Saved %s to S3", event.src_path)
        else:
            self.logger.error("Failed to save %s to S3", event.src_path)

    def on_deleted(self, event):
        super(EventHandler, self).on_deleted(event)

        if event.is_directory:
            return
        self.logger.info("Deleted: %s", event.src_path)

    def on_modified(self, event):
        super(EventHandler, self).on_modified(event)

        if event.is_directory or '.part' in event.src_path:
            return
        self.logger.info("Modified: %s", event.src_path)
        remotePath = osp.join(self.remoteDir, event.src_path.replace(self.basePath, ""))
        if save(self.bucketName, remotePath, event.src_path):
            self.logger.info("Saved %s to S3", event.src_path)
        else:
            self.logger.error("Failed to save %s to S3", event.src_path)
    
            
def start_watching(bucketName="discoart", outputPath = "/discoart/", remoteDir=None):
    event_handler = EventHandler(bucketName,  remoteDir, outputPath)
    observer = Observer()
    observer.schedule(event_handler, outputPath, recursive=True)
    observer.start()
    return observer

if __name__ == '__main__':
    event_handler = EventHandler()
    observer = Observer()
    observer.schedule(event_handler, '/discoart', recursive=True)
    observer.start()
    observer.join() 