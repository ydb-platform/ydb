import boto3
import time
import os


class Connection:
    def __init__(self, access_key_id, secret_access_key, op_id):
        self.client = boto3.session.Session().client(
            service_name='s3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            endpoint_url='http://storage.yandexcloud.net'
        )
        self.bucket = 'yql-perf'
        self.folder = op_id

    def upload(self, file, destination=None):
        if destination is None:
            destination = file

        self.client.upload_file(file, self.bucket, self.folder + '/' + destination)

    def upload_dir(self, dir, suffix=''):
        for path, _, files in os.walk(dir):
            for file in files:
                if file.endswith(suffix):
                    self.upload(os.path.join(path, file))

    def download(self, file, wait_period_seconds=15):
        print(f'Waiting for {file} to appear in s3')
        print('Ctrl-C once to abort')
        while True:
            try:
                self.client.download_file(self.bucket, f'{self.folder}/{file}', file)
                return
            except Exception:
                time.sleep(wait_period_seconds)

    def download_dir(self, suffix=''):
        files = self.client.list_objects_v2(Bucket=self.bucket, Prefix=self.folder)['Contents']
        for file in files:
            if file['Key'].endswith(suffix) and file['Key'].count('/') == 1:
                short_name = file['Key'][len(self.folder) + 1:]
                self.download(short_name)

    def delete_folder(self):
        files = self.client.list_objects_v2(Bucket=self.bucket, Prefix=self.folder)['Contents']
        for file in files:
            self.client.delete_object(Bucket=self.bucket, Key=file['Key'])
