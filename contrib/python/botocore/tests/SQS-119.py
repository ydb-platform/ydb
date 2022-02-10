import boto3
import unittest


class TestSqs119(unittest.TestCase):
    def test_builtin_aws_data(self):
        try:
            boto3.client('sqs', region_name='yandex')
        except:
            raise AssertionError('builtin AWS data is insufficient for SQS')

        try:
            boto3.client('s3', region_name='yandex')
        except:
            raise AssertionError('builtin AWS data is insufficient for S3')
