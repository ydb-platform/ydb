import boto3
import collections
import unittest
from unittest import mock

from botocore import loaders
from botocore.compat import six


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

    def test_services_are_published_whole_while_collected(self):
        # The cache is shared by every loader: another thread reads it while this one collects the services.
        visible = []
        collected_before_s3 = []
        resfs_files = loaders.resource.resfs_files

        def watched_resfs_files(*args, **kwargs):
            for name in resfs_files(*args, **kwargs):
                path = six.ensure_str(name)
                if path.startswith(loaders.HybridJsonLoader.arcadia_resources_path):
                    if '/data/s3/' in path and '/service-2.json' in path:
                        visible.append(dict(loaders.HybridJsonLoader.type_data_cache))
                    elif not visible and len(path.split('/data/')[1].split('/')) >= 3:
                        collected_before_s3.append(path)
                yield name

        # As in a new process: the services are not collected yet.
        empty = collections.defaultdict(lambda: collections.defaultdict(set))
        with mock.patch.object(loaders.HybridJsonLoader, 'type_data_cache', empty), mock.patch.object(
            loaders.resource, 'resfs_files', watched_resfs_files
        ):
            services = loaders.Loader().list_available_services('service-2')

        # Other services come before S3, so a cache filled in place would already show them without S3.
        self.assertTrue(collected_before_s3)
        self.assertEqual(visible, [{}])
        self.assertIn('s3', services)
