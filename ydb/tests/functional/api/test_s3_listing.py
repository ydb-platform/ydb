#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from hamcrest import assert_that, equal_to, not_, contains

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
import ydb as ydb


logger = logging.getLogger(__name__)


class TestS3ListingAPI(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig("%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port)))
        cls.s3client = ydb.S3InternalClient(cls.driver)
        cls.pool = ydb.SessionPool(cls.driver)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'kikimr_cluster'):
            cls.kikimr_cluster.stop()

    def _prepare_test_data(self, table, bucket, paths):
        with self.pool.checkout() as session:
            session.execute_scheme("""
                create table `{table_name}` (
                    BucketName Utf8,
                    DataMd5 String,
                    DataSize Uint64,
                    DataSource String,
                    DataSourceType String,
                    Metadata String,
                    MetadataType String,
                    Name utf8,
                    PartsCount Uint64,
                    UploadStartedUsec Uint64,
                    primary key (BucketName, Name)
                );
            """.format(table_name=table))

        for path in paths:
            with self.pool.checkout() as session:
                session.transaction().execute(
                    "upsert into `{table_name}` (BucketName, Name, Metadata) values"
                    "    ('{bucket}', '{path}', 'some data');".format(bucket=bucket, table_name=table, path=path),
                    commit_tx=True
                )

    def test_s3_listing_full(self):
        table = '/Root/S3/Objects'

        bucket1_paths = ['/home/test_{name}'.format(name=x) for x in range(50)]
        bucket1_paths.extend(['/home/test_{name}/main.cpp'.format(name=x+10) for x in range(30)])

        self._prepare_test_data(table, 'Bucket1', bucket1_paths)

        bucket2_paths = ['asdf', 'boo/bar', 'boo/baz/xyzzy', 'cquux/thud', 'cquux/bla']
        self._prepare_test_data(table, 'Bucket2', bucket2_paths)

        self.do_test(bucket1_paths, table, 'Bucket1', '', '/', 7, ['DataMd5', 'Metadata'])
        self.do_test(bucket1_paths, table, 'Bucket1', '', '', 7, ['DataMd5', 'Metadata'])
        self.do_test(bucket1_paths, table, 'Bucket1', '/home', '/', 7, ['DataMd5', 'Metadata'])
        self.do_test(bucket1_paths, table, 'Bucket1', '/home/', '/', 7, ['DataMd5', 'Metadata'])
        self.do_test(bucket1_paths, table, 'Bucket1', '/etc/', '/', 7, ['DataMd5', 'Metadata'])
        self.do_test(bucket1_paths, table, 'Bucket1', '/etc/', '', 7, ['DataMd5', 'Metadata'])
        self.do_test(bucket1_paths, table, 'Bucket1', '/home/te', '/', 7, ['DataMd5', 'Metadata'])
        self.do_test(bucket1_paths, table, 'Bucket1', '/home/te', '', 7, ['DataMd5', 'Metadata'])
        self.do_test(bucket1_paths, table, 'Bucket1', '/home/test_1', '', 7, ['DataMd5', 'Metadata'])
        self.do_test(bucket1_paths, table, 'Bucket1', '/home/test_1', '/', 7, ['DataMd5', 'Metadata'])
        self.do_test(bucket1_paths, table, 'Bucket1', '/home/test_1/', '', 7, ['DataMd5', 'Metadata'])
        self.do_test(bucket1_paths, table, 'Bucket1', '/home/test_1/', '/', 7, ['DataMd5', 'Metadata'])

        for max_keys in range(2, 10):
            self.do_test(bucket2_paths, table, 'Bucket2', '', '', max_keys, ['DataMd5', 'Metadata'])
            self.do_test(bucket2_paths, table, 'Bucket2', '', '/', max_keys, ['DataMd5', 'Metadata'])
            self.do_test(bucket1_paths, table, 'Bucket1', '/home/', '/', max_keys, ['DataMd5', 'Metadata'])

    def test_s3_listing_max_keys(self):
        table = '/Root/S3/Objects'

        bucket2_paths = ['asdf', 'boo/bar', 'boo/baz/xyzzy', 'cquux/thud', 'cquux/bla']
        self._prepare_test_data(table, 'Bucket2', bucket2_paths)

        result = self.s3client.s3_listing(table, ['Bucket2'], '', '/', 2, [], '')
        logger.debug(result)
        assert_that(result.common_prefixes, equal_to(['boo/']))
        assert_that(len(result.contents), equal_to(1))
        assert_that(result.contents[0][0], equal_to('asdf'))

    def do_test(self, all_paths, table, bucket, prefix, delimiter, step, columns):
        # compares real result from s3 listing reques to the expected result
        prefixes, contents = self.do_s3_listing(table, [bucket], prefix, delimiter, step, columns)
        logger.debug(prefixes)
        logger.debug(contents)
        expected_prefixes, expected_contents = self.emulate_listing(all_paths, prefix, delimiter)
        logger.debug(expected_prefixes)
        logger.debug(expected_contents)
        assert_that(prefixes, equal_to(expected_prefixes))
        assert_that(contents, equal_to(expected_contents))

    def do_s3_listing(self, table, prefix_columns, path_prefix, delimiter, step, columns):
        start_after = None
        more_data = True
        all_common_prefixes = set()
        all_contents = set()
        for i in range(1, 50):
            result = self.s3client.s3_listing(
                table, prefix_columns, path_prefix, delimiter, step, columns, start_after)
            # logger.debug(result)

            for prefix in result.common_prefixes:
                assert_that(all_common_prefixes, not_(contains(prefix)))
                all_common_prefixes.add(prefix)

            for obj in result.contents:
                path = obj[0]
                assert_that(all_contents, not_(contains(path)))
                all_contents.add(path)

            more_data = result.is_truncated
            if more_data:
                # logger.debug('ContinueAfter: ' + str(result.continue_after))
                start_after = result.continue_after
            else:
                break
        assert_that(not more_data)
        return all_common_prefixes, all_contents

    def emulate_listing(self, all_paths, path_prefix, delimiter):
        # filter all_paths based on prefix and delimiter
        # in order to produce the result similar to s3 listing logic
        all_common_prefixes = set()
        all_contents = set()
        for p in all_paths:
            if not p.startswith(path_prefix):
                continue
            if delimiter == '':
                all_contents.add(p)
                continue
            delim_pos = p.find(delimiter, len(path_prefix))
            if delim_pos == -1:
                all_contents.add(p)
            else:
                all_common_prefixes.add(p[:delim_pos+1])

        return all_common_prefixes, all_contents
