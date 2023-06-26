# -*- coding: utf-8 -*-
import os
import json
import yatest.common as ya_common


class TestYaTestContext(object):
    def test_source_path(self):
        resource_path = 'ydb/tests/functional/open_source/resource.txt'
        arcadia_root = ya_common.source_path('')
        ya_source_path = os.path.join(arcadia_root, ya_common.test_source_path('resource.txt'))

        context_file = os.getenv('YA_TEST_CONTEXT_FILE')

        if context_file:
            with open(context_file, 'r') as content:
                context_source_path = os.path.join(json.load(content)['runtime']['source_root'], resource_path)
                assert ya_source_path == context_source_path

        assert os.path.exists(ya_source_path)
