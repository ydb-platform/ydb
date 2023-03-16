import pytest
from ydb.tests.oss.canonical import canons_meta, is_oss


@pytest.fixture(scope='function', autouse=is_oss)
def set_canondata_directory(request):
    test_info = request.node._nodeid.split('::')
    if len(test_info) > 2:
        test_filename = test_info[0].split('/')[-1][:-3]
        test_classname = test_info[1]
        test_name = test_info[2]
        for c in ['[', ']', '/']:
            test_name = test_name.replace(c, '_')
        canons_meta.directory = '.'.join([test_filename, test_classname, test_name])
