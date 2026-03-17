PY3_LIBRARY()

VERSION(8893cc49c5a977b44b8566d43dd1c214ad0411fa)

LICENSE(MIT)

PEERDIR(
    contrib/python/PyYAML
    contrib/deprecated/python/nose
    contrib/python/boto
    contrib/python/aiobotocore
    contrib/python/boto3
    contrib/python/munch
    contrib/python/isodate
    contrib/python/requests
    contrib/python/pytz
    contrib/python/lxml
)

SRCDIR(
    contrib/python/s3-tests
)

PY_SRCS(
    TOP_LEVEL
    s3tests_boto3/__init__.py
    s3tests_boto3/common.py
    s3tests_boto3/functional/__init__.py
    s3tests_boto3/functional/policy.py
    s3tests_boto3/functional/utils.py
)

TEST_SRCS(
    s3tests_boto3/functional/test_headers.py
    s3tests_boto3/functional/test_s3.py
    s3tests_boto3/functional/test_s3select.py
    s3tests_boto3/functional/test_s3_ext.py
    s3tests_boto3/functional/test_patch.py
    s3tests_boto3/functional/test_utils.py
)

NO_LINT()

END()
