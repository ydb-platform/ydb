PY3_LIBRARY()

VERSION(8893cc49c5a977b44b8566d43dd1c214ad0411fa)

LICENSE(MIT)

PEERDIR(
    contrib/python/PyYAML
    contrib/deprecated/python/nose
    contrib/python/boto
    contrib/python/munch
    contrib/python/isodate
    contrib/python/requests
    contrib/python/pytz
    contrib/python/httplib2
    contrib/python/lxml
)

SRCDIR(
    contrib/python/s3-tests
)

PY_SRCS(
    TOP_LEVEL
    s3tests/__init__.py
    s3tests/common.py
    s3tests/functional/__init__.py
    s3tests/functional/policy.py
    s3tests/functional/utils.py
)

TEST_SRCS(
    s3tests/functional/test_headers.py
    s3tests/functional/test_s3.py
    s3tests/functional/test_s3_website.py
    s3tests/functional/test_utils.py
)

NO_LINT()

END()
