PY3_LIBRARY()

PY_SRCS(
    s3.py
    utils.py
)

PEERDIR(
    contrib/python/boto3/py3
    contrib/python/requests/py3
)

END()
