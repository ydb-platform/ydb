PY3_LIBRARY()

PEERDIR(
    ydb/tests/library
    ydb/tests/library/test_meta
)

SRCDIR(ydb/tests/functional/nbs)

PY_SRCS(
    common.py
    helpers.py
    vhost_user_blk_client.py
    fixtures/__init__.py
    fixtures/base.py
    fixtures/cluster.py
    fixtures/faults.py
    fixtures/geometry.py
    fixtures/markers.py
    fixtures/mon.py
)

END()
