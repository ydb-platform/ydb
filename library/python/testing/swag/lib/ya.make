PY23_LIBRARY()

PEERDIR(
    contrib/python/six
    contrib/python/protobuf
    library/python/testing/yatest_common
)

SRCDIR(library/python/testing/swag)

PY_SRCS(
    NAMESPACE library.python.testing.swag

    daemon.py
    gdb.py
    pathutil.py
    ports.py
    proto_traversals.py
)

END()
