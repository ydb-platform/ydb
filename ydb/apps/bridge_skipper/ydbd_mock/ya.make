PY3_PROGRAM(ydbd_mock)

PY_MAIN(ydb.apps.bridge_skipper.ydbd_mock.main:entry)

PY_SRCS(
    __init__.py
    main.py
)

PEERDIR(
    ydb/apps/bridge_skipper/ydbd_mock/lib
)

END()

RECURSE(
    lib
)

RECURSE_FOR_TESTS(
    tests
)
