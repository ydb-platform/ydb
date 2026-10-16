PY3TEST()

TEST_SRCS(
    test_handlers.py
    test_process_profiles.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tools/ydbd_slice
)

END()
