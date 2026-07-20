PY3TEST()

TEST_SRCS(
    test_smoke.py
    test_timing.py
    test_worklist.py
    test_concurrency.py
    test_patch.py
    test_pilot.py
    test_bench.py
)

PEERDIR(
    ydb/tools/include_sanitizer
)

END()
