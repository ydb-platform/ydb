BUILD_ONLY_IF(OS_LINUX)

PY3TEST()

TEST_SRCS(
    test_service_symbols.py
)

PEERDIR(
    contrib/python/pyelftools
)

DEPENDS(
    yql/essentials/udfs/test/policy_probe
)

END()
