PY3TEST()

IF (SANITIZER_TYPE)
    TIMEOUT(600)
ELSE()
    TIMEOUT(120)
ENDIF()

SIZE(MEDIUM)

TEST_SRCS(
    test_cluster_config_validation.py
    test_navigation.py
)

PEERDIR(
    ydb/tools/mnc/viewer
)

END()
