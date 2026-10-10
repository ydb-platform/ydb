UNITTEST()

PEERDIR(
    ydb/core/testlib/default
    ydb/core/ymq/actor
)

SRCS(
    index_events_processor_ut.cpp
    test_events_writer.cpp
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

YQL_LAST_ABI_VERSION()

END()
