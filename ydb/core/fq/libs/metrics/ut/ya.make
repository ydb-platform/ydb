UNITTEST_FOR(ydb/core/fq/libs/metrics)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    metrics_ut.cpp
    sanitize_label_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
