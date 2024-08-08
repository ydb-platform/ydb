UNITTEST_FOR(ydb/library/yql/providers/common/codec)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    yql_json_codec_ut.cpp
    yql_restricted_yson_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
