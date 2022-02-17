UNITTEST_FOR(ydb/library/yql/providers/common/codec)

OWNER(g:yql)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    yql_json_codec_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
