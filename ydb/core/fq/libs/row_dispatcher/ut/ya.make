UNITTEST_FOR(ydb/core/fq/libs/row_dispatcher)

SRCS(
    leader_detector_ut.cpp
    json_parser_ut.cpp
    json_filter_ut.cpp
)

PEERDIR(
    ydb/core/fq/libs/row_dispatcher
    library/cpp/testing/unittest
    #ydb/core/testlib/actors
    ydb/core/testlib
    ydb/library/yql/udfs/common/json2
    ydb/library/yql/udfs/common/yson2
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
