UNITTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

FORK_SUBTESTS()

SRCS(
    yql_issue_ut.cpp
)

PEERDIR(
    library/cpp/unicode/normalization
    ydb/public/sdk/cpp/src/library/yql_common/issue
)

END()
