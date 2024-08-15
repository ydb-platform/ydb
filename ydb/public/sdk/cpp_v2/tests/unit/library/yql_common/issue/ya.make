UNITTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

FORK_SUBTESTS()

SRCS(
    yql_issue_ut.cpp
)

PEERDIR(
    library/cpp/unicode/normalization
    ydb/public/sdk/cpp_v2/src/library/yql_common/issue
)

END()
