LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

PEERDIR(
    yql/essentials/public/issue
    ydb/library/yql/public/ydb_issue
    ydb/public/sdk/cpp/src/library/issue
)

SRCS(
    issue.cpp
)

END()
