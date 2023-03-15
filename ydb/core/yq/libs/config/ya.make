LIBRARY()

SRCS(
    yq_issue.cpp
)

PEERDIR(
    ydb/core/yq/libs/config/protos
    ydb/library/yql/public/issue/protos
)

END()

RECURSE(
    protos
)
