LIBRARY()

SRCS(
    common.cpp
)

PEERDIR(
    ydb/library/conclusion
    ydb/library/yql/dq/actors/protos

    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()
