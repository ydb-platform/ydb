LIBRARY()

PROVIDES(YqlServicePolicy)

SRCS(
    GLOBAL udf_service.cpp
)

PEERDIR(
    contrib/ydb/library/yql/minikql
    yql/essentials/public/udf
)

YQL_LAST_ABI_VERSION()

END()
