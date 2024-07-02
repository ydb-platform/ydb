LIBRARY()

SRCS(
    datetime.cpp
    datetime64.cpp
)

PEERDIR(
    ydb/library/yql/minikql/computation
)

YQL_LAST_ABI_VERSION()

END()
