LIBRARY()

SRCS(
    capture.cpp
)

PEERDIR(
    library/cpp/json
    ydb/core/kqp/opt/rbo
)

YQL_LAST_ABI_VERSION()

END()
