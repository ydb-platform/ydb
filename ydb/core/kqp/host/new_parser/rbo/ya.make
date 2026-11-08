YQL_LIBRARY()

SRCS(kqp_rbo_lowering.cpp)

PEERDIR(
    ydb/core/kqp/host/new_parser
    ydb/core/kqp/opt/rbo
)

END()
