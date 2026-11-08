# Keep operator import in a separate target: syntax, name resolution and
# scalar annotation can be tested without depending on the RBO operator layer.
YQL_LIBRARY()

SRCS(kqp_rbo_lowering.cpp)

PEERDIR(
    ydb/core/kqp/host/new_parser
    ydb/core/kqp/opt/rbo
)

END()
