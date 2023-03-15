LIBRARY()

SRCS(
    kqp_mkql_compiler.cpp
    kqp_olap_compiler.cpp
    kqp_query_compiler.cpp
)

PEERDIR(
    ydb/core/formats
    ydb/core/kqp/common
    ydb/core/protos
    ydb/library/mkql_proto
)

YQL_LAST_ABI_VERSION()

END()
