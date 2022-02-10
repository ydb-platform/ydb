YQL_UDF(yson2_udf)

YQL_ABI_VERSION(
    2
    21
    0
)

OWNER(g:yql)

SRCS(
    yson2_udf.cpp
)

PEERDIR(
    library/cpp/containers/stack_vector 
    library/cpp/yson_pull
    ydb/library/yql/minikql/dom 
)

END()
