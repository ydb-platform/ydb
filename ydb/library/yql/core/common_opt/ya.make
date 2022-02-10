LIBRARY()

OWNER(
    vvvv 
    g:yql
    g:yql_ydb_core
)

SRCS(
    yql_co.h
    yql_co_extr_members.cpp
    yql_co_finalizers.cpp
    yql_co_flow1.cpp
    yql_co_flow2.cpp
    yql_co_last.cpp
    yql_co_simple1.cpp
    yql_co_simple2.cpp
    yql_co_transformer.h
    yql_co_transformer.cpp
)

PEERDIR(
    ydb/library/yql/core
    ydb/library/yql/core/expr_nodes
)

YQL_LAST_ABI_VERSION() 

END()
