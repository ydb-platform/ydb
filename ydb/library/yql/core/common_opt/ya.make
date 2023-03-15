LIBRARY()

SRCS(
    yql_co.h
    yql_co_extr_members.cpp
    yql_flatmap_over_join.cpp
    yql_co_finalizers.cpp
    yql_co_flow1.cpp
    yql_co_flow2.cpp
    yql_co_flowidaw1.cpp
    yql_co_last.cpp
    yql_co_pgselect.cpp
    yql_co_pgselect.h
    yql_co_simple1.cpp
    yql_co_simple2.cpp
    yql_co_simple3.cpp
    yql_co_transformer.cpp
    yql_co_transformer.h
)

PEERDIR(
    ydb/library/yql/core
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/parser/pg_catalog
)

YQL_LAST_ABI_VERSION()

END()
