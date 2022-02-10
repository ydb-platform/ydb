LIBRARY()

OWNER(
    spuchin
    g:kikimr
)

SRCS(
    kqp_query_analyze.cpp
    kqp_query_exec.cpp
    kqp_query_finalize.cpp
    kqp_query_plan.cpp
    kqp_query_rewrite.cpp
    kqp_query_simplify.cpp
    kqp_query_substitute.cpp
    kqp_type_ann.cpp
)

PEERDIR(
    ydb/core/engine
    ydb/core/kqp/common
    ydb/library/yql/dq/actors/protos 
    ydb/library/yql/dq/type_ann 
)

YQL_LAST_ABI_VERSION()
 
GENERATE_ENUM_SERIALIZATION(kqp_prepare_impl.h)

END()
