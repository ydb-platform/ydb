LIBRARY()

OWNER(
    spuchin
    g:kikimr
)

SRCS(
    kqp_common.cpp
    kqp_common.h
    kqp_resolve.cpp
    kqp_resolve.h
    kqp_ru_calc.cpp
    kqp_transform.cpp
    kqp_transform.h
    kqp_yql.cpp
    kqp_yql.h
    kqp_timeouts.h
    kqp_timeouts.cpp
    kqp_lwtrace_probes.h
    kqp_lwtrace_probes.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/engine
    ydb/core/kqp/expr_nodes
    ydb/core/kqp/provider
    ydb/library/aclib
    ydb/library/yql/core/issue
    ydb/library/yql/dq/actors
    ydb/library/yql/dq/common
    library/cpp/lwtrace
    #library/cpp/lwtrace/protos
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(kqp_tx_info.h)
GENERATE_ENUM_SERIALIZATION(kqp_yql.h)

END()
