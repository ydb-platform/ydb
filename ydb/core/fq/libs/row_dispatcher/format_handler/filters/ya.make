LIBRARY()

SRCS(
    purecalc_filter.cpp
    filters_set.cpp
)

PEERDIR(
    ydb/core/fq/libs/actors/logging
    ydb/core/fq/libs/row_dispatcher/events
    ydb/core/fq/libs/row_dispatcher/format_handler/common
    ydb/core/fq/libs/row_dispatcher/purecalc_no_pg_wrapper

    ydb/library/actors/core

    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/invoke_builtins
    yql/essentials/providers/common/schema/parser
    yql/essentials/public/udf
)

YQL_LAST_ABI_VERSION()

END()
