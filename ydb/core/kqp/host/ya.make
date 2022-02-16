LIBRARY()

OWNER(
    spuchin
    g:kikimr
)

SRCS(
    kqp_host.cpp
    kqp_ne_helper.cpp
    kqp_run_data.cpp
    kqp_explain_prepared.cpp
    kqp_run_physical.cpp
    kqp_run_prepared.cpp
    kqp_run_scan.cpp
    kqp_runner.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common
    ydb/core/kqp/opt
    ydb/core/kqp/prepare
    ydb/core/kqp/provider
    ydb/library/yql/core/services
    ydb/library/yql/minikql/invoke_builtins
    ydb/library/yql/sql
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/core
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/udf_resolve
    ydb/library/yql/providers/config
    ydb/library/yql/providers/result/provider
)

YQL_LAST_ABI_VERSION()

END()
