LIBRARY()

SRCS(
    appdata.cpp
    helpers.cpp
    runtime.cpp
    services.cpp
    setup.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/subsystem
    ydb/core/formats
    ydb/core/node_whiteboard
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/testlib/actors
    ydb/core/tx/scheme_board
    ydb/library/actors/dnsresolver
    ydb/library/actors/interconnect
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
