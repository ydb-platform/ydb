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
    ydb/core/blobstorage/subsystem/interface
    ydb/core/formats
    ydb/core/node_whiteboard
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/testlib/actors
    ydb/core/tx/scheme_board
    ydb/library/actors/dnsresolver
    ydb/library/actors/interconnect
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
