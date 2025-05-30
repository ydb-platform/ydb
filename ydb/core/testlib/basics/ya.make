LIBRARY()

SRCS(
    appdata.cpp
    helpers.cpp
    runtime.cpp
    services.cpp
)

PEERDIR(
    ydb/library/actors/dnsresolver
    library/cpp/regex/pcre
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/blobstorage
    ydb/core/blobstorage/crypto
    ydb/core/blobstorage/nodewarden
    ydb/core/blobstorage/pdisk
    ydb/core/client/server
    ydb/core/control
    ydb/core/formats
    ydb/core/mind
    ydb/core/node_whiteboard
    ydb/core/quoter
    ydb/core/statistics/service
    ydb/core/tablet_flat
    ydb/core/testlib/actors
    ydb/core/tx/columnshard
    ydb/core/tx/scheme_board
    ydb/core/tx/schemeshard
    ydb/core/util
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/exception_policy
    ydb/services/kesus
    ydb/services/persqueue_cluster_discovery
    ydb/services/ydb
)

YQL_LAST_ABI_VERSION()

IF (GCC)
    CFLAGS(
        -fno-devirtualize-speculatively
    )
ENDIF()

END()

RECURSE(
    default
    pg
)
