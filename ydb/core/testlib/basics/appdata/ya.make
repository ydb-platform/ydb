LIBRARY()

SRCS(
    ../appdata.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/formats
    ydb/core/protos
    ydb/core/scheme
    ydb/core/testlib/actors
    ydb/core/tx/schemeshard
    ydb/library/pdisk_io
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

IF (GCC)
    CFLAGS(
        -fno-devirtualize-speculatively
    )
ENDIF()

END()
