PROGRAM()

IF (OS_LINUX)
    # Relink with a trace to compare the cost of generating the GDB index.
    ENABLE(LINKER_TIME_TRACE)
    LDFLAGS(-Wl,--no-gdb-index)
ENDIF()

SRCS(
    main.cpp
    proxy.cpp
)

PEERDIR(
    ydb/core/base
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/discovery
    ydb/library/grpc/server
    ydb/apps/etcd_proxy/service
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
)

END()
