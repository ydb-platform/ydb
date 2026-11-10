PROGRAM()

IF (OS_LINUX)
    # Compare mold with LLD without a GDB index on the same inputs.
    SET(_LINKER_ID mold)
    LDFLAGS(
        -Wl,--no-gdb-index
        -Wl,--no-rosegment
        -Wl,--build-id=sha1
        -Wl,--perf
    )
    # Preserve the non-PIE flag normally added only for LLD.
    IF (NOT PIE)
        IF (NOT PIC OR PIC_NO_PIE)
            LDFLAGS(-Wl,-no-pie)
        ENDIF()
    ENDIF()
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
