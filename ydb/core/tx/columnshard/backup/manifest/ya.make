LIBRARY()

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ENDIF()

SRCS(
    manifest.cpp
    manifest_actor.cpp
)

PEERDIR(
    library/cpp/json
    ydb/core/backup/common
    ydb/core/tx/columnshard/engines/protos  # stopgap: columnshard_private_events.h transitively requires engines/protos; direct columnshard dep would create a cycle
    ydb/core/tx/datashard
    ydb/core/wrappers
    ydb/library/actors/core
    ydb/library/conclusion
    ydb/library/services
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
