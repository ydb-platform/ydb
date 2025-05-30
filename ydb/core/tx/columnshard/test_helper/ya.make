LIBRARY()

PEERDIR(
    ydb/core/protos
    ydb/core/formats/arrow
    contrib/libs/apache/arrow
    ydb/library/actors/core
    ydb/core/tx/columnshard/blobs_action/bs
    ydb/library/formats/arrow/protos
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins
    yql/essentials/core/arrow_kernels/request
    ydb/core/tx/columnshard
    ydb/core/wrappers
)

SRCS(
    helper.cpp
    controllers.cpp
    columnshard_ut_common.cpp
    shard_reader.cpp
    shard_writer.cpp
    kernels_wrapper.cpp
    program_constructor.cpp
)

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ELSE()
    PEERDIR(
        ydb/core/tx/columnshard/blobs_action/tier
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()

