LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow
    ydb/core/protos
    ydb/core/testlib
    ydb/core/tx/columnshard
    ydb/core/tx/columnshard/blobs_action/bs
    ydb/core/tx/columnshard/engines/protos
    ydb/core/tx/long_tx_service/public
    ydb/core/wrappers
    ydb/library/actors/core
    ydb/library/formats/arrow/protos
    ydb/public/lib/value
    yql/essentials/core/arrow_kernels/request
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins
)

SRCS(
    inflight_test_enums.h
    helper.cpp
    controllers.cpp
    columnshard_ut_common.cpp
    shard_reader.cpp
    shard_writer.cpp
    kernels_wrapper.cpp
    program_constructor.cpp
)

GENERATE_ENUM_SERIALIZATION(inflight_test_enums.h)

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

