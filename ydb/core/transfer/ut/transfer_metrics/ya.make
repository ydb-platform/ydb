UNITTEST()

ENV(YDB_USE_IN_MEMORY_PDISKS=true)

ENV(YDB_ERASURE=none)

ENV(YDB_FEATURE_FLAGS="enable_topic_transfer,transfer_internal_data_decompression")
ENV(YDB_GRPC_SERVICES="replication")
ENV(YDB_ADDITIONAL_LOG_CONFIGS="PERSQUEUE:DEBUG")
ENV(YDB_REPORT_MONITORING_INFO="true")


IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck)
ENDIF()

PEERDIR(
    ydb/core/transfer/ut/common
)

SRCS(
    transfer_metrics_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

#TIMEOUT(60)
SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:24 cpu:4)
ELSE()
    REQUIREMENTS(ram:24 cpu:2)
ENDIF()

END()
