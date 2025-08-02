UNITTEST()

ENV(YDB_USE_IN_MEMORY_PDISKS=true)

ENV(YDB_ERASURE=block_4-2)

ENV(YDB_FEATURE_FLAGS="enable_topic_transfer")
ENV(YDB_GRPC_SERVICES="replication")

PEERDIR(
    ydb/core/transfer/ut/common
)

SRCS(
    transfer_columntable_ut.cpp
    transfer_common.cpp
    transfer_rowtable_ut.cpp
    transfer_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

#TIMEOUT(60)
SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:24 cpu:4)
ELSE()
    REQUIREMENTS(ram:16 cpu:2)
ENDIF()

END()
