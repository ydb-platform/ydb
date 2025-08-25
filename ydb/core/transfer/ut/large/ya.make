UNITTEST()

ENV(YDB_USE_IN_MEMORY_PDISKS=true)

ENV(YDB_ERASURE=block_4-2)

ENV(YDB_FEATURE_FLAGS="enable_topic_transfer")
ENV(YDB_GRPC_SERVICES="replication")

PEERDIR(
    ydb/core/transfer/ut/common
)

SRCS(
    transfer_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

SIZE(LARGE)
TAG(ya:fat)

IF (SANITIZER_TYPE)
    TAG(ya:not_autocheck)
ELSE()
    REQUIREMENTS(ram:32 cpu:all)
ENDIF()

END()
