LIBRARY()

SRCS(
    common.cpp
)

PEERDIR(
    ydb/library/conclusion
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/data_sharing/common/context
    ydb/core/tablet_flat
)

GENERATE_ENUM_SERIALIZATION(common.h)

END()
