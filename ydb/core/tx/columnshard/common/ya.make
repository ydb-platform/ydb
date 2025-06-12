LIBRARY()

SRCS(
    limits.cpp
    reverse_accessor.cpp
    scalars.cpp
    snapshot.cpp
    portion.cpp
    tablet_id.cpp
    blob.cpp
    volume.cpp
    path_id.cpp
)

PEERDIR(
    ydb/library/formats/arrow/protos
    contrib/libs/apache/arrow
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/common/protos
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/core/tx/columnshard/transactions/protos
    ydb/core/tx/columnshard/export/protos
    ydb/core/scheme/protos
)

GENERATE_ENUM_SERIALIZATION(portion.h)

END()
