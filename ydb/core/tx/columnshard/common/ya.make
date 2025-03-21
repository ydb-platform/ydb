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
    ydb/library/services
    ydb/library/actors/core
    contrib/libs/apache/arrow
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/common/protos
)

GENERATE_ENUM_SERIALIZATION(portion.h)

END()
