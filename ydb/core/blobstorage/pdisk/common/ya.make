LIBRARY()

SRCDIR(
    ydb/core/blobstorage/pdisk
)

SRCS(
    blobstorage_pdisk_params.cpp
    blobstorage_pdisk_drivemodel_db.cpp
    drivedata_serializer.cpp
    defs.h
    blobstorage_pdisk.h
    blobstorage_pdisk_config.h
    blobstorage_pdisk_defs.h
    blobstorage_pdisk_params.h
    blobstorage_pdisk_drivemodel_db.h
    drivedata_serializer.h
    blobstorage_pdisk_data.h
    blobstorage_pdisk_crypto.h
    blobstorage_pdisk_state.h
    blobstorage_pdisk_signature.h
    blobstorage_pdisk_quota_record.h
    blobstorage_pdisk_util_space_color.h
)

GENERATE_ENUM_SERIALIZATION(blobstorage_pdisk_state.h)
GENERATE_ENUM_SERIALIZATION(blobstorage_pdisk_defs.h)

PEERDIR(
    library/cpp/monlib/service/pages
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/crypto
    ydb/core/blobstorage/pdisk/subsystem
    ydb/core/control/lib
    ydb/core/protos
    ydb/core/util
    ydb/library/actors/core
    ydb/library/pdisk_io
)

END()
