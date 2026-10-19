LIBRARY()

IF (YDB_ENABLE_PDISK_SHRED)
    CFLAGS(
        -DENABLE_PDISK_SHRED
    )
ENDIF()
IF (YDB_DISABLE_PDISK_ENCRYPTION)
    CFLAGS(
        -DDISABLE_PDISK_ENCRYPTION
    )
ENDIF()

PEERDIR(
    contrib/libs/lz4
    ydb/library/actors/core
    ydb/library/actors/protos
    ydb/library/actors/util
    ydb/library/actors/wilson
    library/cpp/containers/stack_vector
    library/cpp/deprecated/atomic
    library/cpp/lwtrace
    library/cpp/monlib/dynamic_counters/percentile
    library/cpp/monlib/service/pages
    util
    ydb/core/base
    ydb/core/base/services
    ydb/core/blobstorage/base
    ydb/core/blobstorage/pdisk/subsystem
    ydb/core/blobstorage/crypto
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/lwtrace_probes
    ydb/core/control/lib
    ydb/core/driver_lib/version
    ydb/core/protos
    ydb/core/util
    ydb/library/pdisk_io
    ydb/library/schlab
    ydb/library/schlab/mon
    ydb/library/schlab/schine
)

GENERATE_ENUM_SERIALIZATION(blobstorage_pdisk_state.h)
GENERATE_ENUM_SERIALIZATION(blobstorage_pdisk_defs.h)


# the sources that include blobstorage_pdisk_impl.h; the groups follow YDB_LOG_THIS_FILE_COMPONENT, which a
# source may define only once per translation unit
JOIN_SRCS(
    all_pdisk_impl_1.cpp
    blobstorage_pdisk_actor.cpp
    blobstorage_pdisk_blockdevice_async.cpp
    blobstorage_pdisk_impl_metadata.cpp
    blobstorage_pdisk_completion_impl.cpp
)

JOIN_SRCS(
    all_pdisk_impl_2.cpp
    blobstorage_pdisk_impl.cpp
    blobstorage_pdisk_impl_http.cpp
    blobstorage_pdisk_logreader.cpp
)

JOIN_SRCS(
    all_pdisk_impl_3.cpp
    blobstorage_pdisk_impl_log.cpp
    blobstorage_pdisk_syslogreader.cpp
    blobstorage_pdisk_tools.cpp
)

JOIN_SRCS(
    all_pdisk_1.cpp
    blobstorage_pdisk.cpp
    blobstorage_pdisk_compaction_arbiter.cpp
    blobstorage_pdisk_delayed_cost_loop.cpp
    blobstorage_pdisk_driveestimator.cpp
    blobstorage_pdisk_drivemodel_db.cpp
    blobstorage_pdisk_internal_interface.cpp
    blobstorage_pdisk_log_cache.cpp
    blobstorage_pdisk_mon.cpp
)

JOIN_SRCS(
    all_pdisk_2.cpp
    blobstorage_pdisk_requestimpl.cpp
    blobstorage_pdisk_sectorrestorator.cpp
    blobstorage_pdisk_util_atomicblockcounter.cpp
    blobstorage_pdisk_util_flightcontrol.cpp
    blobstorage_pdisk_util_signal_event.cpp
    blobstorage_pdisk_writer.cpp
    drivedata_serializer.cpp
)

# Not joined: the VDisk tests without a PDisk take only TPDiskParams from this library. Joined with the other
# sources, it would pull in all of the PDisk, with the compatibility info that only binaries define
# (ydb/apps/version), and those tests would not link.
SRCS(
    blobstorage_pdisk_params.cpp
)

END()

RECURSE(
    metadata
    mock
)

RECURSE_FOR_TESTS(
    ut
)
