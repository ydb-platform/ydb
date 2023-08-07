LIBRARY()

PEERDIR(
    contrib/libs/lz4
    library/cpp/actors/core
    library/cpp/actors/protos
    library/cpp/actors/util
    library/cpp/actors/wilson
    library/cpp/containers/stack_vector
    library/cpp/deprecated/atomic
    library/cpp/lwtrace
    library/cpp/monlib/dynamic_counters/percentile
    library/cpp/monlib/service/pages
    util
    ydb/core/base
    ydb/core/base/services
    ydb/core/blobstorage/base
    ydb/core/blobstorage/crypto
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/lwtrace_probes
    ydb/core/control
    ydb/core/driver_lib/version
    ydb/core/protos
    ydb/core/util
    ydb/library/pdisk_io
    ydb/library/schlab
    ydb/library/schlab/mon
    ydb/library/schlab/schine
)

GENERATE_ENUM_SERIALIZATION(blobstorage_pdisk_state.h)

SRCS(
    blobstorage_pdisk.cpp
    blobstorage_pdisk_actor.cpp
    blobstorage_pdisk_blockdevice_async.cpp
    blobstorage_pdisk_completion_impl.cpp
    blobstorage_pdisk_delayed_cost_loop.cpp
    blobstorage_pdisk_driveestimator.cpp
    blobstorage_pdisk_drivemodel_db.cpp
    blobstorage_pdisk_impl.cpp
    blobstorage_pdisk_impl_http.cpp
    blobstorage_pdisk_impl_log.cpp
    blobstorage_pdisk_internal_interface.cpp
    blobstorage_pdisk_log_cache.cpp
    blobstorage_pdisk_logreader.cpp
    blobstorage_pdisk_mon.cpp
    blobstorage_pdisk_params.cpp
    blobstorage_pdisk_requestimpl.cpp
    blobstorage_pdisk_syslogreader.cpp
    blobstorage_pdisk_sectorrestorator.cpp
    blobstorage_pdisk_tools.cpp
    blobstorage_pdisk_util_atomicblockcounter.cpp
    blobstorage_pdisk_util_flightcontrol.cpp
    blobstorage_pdisk_util_signal_event.cpp
    blobstorage_pdisk_writer.cpp
    drivedata_serializer.cpp
)

END()

RECURSE(
    mock
)

RECURSE_FOR_TESTS(
    ut
)
