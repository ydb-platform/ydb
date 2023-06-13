LIBRARY()

SRCS(
    group_stat_aggregator.cpp
    node_warden_cache.cpp
    node_warden_group.cpp
    node_warden_group_resolver.cpp
    node_warden_impl.cpp
    node_warden_mon.cpp
    node_warden_pdisk.cpp
    node_warden_pipe.cpp
    node_warden_proxy.cpp
    node_warden_resource.cpp
    node_warden_scrub.cpp
    node_warden_stat_aggr.cpp
    node_warden_vdisk.cpp
)

PEERDIR(
    library/cpp/json
    ydb/core/base
    ydb/core/blob_depot/agent
    ydb/core/blobstorage/crypto
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/pdisk
    ydb/core/control
    ydb/library/pdisk_io
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_sequence
)
