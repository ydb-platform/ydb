LIBRARY()

SRCS(
    ../nbs_dbg_like_alloc_helper.cpp
    ../nbs_dbg_like_alloc_helper.h
    ../nbs_dbg_like_load.cpp
    ../nbs_dbg_like_load.h
    ../nbs_dbg_like_load_defs.h
    ../nbs_dbg_like_load_service.cpp
    ../nbs_dbg_like_load_service.h
    ../nbs_dbg_like_load_tablet.cpp
    ../nbs_dbg_like_load_tablet.h
)

PEERDIR(
    library/cpp/containers/absl
    library/cpp/histogram/hdr
    library/cpp/http/fetch
    library/cpp/json
    library/cpp/json/writer
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/dynamic_counters/percentile
    library/cpp/monlib/service
    library/cpp/monlib/service/pages
    ydb/core/base
    ydb/core/base/services
    ydb/core/blobstorage/base
    ydb/core/blobstorage/ddisk
    ydb/core/keyvalue
    ydb/core/load_test/common
    ydb/core/mind/hive
    ydb/core/mon
    ydb/core/nbs/cloud/storage/core/protos
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx/scheme_cache
    ydb/core/util
    ydb/library/actors/core
    ydb/library/actors/wilson
    ydb/library/wilson_ids
    ydb/public/lib/base
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(../nbs_dbg_like_load_defs.h)

IF (OS_LINUX)
    SRCS(../nbs2_load_actor.cpp)

    PEERDIR(
        library/cpp/protobuf/json
        library/cpp/time_provider
        ydb/core/nbs/cloud/blockstore/libs/common
        ydb/core/nbs/cloud/blockstore/libs/service
        ydb/core/nbs/cloud/blockstore/libs/storage/api
        ydb/core/nbs/cloud/blockstore/tools/testing/loadtest/lib
        ydb/core/nbs/cloud/storage/core/libs/common
        ydb/core/nbs/cloud/storage/core/libs/diagnostics
        ydb/library/workload/abstract
        ydb/library/workload/kv
        ydb/library/workload/stock
        ydb/public/sdk/cpp/src/client/proto
        ydb/public/sdk/cpp/src/library/operation_id
    )
ENDIF()

END()
