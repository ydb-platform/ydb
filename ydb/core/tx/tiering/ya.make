LIBRARY()

SRCS(
    common.cpp
    tier_cleaner.cpp
    path_cleaner.cpp
    GLOBAL cleaner_task.cpp
    manager.cpp
    GLOBAL external_data.cpp
    snapshot.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/json/writer
    ydb/core/blobstorage
    ydb/core/protos
    ydb/core/tx/tiering/rule
    ydb/core/tx/tiering/tier
    ydb/core/tablet_flat/protos
    ydb/core/wrappers
    ydb/public/api/protos
    ydb/services/bg_tasks/abstract
    ydb/services/metadata
)

END()

RECURSE_FOR_TESTS(
    ut
)