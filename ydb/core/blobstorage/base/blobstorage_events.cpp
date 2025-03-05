#include "blobstorage_events.h"
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>

namespace NKikimr {

    TEvNodeWardenStorageConfig::TEvNodeWardenStorageConfig(const NKikimrBlobStorage::TStorageConfig& config,
            const NKikimrBlobStorage::TStorageConfig *proposedConfig, bool selfManagementEnabled)
        : Config(std::make_unique<NKikimrBlobStorage::TStorageConfig>(config))
        , ProposedConfig(proposedConfig
            ? std::make_unique<NKikimrBlobStorage::TStorageConfig>(*proposedConfig)
            : nullptr)
        , SelfManagementEnabled(selfManagementEnabled)
    {}

    TEvNodeWardenStorageConfig::~TEvNodeWardenStorageConfig()
    {}

} // NKikimr
