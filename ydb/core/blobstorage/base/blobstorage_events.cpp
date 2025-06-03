#include "blobstorage_events.h"
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>

namespace NKikimr {

    TEvNodeWardenStorageConfig::TEvNodeWardenStorageConfig(std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> config,
            std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> proposedConfig, bool selfManagementEnabled,
            TBridgeInfo::TPtr bridgeInfo)
        : Config(std::move(config))
        , ProposedConfig(std::move(proposedConfig))
        , SelfManagementEnabled(selfManagementEnabled)
        , BridgeInfo(std::move(bridgeInfo))
    {}

    TEvNodeWardenStorageConfig::~TEvNodeWardenStorageConfig()
    {}

} // NKikimr
