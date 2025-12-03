#include "blobstorage_events.h"
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>

namespace NKikimr {

    TEvNodeWardenStorageConfig::TEvNodeWardenStorageConfig(std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> config,
            bool selfManagementEnabled, TBridgeInfo::TPtr bridgeInfo,
            std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> committedConfig)
        : Config(std::move(config))
        , SelfManagementEnabled(selfManagementEnabled)
        , BridgeInfo(std::move(bridgeInfo))
        , CommittedConfig(std::move(committedConfig))
    {}

    TEvNodeWardenStorageConfig::~TEvNodeWardenStorageConfig()
    {}

} // NKikimr
