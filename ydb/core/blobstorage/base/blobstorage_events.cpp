#include "blobstorage_events.h"
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>

namespace NKikimr {

    TEvNodeWardenStorageConfig::TEvNodeWardenStorageConfig(const NKikimrBlobStorage::TStorageConfig& config)
        : Config(std::make_unique<NKikimrBlobStorage::TStorageConfig>(config))
    {}

    TEvNodeWardenStorageConfig::~TEvNodeWardenStorageConfig()
    {}

} // NKikimr
