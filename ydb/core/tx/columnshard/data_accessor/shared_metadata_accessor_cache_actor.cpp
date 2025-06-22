#include "shared_metadata_accessor_cache_actor.h"

#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NOlap::NDataAccessorControl {

NActors::IActor* TSharedMetadataAccessorCacheActor::CreateActor() {
    return new TSharedMetadataAccessorCacheActor();
}

void TSharedMetadataAccessorCacheActor::Bootstrap() {
    AccessorsCallback = std::make_shared<TActorAccessorsCallback>(SelfId());
    if (HasAppData()) {
        TotalMemorySize = AppDataVerified().ColumnShardConfig.GetSharedMetadataAccessorCacheSize();
    }
    MetadataCache = std::make_shared<IGranuleDataAccessor::TSharedMetadataAccessorCache>(TotalMemorySize);
    Become(&TThis::StateWait);
}

}
