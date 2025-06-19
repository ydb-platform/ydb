#include "shared_metadata_accessor_cache_actor.h"

#include "ydb/core/protos/config.pb.h"

namespace NKikimr::NOlap::NDataAccessorControl {

NActors::IActor* TSharedMetadataAccessorCacheActor::CreateActor() {
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_accessor_cache", "CreateActor");
    return new TSharedMetadataAccessorCacheActor();
}

void TSharedMetadataAccessorCacheActor::Bootstrap() {
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_accessor_cache", "CreatBootstrapeActor");
    AccessorsCallback = std::make_shared<TActorAccessorsCallback>(SelfId());
    if (HasAppData()) {
        TotalMemorySize = AppDataVerified().ColumnShardConfig.GetSharedMetadataAccessorCacheSize();
    }
    MetadataCache = std::make_shared<IGranuleDataAccessor::TSharedMetadataAccessorCache>(TotalMemorySize);
    Become(&TThis::StateWait);
}

}
