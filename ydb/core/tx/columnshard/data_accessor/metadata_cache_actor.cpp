#include "metadata_cache_actor.h"

#include "ydb/core/protos/config.pb.h"

namespace NKikimr::NOlap::NDataAccessorControl {

NActors::IActor* TMetadataCacheActor::CreateActor() {
    return new TMetadataCacheActor();
}

void TMetadataCacheActor::Bootstrap() {
    AccessorsCallback = std::make_shared<TActorAccessorsCallback>(SelfId());
    if (HasAppData()) {
        TotalMemorySize = AppDataVerified().ColumnShardConfig.GetSharedMetadataCacheSize();
    }
    MetadataCache = std::make_shared<IGranuleDataAccessor::TMetadataCache>(TotalMemorySize);
    Become(&TThis::StateWait);
}

}
