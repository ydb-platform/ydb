#pragma once
#include <ydb/core/tx/columnshard/data_accessor/abstract/manager.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB {
class TManager: public IMetadataMemoryManager {
private:
    const NActors::TActorId TabletActorId;
    const ui64 MemoryCacheSize;
    const bool FetchOnStart = true;

    virtual std::shared_ptr<ITxReader> DoBuildLoader(
        const TVersionedIndex& versionedIndex, TGranuleMeta* granule, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) override;

public:
    virtual bool NeedPrefetch() const override {
        return FetchOnStart;
    }

    TManager(const NActors::TActorId& actorId, const ui64 memoryCacheSize, const bool fetchOnStart)
        : TabletActorId(actorId)
        , MemoryCacheSize(memoryCacheSize)
        , FetchOnStart(fetchOnStart)
    {

    }
};
}   // namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB
