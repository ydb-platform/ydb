#pragma once
#include <ydb/core/tx/columnshard/data_accessor/abstract/manager.h>

namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB {
class TManager: public IMetadataMemoryManager {
private:
    const NActors::TActorId TabletActorId;
    virtual std::unique_ptr<IGranuleDataAccessor> DoBuildCollector(const ui64 pathId) override;

    virtual std::shared_ptr<ITxReader> DoBuildLoader(
        const TVersionedIndex& versionedIndex, TGranuleMeta* granule, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) override;

public:
    TManager(const NActors::TActorId& actorId)
        : TabletActorId(actorId)
    {

    }
};
}   // namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB
