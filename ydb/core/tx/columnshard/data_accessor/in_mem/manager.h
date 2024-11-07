#pragma once

namespace NKikimr::NOlap::NDataAccessorControl::NInMem {
class TManager: public IMetadataMemoryManager {
private:
    virtual std::unique_ptr<IGranuleDataAccessor> DoBuildCollector() override;

    virtual std::shared_ptr<ITxReader> DoBuildGranuleLoader(
        const TVersionedIndex& versionedIndex, TGranuleMeta* granule, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) override;

public:
};
}