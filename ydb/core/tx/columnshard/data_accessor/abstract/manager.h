#pragma once

namespace NKikimr::NOlap {
class TGranuleMeta;
}

namespace NKikimr::NOlap::NDataAccessorControl {
class IMetadataMemoryManager {
private:
    virtual std::unique_ptr<IGranuleDataAccessor> DoBuildCollector() = 0;
    virtual std::shared_ptr<ITxReader> DoBuildGranuleLoader(
        const TVersionedIndex& versionedIndex, TGranuleMeta* granule, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) = 0;

public:
    virtual std::unique_ptr<IGranuleDataAccessor> BuildCollector() override {
        return DoBuildCollector();
    }

    std::shared_ptr<ITxReader> BuildGranuleLoader(
        const TVersionedIndex& versionedIndex, TGranuleMeta* granule, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) {
        return DoBuildGranuleLoader(versionedIndex, granule, dsGroupSelector);
    }
};
}