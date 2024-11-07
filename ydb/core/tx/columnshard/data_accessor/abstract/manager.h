#pragma once
#include "collector.h"

#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/tx_reader/abstract.h>

namespace NKikimr::NOlap {
class TGranuleMeta;
}

namespace NKikimr::NOlap::NDataAccessorControl {
class IMetadataMemoryManager {
private:
    virtual std::unique_ptr<IGranuleDataAccessor> DoBuildCollector(const ui64 pathId) = 0;
    virtual std::shared_ptr<ITxReader> DoBuildGranuleLoader(
        const TVersionedIndex& versionedIndex, TGranuleMeta* granule, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) = 0;

public:
    virtual ~IMetadataMemoryManager() = default;

    std::unique_ptr<IGranuleDataAccessor> BuildCollector(const ui64 pathId) {
        return DoBuildCollector(pathId);
    }

    std::shared_ptr<ITxReader> BuildGranuleLoader(
        const TVersionedIndex& versionedIndex, TGranuleMeta* granule, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) {
        return DoBuildGranuleLoader(versionedIndex, granule, dsGroupSelector);
    }
};
}   // namespace NKikimr::NOlap::NDataAccessorControl
