#pragma once
#include "collector.h"

#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/tx_reader/abstract.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NOlap {
class TGranuleMeta;
}

namespace NKikimr::NOlap::NDataAccessorControl {
class IMetadataMemoryManager {
private:
    virtual std::shared_ptr<ITxReader> DoBuildLoader(
        const TVersionedIndex& versionedIndex, TGranuleMeta* granule, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) = 0;

public:
    virtual ~IMetadataMemoryManager() = default;
    virtual bool NeedPrefetch() const {
        return false;
    }

    std::shared_ptr<ITxReader> BuildLoader(
        const TVersionedIndex& versionedIndex, TGranuleMeta* granule, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) {
        return DoBuildLoader(versionedIndex, granule, dsGroupSelector);
    }
};
}   // namespace NKikimr::NOlap::NDataAccessorControl
