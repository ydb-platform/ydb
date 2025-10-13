#pragma once
#include <ydb/core/tx/columnshard/data_accessor/abstract/manager.h>
#include <ydb/core/tx/columnshard/tx_reader/abstract.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NOlap::NDataAccessorControl::NInMem {
class TManager: public IMetadataMemoryManager {
private:

    virtual std::shared_ptr<ITxReader> DoBuildLoader(
        const TVersionedIndex& versionedIndex, TGranuleMeta* granule, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) override;

public:
};
}   // namespace NKikimr::NOlap::NDataAccessorControl::NInMem
