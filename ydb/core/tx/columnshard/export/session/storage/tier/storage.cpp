#include "storage.h"
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>

namespace NKikimr::NOlap::NExport {

NKikimr::TConclusion<std::shared_ptr<IBlobsStorageOperator>> TTierStorageInitializer::DoInitializeOperator(const std::shared_ptr<IStoragesManager>& storages) const {
    auto bOperator = storages->GetOperatorOptional(TierName);
    if (!bOperator) {
        return TConclusionStatus::Fail("cannot find tier with name '" + TierName + "' for export destination");
    }
    return bOperator;
}

}