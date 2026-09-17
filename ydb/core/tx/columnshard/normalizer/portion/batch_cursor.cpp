#include "batch_cursor.h"

#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

bool TPortionToProcess::IsInDefaultStorage(const ui32 entityId) const {
    const TString& effectiveTier = TierName.empty() ? NBlobOperations::TGlobal::DefaultStorageId : TierName;
    return Schema->GetIndexInfo().GetEntityStorageId(entityId, effectiveTier) == NBlobOperations::TGlobal::DefaultStorageId;
}

THowToProcessPortion DefineHowToProcessPortion(const TString& portionTier, const std::shared_ptr<ISnapshotSchema>& schema) {
    const TString& effectiveTier = portionTier.empty() ? NBlobOperations::TGlobal::DefaultStorageId : portionTier;
    const TIndexInfo& indexInfo = schema->GetIndexInfo();
    if (effectiveTier == NBlobOperations::TGlobal::DefaultStorageId) {
        return THowToProcessPortion::All;
    }
    for (auto&& entityId : indexInfo.GetEntityIds()) {
        auto entityStorageId = indexInfo.GetEntityStorageId(entityId, effectiveTier);
        if (entityStorageId == NBlobOperations::TGlobal::DefaultStorageId) {
            return THowToProcessPortion::OnlyIndices;
        }
    }
    return THowToProcessPortion::Skip;
}

}   // namespace NKikimr::NOlap
