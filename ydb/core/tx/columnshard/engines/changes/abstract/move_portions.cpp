#include "move_portions.h"

#include <ydb/core/tx/columnshard/counters/portions.h>
#include <ydb/core/tx/columnshard/engines/changes/counters/general.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap {

std::shared_ptr<NDataLocks::ILock> TMovePortionsChange::DoBuildDataLock(const TString& id, const NDataLocks::ELockCategory lockCategory) const {
    THashSet<TPortionAddress> portions;
    for (auto&& i : Portions) {
        AFL_VERIFY(portions.emplace(i.first).second);
    }
    return std::make_shared<NDataLocks::TListPortionsLock>(id, portions, lockCategory);
}

void TMovePortionsChange::DoApplyOnExecute(
    NColumnShard::TColumnShard* /* self */, TWriteIndexContext& context, const TDataAccessorsResult& fetchedDataAccessor) {
    auto schemaPtr = context.EngineLogs.GetVersionedIndex().GetLastSchema();
    for (auto&& [_, i] : Portions) {
        const auto pred = [&](TPortionInfo& portionCopy) {
            portionCopy.MutableMeta().ResetCompactionLevel(TargetCompactionLevel.value_or(0));
        };
        context.EngineLogs.GetGranuleVerified(i->GetPathId())
            .ModifyPortionOnExecute(context.DBWrapper, fetchedDataAccessor.GetPortionAccessorVerified(i->GetPortionId()), pred,
                schemaPtr->GetIndexInfo().GetPKFirstColumnId());
    }
}

void TMovePortionsChange::DoApplyOnComplete(
    NColumnShard::TColumnShard* /* self */, TWriteIndexCompleteContext& context, const TDataAccessorsResult& /*fetchedDataAccessor*/) {
    if (!Portions.size()) {
        return;
    }
    THashMap<ui32, TSimplePortionsGroupInfo> portionGroups;
    for (auto&& [_, i] : Portions) {
        portionGroups[i->GetMeta().GetCompactionLevel()].AddPortion(i);
    }
    NChanges::TGeneralCompactionCounters::OnMovePortionsByLevel(portionGroups, TargetCompactionLevel.value_or(0));
    for (auto&& [_, i] : Portions) {
        const auto pred = [&](const std::shared_ptr<TPortionInfo>& portion) {
            portion->MutableMeta().ResetCompactionLevel(TargetCompactionLevel.value_or(0));
        };
        context.EngineLogs.ModifyPortionOnComplete(i, pred);
    }
}

}   // namespace NKikimr::NOlap
