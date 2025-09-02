#include "remove_portions.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap {

std::shared_ptr<NDataLocks::ILock> TRemovePortionsChange::DoBuildDataLock(
    const TString& id, const NDataLocks::ELockCategory lockCategory) const {
    THashSet<TPortionAddress> portions;
    for (auto&& i : Portions) {
        AFL_VERIFY(portions.emplace(i.first).second);
    }
    return std::make_shared<NDataLocks::TListPortionsLock>(id, portions, lockCategory);
}

void TRemovePortionsChange::DoApplyOnExecute(
    NColumnShard::TColumnShard* /* self */, TWriteIndexContext& context, const TDataAccessorsResult& fetchedDataAccessors) {
    THashSet<ui64> usedPortionIds;
    auto schemaPtr = context.EngineLogs.GetVersionedIndex().GetLastSchema();
    for (auto&& [_, i] : Portions) {
        Y_ABORT_UNLESS(!i->HasRemoveSnapshot());
        AFL_VERIFY(usedPortionIds.emplace(i->GetPortionId()).second)("portion_info", i->DebugString(true));
        const auto pred = [&](TPortionInfo& portionCopy) {
            portionCopy.SetRemoveSnapshot(context.Snapshot);
        };
        context.EngineLogs.GetGranuleVerified(i->GetPathId())
            .ModifyPortionOnExecute(context.DBWrapper, fetchedDataAccessors.GetPortionAccessorVerified(i->GetPortionId()), pred,
                schemaPtr->GetIndexInfo().GetPKFirstColumnId());
    }
}

void TRemovePortionsChange::DoApplyOnComplete(
    NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context, const TDataAccessorsResult& /*fetchedDataAccessors*/) {
    if (self) {
        self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_PORTIONS_DEACTIVATED, Portions.size());

        for (auto& [_, portionInfo] : Portions) {
            self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_BYTES_DEACTIVATED, portionInfo->GetTotalBlobBytes());
            self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_RAW_BYTES_DEACTIVATED, portionInfo->GetTotalRawBytes());
        }
    }

    for (auto&& [_, i] : Portions) {
        Y_ABORT_UNLESS(!i->HasRemoveSnapshot());
        const auto pred = [&](const std::shared_ptr<TPortionInfo>& portion) {
            portion->SetRemoveSnapshot(context.Snapshot);
        };
        context.EngineLogs.ModifyPortionOnComplete(i, pred);
        context.EngineLogs.AddCleanupPortion(i);
    }
}

}   // namespace NKikimr::NOlap
