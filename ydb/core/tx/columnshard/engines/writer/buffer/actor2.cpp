#include "actor2.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

namespace NKikimr::NOlap::NWritingPortions {

TActor::TActor(ui64 tabletId, const TActorId& parent)
    : TabletId(tabletId)
    , ParentActorId(parent) {
}

void TActor::Bootstrap() {
    Become(&TThis::StateWait);
    if (AppDataVerified().ColumnShardConfig.HasWritingBufferDurationMs()) {
        FlushDuration = TDuration::MilliSeconds(AppDataVerified().ColumnShardConfig.GetWritingBufferDurationMs());
    } else {
        FlushDuration = std::nullopt;
    }
    if (FlushDuration) {
        Schedule(*FlushDuration, new TEvFlushBuffer);
    }
}

void TActor::Flush() {
    for (auto&& i : Aggregations) {
        i.second.Flush(TabletId);
    }
    Aggregations.clear();
    SumSize = 0;
}

void TActor::Handle(TEvFlushBuffer::TPtr& /*ev*/) {
    if (AppDataVerified().ColumnShardConfig.HasWritingBufferDurationMs() && AppDataVerified().ColumnShardConfig.GetWritingBufferDurationMs()) {
        FlushDuration = TDuration::MilliSeconds(AppDataVerified().ColumnShardConfig.GetWritingBufferDurationMs());
    } else {
        FlushDuration = std::nullopt;
    }
    Flush();
    if (FlushDuration) {
        Schedule(*FlushDuration, new TEvFlushBuffer);
    }
}

void TActor::Handle(TEvAddInsertedDataToBuffer::TPtr& ev) {
    auto* evBase = ev->Get();
    AFL_VERIFY(evBase->GetWriteData()->GetBlobsAction()->GetStorageId() == NOlap::IStoragesManager::DefaultStorageId);

    SumSize += evBase->GetWriteData()->GetSize();
    const auto& pathId = evBase->GetWriteData()->GetWriteMeta().GetPathId().InternalPathId;
    const ui64 schemaVersion = evBase->GetContext()->GetActualSchema()->GetVersion();
    TAggregationId aggrId(pathId, schemaVersion, evBase->GetWriteData()->GetWriteMeta().GetModificationType());
    auto it = Aggregations.find(aggrId);
    if (it == Aggregations.end()) {
        it = Aggregations
                 .emplace(aggrId, TWriteAggregation(*evBase->GetContext(), pathId, evBase->GetWriteData()->GetWriteMeta().GetModificationType()))
                 .first;
    } else {
        it->second.MergeContext(*evBase->GetContext());
    }
    it->second.AddUnit(TWriteUnit(evBase->GetWriteData(), evBase->GetRecordBatch()));
    if (it->second.GetSumSize() > (ui64)AppDataVerified().ColumnShardConfig.GetWritingBufferVolumeMb() * 1024 * 1024 || !FlushDuration) {
        SumSize -= it->second.GetSumSize();
        it->second.Flush(TabletId);
    }
}

void TWriteAggregation::Flush(const ui64 tabletId) {
    if (Units.size()) {
        Context.GetWritingCounters()->OnAggregationWrite(Units.size(), SumSize);
        std::shared_ptr<NConveyor::ITask> task =
            std::make_shared<TBuildPackSlicesTask>(std::move(Units), Context, PathId, tabletId, ModificationType);
        NConveyorComposite::TInsertServiceOperator::SendTaskToExecute(task);
        Units.clear();
        SumSize = 0;
    }
}

}   // namespace NKikimr::NOlap::NWritingPortions
