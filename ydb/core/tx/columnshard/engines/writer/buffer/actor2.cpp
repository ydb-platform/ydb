#include "actor.h"
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard::NWritingPortions {

TActor::TActor(ui64 tabletId, const TActorId& parent)
    : TabletId(tabletId)
    , ParentActorId(parent)
{

}

void TActor::Bootstrap() {
    Become(&TThis::StateWait);
    Schedule(FlushDuration, new TEvFlushBuffer);
    if (AppDataVerified().ColumnShardConfig.HasWritingBufferDurationMs()) {
        FlushDuration = TDuration::MilliSeconds(AppDataVerified().ColumnShardConfig.GetWritingBufferDurationMs());
    } else {
        FlushDuration = std::nullopt;
    }
}

void TActor::Flush() {
    for (auto&& i : Aggregations) {
        i.second.Flush();
    }
    Aggregations.clear();
    SumSize = 0;
}

void TActor::Handle(TEvFlushBuffer::TPtr& /*ev*/) {
    if (AppDataVerified().ColumnShardConfig.HasWritingBufferDurationMs()) {
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
    const ui64 pathId = evBase->GetWriteData()->GetWriteMeta().GetPathId();
    const ui64 schemaVersion = evBase->GetContext()->GetSchemaVersion();
    TAggregationId aggrId(pathId, schemaVersion, evBase->GetWriteData()->GetWriteMeta().GetModificationType(),
        evBase->GetWriteData()->GetWriteMeta().GetBehaviour(), evBase->GetRecordBatch()->schema()->ToString());
    auto it = Aggregations.find(aggrId);
    if (it == Aggregations.end()) {
        it = Aggregations
                 .emplace(aggrId, TWriteAggregation(*evBase->GetContext(), pathId, evBase->GetWriteData()->GetWriteMeta().GetModificationType()))
                 .first;
    } else {
        it->second.MergeContext(*evBase->GetContext());
    }
    it->second.AddUnit(TWriteUnit(evBase->GetWriteData(), evBase->GetRecordBatch()));
    if (it->second.GetSumSize() > 16 * 1024 * 1024 || !FlushDuration) {
        it->second.Flush();
    }
}


}
