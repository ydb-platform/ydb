#include "controller.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/gc.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/indexation.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NYDBTest::NColumnShard {

bool TController::DoOnAfterFilterAssembling(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (batch) {
        FilteredRecordsCount.Add(batch->num_rows());
    }
    return true;
}

bool TController::DoOnWriteIndexComplete(const NOlap::TColumnEngineChanges& /*changes*/, const ::NKikimr::NColumnShard::TColumnShard& /*shard*/) {
    Indexations.Inc();
    TGuard<TMutex> g(Mutex);
    if (SharingIds.empty()) {
        CheckInvariants();
    }
    return true;
}

bool TController::DoOnStartCompaction(std::shared_ptr<NOlap::TColumnEngineChanges>& changes) {
    if (auto compaction = dynamic_pointer_cast<NOlap::TCompactColumnEngineChanges>(changes)) {
        Compactions.Inc();
    }
    return true;
}

void TController::DoOnAfterGCAction(const ::NKikimr::NColumnShard::TColumnShard& /*shard*/, const NOlap::IBlobsGCAction& action) {
    for (auto d = action.GetBlobsToRemove().GetDirect().GetIterator(); d.IsValid(); ++d) {
        AFL_VERIFY(RemovedBlobIds[action.GetStorageId()][d.GetBlobId()].emplace(d.GetTabletId()).second);
    }
//    TGuard<TMutex> g(Mutex);
//    if (SharingIds.empty()) {
//        CheckInvariants();
//    }
}

void TController::CheckInvariants(const ::NKikimr::NColumnShard::TColumnShard& shard, TCheckContext& context) const {
    if (!shard.HasIndex()) {
        return;
    }
    const auto& index = shard.GetIndexAs<NOlap::TColumnEngineForLogs>();
    std::vector<std::shared_ptr<NOlap::TGranuleMeta>> granules = index.GetTables({}, {});
    THashMap<TString, THashSet<NOlap::TUnifiedBlobId>> ids;
    for (auto&& i : granules) {
        for (auto&& p : i->GetPortions()) {
            p.second->FillBlobIdsByStorage(ids);
        }
    }
    for (auto&& i : ids) {
        auto it = RemovedBlobIds.find(i.first);
        if (it == RemovedBlobIds.end()) {
            continue;
        }
        for (auto&& b : i.second) {
            auto itB = it->second.find(b);
            if (itB != it->second.end()) {
                AFL_VERIFY(!itB->second.contains((NOlap::TTabletId)shard.TabletID()));
            }
        }
    }
    THashMap<TString, NOlap::TBlobsCategories> shardBlobsCategories = shard.GetStoragesManager()->GetSharedBlobsManager()->GetBlobCategories();
    for (auto&& i : shardBlobsCategories) {
        auto manager = shard.GetStoragesManager()->GetOperatorVerified(i.first);
        const NOlap::TTabletsByBlob blobs = manager->GetBlobsToDelete();
        for (auto b = blobs.GetIterator(); b.IsValid(); ++b) {
            i.second.RemoveSharing(b.GetTabletId(), b.GetBlobId());
        }
        for (auto b = blobs.GetIterator(); b.IsValid(); ++b) {
            i.second.RemoveBorrowed(b.GetTabletId(), b.GetBlobId());
        }
    }
    context.AddCategories(shard.TabletID(), std::move(shardBlobsCategories));
}

TController::TCheckContext TController::CheckInvariants() const {
    TGuard<TMutex> g(Mutex);
    TCheckContext context;
    for (auto&& i : ShardActuals) {
        CheckInvariants(*i.second, context);
    }
    Cerr << context.DebugString() << Endl;
    context.Check();
    return context;
}

void TController::DoOnTabletInitCompleted(const ::NKikimr::NColumnShard::TColumnShard& shard) {
    TGuard<TMutex> g(Mutex);
    AFL_VERIFY(ShardActuals.emplace(shard.TabletID(), &shard).second);
}

void TController::DoOnTabletStopped(const ::NKikimr::NColumnShard::TColumnShard& shard) {
    TGuard<TMutex> g(Mutex);
    AFL_VERIFY(ShardActuals.erase(shard.TabletID()));
}

std::vector<ui64> TController::GetPathIds(const ui64 tabletId) const {
    TGuard<TMutex> g(Mutex);
    std::vector<ui64> result;
    for (auto&& i : ShardActuals) {
        if (i.first == tabletId) {
            const auto& index = i.second->GetIndexAs<NOlap::TColumnEngineForLogs>();
            std::vector<std::shared_ptr<NOlap::TGranuleMeta>> granules = index.GetTables({}, {});

            for (auto&& g : granules) {
                result.emplace_back(g->GetPathId());
            }
            break;
        }
    }
    return result;
}

bool TController::IsTrivialLinks() const {
    TGuard<TMutex> g(Mutex);
    for (auto&& i : ShardActuals) {
        if (!i.second->GetStoragesManager()->GetSharedBlobsManager()->IsTrivialLinks()) {
            return false;
        }
        if (i.second->GetStoragesManager()->HasBlobsToDelete()) {
            return false;
        }
    }
    return true;
}

}
