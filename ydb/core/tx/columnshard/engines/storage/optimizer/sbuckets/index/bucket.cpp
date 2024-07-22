#include "bucket.h"
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets {

void TPortionsBucket::RebuildOptimizedFeature(const TInstant currentInstant) const {
    for (auto&& [_, p] : Portions) {
        p.MutablePortionInfo().InitRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized, Portions.size() == 1 && currentInstant > p->RecordSnapshotMax().GetPlanInstant() +
            NYDBTest::TControllers::GetColumnShardController()->GetLagForCompactionBeforeTierings(TDuration::Minutes(60))
        );
    }
}

std::shared_ptr<NKikimr::NOlap::TColumnEngineChanges> TPortionsBucket::BuildOptimizationTask(std::shared_ptr<TGranuleMeta> granule,
    const std::shared_ptr<NDataLocks::TManager>& locksManager, const std::shared_ptr<arrow::Schema>& primaryKeysSchema, const std::shared_ptr<IStoragesManager>& storagesManager) const {
    auto context = Logic->BuildTask(TInstant::Now(), GetMemLimit(), *this);
    AFL_VERIFY(context.GetPortions().size() > 1)("size", context.GetPortions().size());
    ui64 size = 0;
    for (auto&& i : context.GetPortions()) {
        size += i->GetTotalBlobBytes();
        AFL_VERIFY(!locksManager->IsLocked(*i));
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("size", size)("next", Finish.DebugString())("count", context.GetPortions().size())("event", "start_optimization");
    TSaverContext saverContext(storagesManager);
    auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, context.GetPortions(), saverContext);
    for (auto&& i : context.GetSplitRightOpenIntervalPoints()) {
        NArrow::NMerger::TSortableBatchPosition pos(i.ToBatch(primaryKeysSchema), 0, primaryKeysSchema->field_names(), {}, false);
        result->AddCheckPoint(pos, false);
    }
    return result;
}

bool TPortionsBucket::IsLocked(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const {
    for (auto&& i : Portions) {
        if (dataLocksManager->IsLocked(*i.second.GetPortionInfo())) {
            return true;
        }
    }
    return false;
}

ui64 TPortionsBucket::GetMemLimit() const {
    return HasAppData() ? AppDataVerified().ColumnShardConfig.GetCompactionMemoryLimit() : 512 * 1024 * 1024;
}

}
