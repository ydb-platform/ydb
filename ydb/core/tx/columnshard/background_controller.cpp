#include "background_controller.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NColumnShard {

void TBackgroundController::StartTtl(const NOlap::TColumnEngineChanges& changes) {
    const NOlap::TTTLColumnEngineChanges* ttlChanges = dynamic_cast<const NOlap::TTTLColumnEngineChanges*>(&changes);
    Y_VERIFY(ttlChanges);
    Y_VERIFY(TtlPortions.empty());

    TtlPortions = ttlChanges->GetTouchedPortions();
}

bool TBackgroundController::StartCompaction(const NOlap::TPlanCompactionInfo& info, const NOlap::TColumnEngineChanges& changes) {
    Y_VERIFY(ActiveCompactionInfo.emplace(info.GetPathId(), info).second);
    Y_VERIFY(CompactionInfoPortions.emplace(info.GetPathId(), changes.GetTouchedPortions()).second);
    return true;
}

THashSet<NOlap::TPortionAddress> TBackgroundController::GetConflictTTLPortions() const {
    THashSet<NOlap::TPortionAddress> result = TtlPortions;
    for (auto&& i : CompactionInfoPortions) {
        for (auto&& g : i.second) {
            Y_VERIFY(result.emplace(g).second);
        }
    }
    return result;
}

THashSet<NOlap::TPortionAddress> TBackgroundController::GetConflictCompactionPortions() const {
    THashSet<NOlap::TPortionAddress> result = TtlPortions;
    for (auto&& i : CompactionInfoPortions) {
        for (auto&& g : i.second) {
            Y_VERIFY(result.emplace(g).second);
        }
    }
    return result;
}

void TBackgroundController::CheckDeadlines() {
    for (auto&& i : ActiveCompactionInfo) {
        if (TMonotonic::Now() - i.second.GetStartTime() > NOlap::TCompactionLimits::CompactionTimeout) {
            AFL_EMERG(NKikimrServices::TX_COLUMNSHARD)("event", "deadline_compaction");
            Y_VERIFY_DEBUG(false);
        }
    }
}

void TBackgroundController::StartIndexing(const NOlap::TColumnEngineChanges& /*changes*/) {
    Y_VERIFY(!ActiveIndexing);
    ActiveIndexing = true;
}


TString TBackgroundActivity::DebugString() const {
    return TStringBuilder()
        << "indexation:" << HasIndexation() << ";"
        << "compaction:" << HasCompaction() << ";"
        << "cleanup:" << HasCleanup() << ";"
        << "ttl:" << HasTtl() << ";"
        ;
}

}
