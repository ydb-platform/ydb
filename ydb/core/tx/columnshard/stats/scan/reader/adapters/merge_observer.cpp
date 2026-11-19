#include "merge_observer.h"

#include <ydb/core/tx/columnshard/stats/scan/reader/operations.h>

namespace NKikimr::NOlap::NStats::NScan {

TMergeObserver::TMergeObserver(TLocalStats* localStats, ui64 requestedMemory)
    : LocalStats(localStats)
{
    if (!LocalStats) {
        return;
    }
    auto* pkStats = LocalStats->AddChild(LocalStats->GetRoot(), MakeMetastepStatisticsDefinition(NColumnShardStats::TOperation::kPKFilter));
    if (pkStats) {
        auto reserve = MakeStepStatisticsDefinition(NColumnShardStats::TOperation::kReserveMemory, "ReserveMemory");
        reserve.MutableReserveMemory()->MutableQuotaBytes()->SetApplicability(NColumnShardStats::APPLICABLE);
        reserve.MutableReserveMemory()->MutableRequestedBytes()->SetApplicability(NColumnShardStats::APPLICABLE);
        reserve.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
        reserve.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
        LocalStats->AddChild(*pkStats, reserve);
        auto merge = MakeStepStatisticsDefinition(NColumnShardStats::TOperation::kMerge, "Merge");
        merge.MutableMerge()->SetPurpose(NColumnShardStats::ROW_VERSIONS);
        LocalStats->AddChild(*pkStats, merge);
    }
    auto* resultStats =
        LocalStats->AddChild(LocalStats->GetRoot(), MakeMetastepStatisticsDefinition(NColumnShardStats::TOperation::kBuildResult));
    if (resultStats) {
        auto prepare = MakeStepStatisticsDefinition(NColumnShardStats::TOperation::kPrepareResult, "PrepareResult");
        prepare.MutablePrepareResult()->SetAction(NColumnShardStats::PREPARE_RESULT);
        prepare.MutableStatistics()->SetInputRepresentation(NColumnShardStats::ARROW_LOGICAL);
        LocalStats->AddChild(*resultStats, prepare);
        LocalStats->AddChild(*resultStats, MakeStepStatisticsDefinition(NColumnShardStats::TOperation::kBuildResult, "BuildResult"));
    }
    PKScope = LocalStats->Measure(pkStats);
    PKScope.Observe([](auto& observation) {
        observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows).SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    });
    PKScope.Suspend();
    ReserveScope = LocalStats->Measure(pkStats ? pkStats->FindChild("ReserveMemory") : nullptr, PKScope.GetHandle());
    ReserveScope.Observe([&](auto& observation) {
        observation.Metric(&NColumnShardStats::TReserveMemoryDetails::MutableRequestedBytes).SetValue(requestedMemory);
    });
    ReserveScope.Suspend();
    DecodeObserver = std::make_unique<TDecodeObserver>(*LocalStats);
    DecodeObserver->SetParent(pkStats ? pkStats->FindChild("Merge") : nullptr);
}

void TMergeObserver::OnMergeStarted(std::optional<ui64> sources, std::optional<ui64> inputRows) {
    if (!LocalStats) {
        return;
    }
    auto* pkStats = LocalStats->GetRoot().FindChild("PKFilter");
    MergeScope = LocalStats->Measure(pkStats ? pkStats->FindChild("Merge") : nullptr, PKScope.GetHandle());
    if (DecodeObserver) {
        DecodeObserver->SetParentCall(MergeScope.GetHandle());
    }
    MergeScope.Observe([&](auto& observation) {
        observation.Metric(&NColumnShardStats::TMergeDetails::MutableIntervals).SetValue(1);
        if (sources) {
            observation.Metric(&NColumnShardStats::TMergeDetails::MutableSources).SetValue(*sources);
        }
    });
    if (inputRows) {
        OnInputRows(*inputRows);
    }
}

void TMergeObserver::OnInputRows(ui64 rows) {
    MergeScope.Observe([&](auto& observation) {
        observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows).SetApplicability(NColumnShardStats::APPLICABLE);
        observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows).SetValue(rows);
    });
}

void TMergeObserver::OnPKOutputRows(ui64 rows) {
    PKScope.Observe([&](auto& observation) {
        observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows).SetApplicability(NColumnShardStats::APPLICABLE);
        observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows).SetValue(rows);
    });
}

void TMergeObserver::OnMergeFinished(ui64 outputRows) {
    MergeScope.Observe([&](auto& observation) {
        observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows).SetApplicability(NColumnShardStats::APPLICABLE);
        observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows).SetValue(outputRows);
    });
    OnPKOutputRows(outputRows);
    MergeScope.Finish();
    PKScope.Finish();
}

void TMergeObserver::OnMergeFinished() {
    MergeScope.Finish();
}

void TMergeObserver::OnPKResumed() {
    PKScope.Resume();
}

void TMergeObserver::OnPKFinished() {
    PKScope.Finish();
}

void TMergeObserver::OnMemoryAllocated(ui64 bytes) {
    ReserveScope.Resume();
    ReserveScope.Observe([&](auto& observation) {
        observation.Metric(&NColumnShardStats::TReserveMemoryDetails::MutableQuotaBytes).SetValue(bytes);
    });
    ReserveScope.Finish();
}

void TMergeObserver::OnAllocationImpossible() {
    OnMemoryAllocated(0);
    PKScope.Finish();
}

}   // namespace NKikimr::NOlap::NStats::NScan
