#include "aggregator_impl.h"
#include "analyze_scan_budget.h"

#include <ydb/core/tx/datashard/datashard.h>

#include <algorithm>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxScheduleTraversal : public TTxBase {
    const bool ForceTraversal;
    std::vector<std::pair<TPathId, ui64>> Starts;

    TTxScheduleTraversal(TSelf* self, bool forceTraversal)
        : TTxBase(self)
        , ForceTraversal(forceTraversal)
    {}

    TTxType GetTxType() const override { return TXTYPE_SCHEDULE_TRAVERSAL; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {

        if (!Self->EnableColumnStatistics) {
            YDB_LOG_TRACE("Column statistics disabled, won't schedule traversals",
                {"tabletId", Self->TabletID()});
            return true;
        }

        Self->RecalcForceTraversalInflightMaxTimeCounter(ctx.Now());

        YDB_LOG_TRACE("TTxScheduleTraversal::Execute",
            {"tabletId", Self->TabletID()});

        NIceDb::TNiceDb db(txc.DB);
        // Only periodic ticks admit background work, so failed background scans
        // cannot immediately retry themselves through the completion wakeup.
        Self->ScheduleTraversalsForScan(db, ctx, !ForceTraversal, Starts);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        bool startedBackground = false;
        for (const auto& [path, attempt] : Starts) {
            auto it = Self->ActiveTraversals.find(path);
            if (it != Self->ActiveTraversals.end() && it->second.Attempt == attempt && !it->second.Finishing) {
                Self->StartTraversal(ctx, it->second);
                startedBackground = startedBackground || it->second.OperationId.empty();
            }
        }
        if (ForceTraversal || startedBackground) {
            Self->RefreshScanUserDependencies();
        }
        YDB_LOG_TRACE("TTxScheduleTraversal::Complete",
            {"tabletId", Self->TabletID()});

        Self->ResolveStatisticsTablePathId();
        if (!ForceTraversal) {
            if (Self->EnableColumnStatistics) {
                Self->Schedule(Self->TraversalPeriod, new TEvPrivate::TEvScheduleTraversal());
            } else {
                Self->TraversalSchedulerStarted = false;
            }
        }
    }
};

void TStatisticsAggregator::RefreshScanUserDependencies() {
    if (std::none_of(ActiveTraversals.begin(), ActiveTraversals.end(), [](const auto& item) {
        return item.second.OperationId.empty() && item.second.AnalyzeActorId;
    })) {
        HasScanUserDependencies = false;
        return;
    }

    THashSet<TActorId> owners;
    for (const auto& operation : ForceTraversals) {
        if (IsTerminalAnalyzeState(operation.State)) {
            continue;
        }
        for (const auto& table : operation.Tables) {
            auto it = ActiveTraversals.find(table.PathId);
            if (table.Status == TForceTraversalTable::EStatus::None && it != ActiveTraversals.end() &&
                    it->second.OperationId.empty() && it->second.AnalyzeActorId) {
                owners.insert(it->second.AnalyzeActorId);
            }
        }
    }
    if (HasScanUserDependencies || !owners.empty()) {
        HasScanUserDependencies = !owners.empty();
        Send(ScanBudgetActorId, new TEvAnalyzeScanBudget::TEvSetUserDependencies(std::move(owners)));
    }
}

void TStatisticsAggregator::Handle(TEvPrivate::TEvScheduleTraversal::TPtr&) {
    Execute(new TTxScheduleTraversal(this, /*forceTraversal=*/false), TActivationContext::AsActorContext());
}

void TStatisticsAggregator::Handle(TEvPrivate::TEvScheduleForceTraversal::TPtr&) {
    Execute(new TTxScheduleTraversal(this, /*forceTraversal=*/true), TActivationContext::AsActorContext());
}

void TStatisticsAggregator::StartTraversalScheduler() {
    if (!EnableColumnStatistics || TraversalSchedulerStarted) {
        return;
    }
    TraversalSchedulerStarted = true;
    Schedule(TraversalPeriod, new TEvPrivate::TEvScheduleTraversal());
}

} // NKikimr::NStat
