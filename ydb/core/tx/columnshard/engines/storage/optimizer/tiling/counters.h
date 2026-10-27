#pragma once
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/counters/portions.h>

#include <ydb/library/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/system/mutex.h>

#include <array>
#include <atomic>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

static constexpr ui32 TILING_LAYERS_COUNT = 10;
static constexpr ui32 TILING_PRIORITY_LEVEL_COUNT = 11;

class TPortionCategoryCounterAgents: public NColumnShard::TPortionCategoryCounterAgents {
private:
    using TBase = NColumnShard::TPortionCategoryCounterAgents;

public:
    using TLevelNodeCounts = std::array<std::atomic<i64>, TILING_PRIORITY_LEVEL_COUNT>;

    const std::shared_ptr<TLevelNodeCounts> LevelNodeCounts = std::make_shared<TLevelNodeCounts>();
    const NMonitoring::TDynamicCounters::TCounterPtr MaxOverload;
    const NColumnShard::TIncrementalHistogram OverloadHistogram;
    const NColumnShard::TIncrementalHistogram WidthHistogram;
    const NColumnShard::TIncrementalHistogram HeightHistogram;

    TPortionCategoryCounterAgents(TCommonCountersOwner& base, const TString& categoryName)
        : TBase(base, categoryName)
        , MaxOverload(TBase::GetValue("Overload/Max"))
        , OverloadHistogram(base.GetModuleId(), "ByLevel/Overload", categoryName, NColumnShard::THistorgamBorders::PortionWidthBorders)
        , WidthHistogram(base.GetModuleId(), "ByLevel/Width", categoryName, NColumnShard::THistorgamBorders::PortionWidthBorders)
        , HeightHistogram(base.GetModuleId(), "ByLevel/Height", categoryName, NColumnShard::THistorgamBorders::PortionWidthBorders)
    {
    }
};

class TPortionCategoryCounters: public NColumnShard::TPortionCategoryCounters {
private:
    using TBase = NColumnShard::TPortionCategoryCounters;
    using TLevelNodeCounts = TPortionCategoryCounterAgents::TLevelNodeCounts;
    std::shared_ptr<TLevelNodeCounts> LevelNodeCounts;
    NMonitoring::TDynamicCounters::TCounterPtr MaxOverload;
    std::shared_ptr<NColumnShard::TIncrementalHistogram::TGuard> OverloadHistogram;
    std::shared_ptr<NColumnShard::TIncrementalHistogram::TGuard> WidthHistogram;
    std::shared_ptr<NColumnShard::TIncrementalHistogram::TGuard> HeightHistogram;
    std::optional<ui64> LastOverload;
    std::optional<ui64> LastHeight;

    static ui32 LevelBucket(const ui64 overload) {
        return std::min<ui64>(overload, TILING_PRIORITY_LEVEL_COUNT - 1);
    }

public:
    TPortionCategoryCounters(TPortionCategoryCounterAgents& agents)
        : TBase(agents)
        , LevelNodeCounts(agents.LevelNodeCounts)
        , MaxOverload(agents.MaxOverload)
        , OverloadHistogram(agents.OverloadHistogram.BuildGuard())
        , WidthHistogram(agents.WidthHistogram.BuildGuard())
        , HeightHistogram(agents.HeightHistogram.BuildGuard())
    {
    }

    ~TPortionCategoryCounters() {
        if (LastOverload) {
            (*LevelNodeCounts)[LevelBucket(*LastOverload)].fetch_sub(1);
        }
    }

    void SetOverload(const ui64 overload) {
        // Add before subtracting: a reader on another shard must never see the critical bucket empty while this
        // level stays overloaded.
        OverloadHistogram->Add(overload, 1);
        (*LevelNodeCounts)[LevelBucket(overload)].fetch_add(1);
        if (LastOverload) {
            OverloadHistogram->Sub(*LastOverload, 1);
            (*LevelNodeCounts)[LevelBucket(*LastOverload)].fetch_sub(1);
        }
        LastOverload = overload;
    }

    void SetHeight(const ui64 height) {
        if (LastHeight) {
            HeightHistogram->Sub(*LastHeight, 1);
        }
        HeightHistogram->Add(height, 1);
        LastHeight = height;
    }

    i64 GetMaxOverloadLevel() const {
        for (i64 level = TILING_PRIORITY_LEVEL_COUNT - 1; level >= 0; --level) {
            if ((*LevelNodeCounts)[level].load() > 0) {
                MaxOverload->Set(level);
                return level;
            }
        }
        MaxOverload->Set(0);
        return 0;
    }

    void AddWidth(const ui64 width) {
        WidthHistogram->Add(width, 1);
    }

    void RemoveWidth(const ui64 width) {
        WidthHistogram->Sub(width, 1);
    }
};

class TLevelAgents {
public:
    const std::shared_ptr<TPortionCategoryCounterAgents> Portions;

    TLevelAgents(const TString& name, NColumnShard::TCommonCountersOwner& baseOwner)
        : Portions(std::make_shared<TPortionCategoryCounterAgents>(baseOwner, name))
    {
    }
};

/// The node-wide view of the compacted snapshot S of the tiling-s planners (design.md, Observability): the oldest S over
/// the node's tables, the clock, and their difference, all in seconds since the epoch, so that the compaction delay can
/// be graphed. Every planner reports its own S into a small registry under a mutex, at most once per second (the adapter
/// throttles it, 2026-10-27): the mutex is shared by every shard of the node and must stay off the per-portion path.
class TCompactedSnapshotCounters {
private:
    /// A planner's last report. A settled planner, nothing outside its line, reports the clock minus L (decision 6): its
    /// delay is L for as long as it stays settled, so between reports its delay is kept rather than its snapshot; a
    /// planner with portions outside keeps its snapshot, so its delay grows with the clock as it should (2026-10-27).
    struct TReport {
        ui64 SnapshotSeconds = 0;
        ui64 ReportedAtSeconds = 0;
        bool Settled = false;

        ui64 SnapshotAt(const ui64 nowSeconds) const {
            if (Settled && ReportedAtSeconds <= nowSeconds) {
                return nowSeconds - (ReportedAtSeconds - SnapshotSeconds);
            }
            return SnapshotSeconds;
        }
    };

    TMutex Mutex;
    THashMap<ui64, TReport> SnapshotSecondsByPlanner;
    const NMonitoring::TDynamicCounters::TCounterPtr OldestSnapshotSeconds;
    const NMonitoring::TDynamicCounters::TCounterPtr CurrentSnapshotSeconds;
    const NMonitoring::TDynamicCounters::TCounterPtr MaxDelaySeconds;
    const NMonitoring::TDynamicCounters::TCounterPtr Planners;

    void Publish(const ui64 nowSeconds) {
        ui64 oldest = nowSeconds;
        for (const auto& [_, report] : SnapshotSecondsByPlanner) {
            oldest = std::min(oldest, report.SnapshotAt(nowSeconds));
        }
        OldestSnapshotSeconds->Set(oldest);
        CurrentSnapshotSeconds->Set(nowSeconds);
        MaxDelaySeconds->Set(nowSeconds - oldest);
        Planners->Set(SnapshotSecondsByPlanner.size());
    }

public:
    TCompactedSnapshotCounters(const NColumnShard::TCommonCountersOwner& owner)
        : OldestSnapshotSeconds(owner.GetValue("Snapshot/Compacted/OldestSeconds"))
        , CurrentSnapshotSeconds(owner.GetValue("Snapshot/CurrentSeconds"))
        , MaxDelaySeconds(owner.GetValue("Snapshot/Compacted/MaxDelaySeconds"))
        , Planners(owner.GetValue("Snapshot/Compacted/Planners"))
    {
    }

    void Set(const ui64 plannerId, const ui64 snapshotSeconds, const ui64 nowSeconds, const bool settled = false) {
        TGuard<TMutex> guard(Mutex);
        SnapshotSecondsByPlanner[plannerId] = TReport{ .SnapshotSeconds = snapshotSeconds, .ReportedAtSeconds = nowSeconds, .Settled = settled };
        Publish(nowSeconds);
    }

    void Remove(const ui64 plannerId, const ui64 nowSeconds) {
        TGuard<TMutex> guard(Mutex);
        SnapshotSecondsByPlanner.erase(plannerId);
        Publish(nowSeconds);
    }

    ui64 GetOldestSeconds() const {
        return OldestSnapshotSeconds->Val();
    }

    ui64 GetCurrentSeconds() const {
        return CurrentSnapshotSeconds->Val();
    }

    ui64 GetMaxDelaySeconds() const {
        return MaxDelaySeconds->Val();
    }
};

class TGlobalCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    std::vector<std::shared_ptr<TLevelAgents>> Levels;
    std::vector<std::shared_ptr<TLevelAgents>> Accumulators;
    std::shared_ptr<TLevelAgents> LastLevel;
    std::shared_ptr<TLevelAgents> Tiling;
    const NMonitoring::TDynamicCounters::TCounterPtr TasksSkippedByPriorityGap;
    const NMonitoring::THistogramPtr GeneratedTasksPriorityLevel;
    /// The memory shortage of the advance (tiling-s, 2026-10-26): the bytes an advance would have taken had the budget
    /// not cut its window, the shortfall against the budget summed over cut windows, and the number of cut windows.
    const NMonitoring::THistogramPtr AdvanceIdealTaskBytes;
    const NMonitoring::TDynamicCounters::TCounterPtr AdvanceMemoryShortageBytes;
    const NMonitoring::TDynamicCounters::TCounterPtr AdvanceBudgetCuts;
    /// Time spent inside the tiling-s adapter per engine call, in microseconds (2026-10-27): the planner's share of the
    /// shard's transaction Complete phases.
    const NMonitoring::THistogramPtr PlannerModifyPortionsUs;
    const NMonitoring::THistogramPtr PlannerUsefulMetricUs;
    const NMonitoring::THistogramPtr PlannerNextTaskUs;
    const NMonitoring::THistogramPtr PlannerOverloadedUs;
    const std::shared_ptr<TCompactedSnapshotCounters> CompactedSnapshot;

public:
    TGlobalCounters()
        : TBase("TilingCompactionOptimizer")
        , TasksSkippedByPriorityGap(TBase::GetDeriviative("Tasks/SkippedByPriorityGap/Count"))
        , GeneratedTasksPriorityLevel(TBase::GetHistogram("Tasks/GeneratedByPriorityLevel", NMonitoring::LinearHistogram(12, 0, 1)))
        , AdvanceIdealTaskBytes(TBase::GetHistogram("Advance/IdealTaskBytes", NMonitoring::ExponentialHistogram(25, 2, 1024)))
        , AdvanceMemoryShortageBytes(TBase::GetDeriviative("Advance/MemoryShortage/Bytes"))
        , AdvanceBudgetCuts(TBase::GetDeriviative("Advance/BudgetCut/Count"))
        , PlannerModifyPortionsUs(TBase::GetHistogram("Planner/ModifyPortionsUs", NMonitoring::ExponentialHistogram(22, 2)))
        , PlannerUsefulMetricUs(TBase::GetHistogram("Planner/UsefulMetricUs", NMonitoring::ExponentialHistogram(22, 2)))
        , PlannerNextTaskUs(TBase::GetHistogram("Planner/NextTaskUs", NMonitoring::ExponentialHistogram(22, 2)))
        , PlannerOverloadedUs(TBase::GetHistogram("Planner/OverloadedUs", NMonitoring::ExponentialHistogram(22, 2)))
        , CompactedSnapshot(std::make_shared<TCompactedSnapshotCounters>(*this))
    {
        for (ui32 i = 0; i < TILING_LAYERS_COUNT; ++i) {
            Levels.emplace_back(std::make_shared<TLevelAgents>("level=" + ::ToString(i), *this));
            Accumulators.emplace_back(std::make_shared<TLevelAgents>("acc=" + ::ToString(i), *this));
        }
        LastLevel = std::make_shared<TLevelAgents>("last", *this);
        Tiling = std::make_shared<TLevelAgents>("tiling", *this);
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildClient(
        const std::vector<std::shared_ptr<TLevelAgents>>& agentList, const ui32 idx, const TString& debugName) {
        AFL_VERIFY(idx < agentList.size())("idx", idx)("limit", agentList.size())("type", debugName);
        return std::make_shared<TPortionCategoryCounters>(*agentList[idx]->Portions);
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildLevelClient(const ui32 levelId) {
        return BuildClient(Singleton<TGlobalCounters>()->Levels, levelId, "level");
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildAccumulatorClient(const ui32 accId) {
        return BuildClient(Singleton<TGlobalCounters>()->Accumulators, accId, "accumulator");
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildLastClient() {
        return std::make_shared<TPortionCategoryCounters>(*Singleton<TGlobalCounters>()->LastLevel->Portions);
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildTilingClient() {
        return std::make_shared<TPortionCategoryCounters>(*Singleton<TGlobalCounters>()->Tiling->Portions);
    }

    static const NMonitoring::TDynamicCounters::TCounterPtr& GetTasksSkippedByPriorityGap() {
        return Singleton<TGlobalCounters>()->TasksSkippedByPriorityGap;
    }

    static const NMonitoring::THistogramPtr& GetGeneratedTasksPriorityLevel() {
        return Singleton<TGlobalCounters>()->GeneratedTasksPriorityLevel;
    }

    static const NMonitoring::THistogramPtr& GetAdvanceIdealTaskBytes() {
        return Singleton<TGlobalCounters>()->AdvanceIdealTaskBytes;
    }

    static const NMonitoring::TDynamicCounters::TCounterPtr& GetAdvanceMemoryShortageBytes() {
        return Singleton<TGlobalCounters>()->AdvanceMemoryShortageBytes;
    }

    static const NMonitoring::TDynamicCounters::TCounterPtr& GetAdvanceBudgetCuts() {
        return Singleton<TGlobalCounters>()->AdvanceBudgetCuts;
    }

    static const NMonitoring::THistogramPtr& GetPlannerModifyPortionsUs() {
        return Singleton<TGlobalCounters>()->PlannerModifyPortionsUs;
    }

    static const NMonitoring::THistogramPtr& GetPlannerUsefulMetricUs() {
        return Singleton<TGlobalCounters>()->PlannerUsefulMetricUs;
    }

    static const NMonitoring::THistogramPtr& GetPlannerNextTaskUs() {
        return Singleton<TGlobalCounters>()->PlannerNextTaskUs;
    }

    static const NMonitoring::THistogramPtr& GetPlannerOverloadedUs() {
        return Singleton<TGlobalCounters>()->PlannerOverloadedUs;
    }

    static const std::shared_ptr<TCompactedSnapshotCounters>& GetCompactedSnapshotCounters() {
        return Singleton<TGlobalCounters>()->CompactedSnapshot;
    }
};

class TLevelCounters {
public:
    const std::shared_ptr<TPortionCategoryCounters> Portions;

    explicit TLevelCounters(std::shared_ptr<TPortionCategoryCounters> portions)
        : Portions(std::move(portions))
    {
        AFL_VERIFY(Portions);
    }
};

class TCounters {
public:
    std::vector<TLevelCounters> Levels;
    std::vector<TLevelCounters> Accumulators;
    TLevelCounters LastLevel;
    TLevelCounters Tiling;
    const NMonitoring::TDynamicCounters::TCounterPtr TasksSkippedByPriorityGap;
    const NMonitoring::THistogramPtr GeneratedTasksPriorityLevel;
    const NMonitoring::THistogramPtr AdvanceIdealTaskBytes;
    const NMonitoring::TDynamicCounters::TCounterPtr AdvanceMemoryShortageBytes;
    const NMonitoring::TDynamicCounters::TCounterPtr AdvanceBudgetCuts;
    const NMonitoring::THistogramPtr PlannerModifyPortionsUs;
    const NMonitoring::THistogramPtr PlannerUsefulMetricUs;
    const NMonitoring::THistogramPtr PlannerNextTaskUs;
    const NMonitoring::THistogramPtr PlannerOverloadedUs;
    const std::shared_ptr<TCompactedSnapshotCounters> CompactedSnapshot;

    TCounters()
        : LastLevel(TGlobalCounters::BuildLastClient())
        , Tiling(TGlobalCounters::BuildTilingClient())
        , TasksSkippedByPriorityGap(TGlobalCounters::GetTasksSkippedByPriorityGap())
        , GeneratedTasksPriorityLevel(TGlobalCounters::GetGeneratedTasksPriorityLevel())
        , AdvanceIdealTaskBytes(TGlobalCounters::GetAdvanceIdealTaskBytes())
        , AdvanceMemoryShortageBytes(TGlobalCounters::GetAdvanceMemoryShortageBytes())
        , AdvanceBudgetCuts(TGlobalCounters::GetAdvanceBudgetCuts())
        , PlannerModifyPortionsUs(TGlobalCounters::GetPlannerModifyPortionsUs())
        , PlannerUsefulMetricUs(TGlobalCounters::GetPlannerUsefulMetricUs())
        , PlannerNextTaskUs(TGlobalCounters::GetPlannerNextTaskUs())
        , PlannerOverloadedUs(TGlobalCounters::GetPlannerOverloadedUs())
        , CompactedSnapshot(TGlobalCounters::GetCompactedSnapshotCounters())
    {
        for (ui32 i = 0; i < TILING_LAYERS_COUNT; ++i) {
            Levels.emplace_back(TGlobalCounters::BuildLevelClient(i));
            Accumulators.emplace_back(TGlobalCounters::BuildAccumulatorClient(i));
        }
    }

    const TLevelCounters& GetLevelCounters(const ui32 levelIdx) const {
        AFL_VERIFY(levelIdx < Levels.size())("idx", levelIdx)("count", Levels.size());
        return Levels[levelIdx];
    }

    const TLevelCounters& GetAccumulatorCounters(const ui32 accIdx) const {
        AFL_VERIFY(accIdx < Accumulators.size())("idx", accIdx)("count", Accumulators.size());
        return Accumulators[accIdx];
    }

    const TLevelCounters& GetLastLevelCounters() const {
        return LastLevel;
    }

    const TLevelCounters& GetTilingCounters() const {
        return Tiling;
    }

    TCompactedSnapshotCounters& GetCompactedSnapshotCounters() const {
        return *CompactedSnapshot;
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
