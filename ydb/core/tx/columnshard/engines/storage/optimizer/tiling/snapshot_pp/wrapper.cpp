#include "planner.h"
#include "settings.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>

#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap::NStorageOptimizer::NSnapshotTiling {

namespace {

using TCore = TSnapshotTiling<NArrow::TSimpleRow, TPortionInfo, TSnapshot>;

TInstant CurrentInstant() {
    return HasAppData() ? AppDataVerified().TimeProvider->Now() : TInstant::Now();
}

/// The reason label of the compaction counters (design.md, Observability): the advance is the snapshot family.
TString ReasonName(const ETaskReason reason) {
    switch (reason) {
        case ETaskReason::Accumulator:
            return "accumulator";
        case ETaskReason::Level:
            return "level";
        case ETaskReason::AdvanceMerge:
            return "snapshot_merge";
        case ETaskReason::AdvancePrefix:
            return "snapshot_prefix";
        case ETaskReason::AdvanceMove:
            return "snapshot_move";
    }
    return "generic";
}

/// The adapter between the engine's planner interface and the core (implementation_plan.md, "Layout"). The engine calls
/// it through const methods; task requests hand portions out and re-sync the core, hence the mutable core.
class TOptimizerPlannerAdapter: public IOptimizerPlanner {
private:
    using TBase = IOptimizerPlanner;
    TSnapshotTilingSettings Settings;
    NTiling::TCounters Counters;
    mutable TCore Core;
    std::shared_ptr<IStoragesManager> StoragesManager;
    std::shared_ptr<arrow::Schema> PrimaryKeysSchema;
    static inline std::atomic<ui64> NextPlannerId{ 1 };
    const ui64 PlannerId = NextPlannerId++;

    /// The node's clock as a plan step (decision 6, revised 2026-10-16). Plan steps are mediator milliseconds, so a skew
    /// between the two clocks only delays the settle time, it never passes it early by more than the skew.
    TCore::TClock BuildClock() const {
        const TInstant now = CurrentInstant();
        return TCore::TClock{
            .Now = TSnapshot::MaxForPlanInstant(now),
            .SettledBefore = TSnapshot::MaxForPlanInstant(now - Settings.SettleTime),
        };
    }

    /// The node-wide gauges of S (design.md, Observability): the forced-request reading, or the clock minus L when nothing
    /// is outside the line (decision 6), so that a settled table shows a delay of exactly L. At most once per second per
    /// planner (2026-10-27): the registry behind the gauges is one mutex shared by every shard of the node, and taking it
    /// on every portion change convoyed the shards' transaction Complete phases under a saturated CPU; the gauges are
    /// scraped every few seconds, so a one-second staleness costs nothing.
    static constexpr TDuration SnapshotsPublishPeriod = TDuration::Seconds(1);
    mutable TInstant SnapshotsPublished = TInstant::Zero();

    void PublishSnapshots(const TCore::TClock& clock) const {
        const TInstant now = TInstant::MilliSeconds(clock.Now.GetPlanStep());
        if (now < SnapshotsPublished + SnapshotsPublishPeriod) {
            return;
        }
        SnapshotsPublished = now;
        const auto s = Core.GetForcedCompactedSnapshot();
        const ui64 snapshotSeconds = (s ? *s : clock.SettledBefore).GetPlanStep() / 1000;
        Counters.GetCompactedSnapshotCounters().Set(PlannerId, snapshotSeconds, clock.Now.GetPlanStep() / 1000, /*settled=*/!s);
    }

    static bool IsLockedBy(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, const TPortionInfo::TConstPtr& p) {
        return dataLocksManager && dataLocksManager->IsLocked(*p, NDataLocks::ELockCategory::Compaction).has_value();
    }

    /// One core task is one engine change: a merge with its checkpoints, or a metadata move, which reads no blobs
    /// (TGeneralCompactColumnEngineChanges::NeedConstruction) and only rewrites the compaction level (decision 26).
    std::shared_ptr<TColumnEngineChanges> BuildChanges(const std::shared_ptr<TGranuleMeta>& granule, const TCore::TTask& task) const {
        std::vector<TPortionInfo::TConstPtr> toMerge;
        std::vector<TPortionInfo::TPtr> toMove;
        if (task.Kind == ETaskKind::MetadataMove) {
            for (const auto& p : task.Portions) {
                // The engine keeps its portions mutable; the core hands them out as const.
                toMove.push_back(std::const_pointer_cast<TPortionInfo>(p));
            }
        } else {
            toMerge = task.Portions;
        }
        auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, toMerge, TSaverContext(StoragesManager));
        if (!toMove.empty()) {
            result->AddMovePortions(toMove);
        }
        result->SetTargetCompactionLevel(task.TargetLevel);
        result->SetPortionExpectedSize(Settings.PortionExpectedSize);
        result->SetTaskReason(ReasonName(task.Reason));
        for (const auto& cut : task.CheckPoints) {
            // The merger's include flag puts a row equal to the key into the left slice (implementation_plan.md,
            // "Verified in code", item 2), which is the core's KeyBelongsToLeft.
            NArrow::NMerger::TSortableBatchPosition position(cut.Key.ToBatch(), 0, PrimaryKeysSchema->field_names(), {}, false);
            result->AddCheckPoint(position, cut.KeyBelongsToLeft);
        }
        YDB_LOG_DEBUG("",
            {"event", "tiling-s_task"},
            {"task_id", result->GetTaskIdentifier()},
            {"kind", (ui32)task.Kind},
            {"reason", (ui32)task.Reason},
            {"portions", task.Portions.size()},
            {"bytes", task.Bytes},
            {"ideal_bytes", task.IdealBytes},
            {"target_level", (ui32)task.TargetLevel},
            {"cuts", task.CheckPoints.size()});
        return result;
    }

protected:
    /// Collects the wall time of one adapter call into its histogram, in microseconds (2026-10-27).
    class TTimed {
        const NMonitoring::THistogramPtr& Histogram;
        const TMonotonic Start = TMonotonic::Now();

    public:
        explicit TTimed(const NMonitoring::THistogramPtr& histogram)
            : Histogram(histogram)
        {
        }

        ~TTimed() {
            Histogram->Collect(static_cast<i64>((TMonotonic::Now() - Start).MicroSeconds()));
        }
    };

    void DoModifyPortions(const std::vector<TPortionInfo::TPtr>& add, const std::vector<TPortionInfo::TPtr>& remove) override {
        TTimed timed(Counters.PlannerModifyPortionsUs);
        Core.ModifyPortions(add, std::vector<TPortionInfo::TConstPtr>(remove.begin(), remove.end()));
        PublishSnapshots(BuildClock());
    }

    bool DoUsesPullCompactionScheduling() const override {
        return true;
    }

    ui32 DoGetMaxCompactionInflight() const override {
        return Settings.CompactionThreads;
    }

    std::shared_ptr<TColumnEngineChanges> DoGetNextOptimizationTask(
        std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const override {
        TTimed timed(Counters.PlannerNextTaskUs);
        const auto isLocked = [&dataLocksManager](const TPortionInfo::TConstPtr& p) {
            return IsLockedBy(dataLocksManager, p);
        };
        const auto clock = BuildClock();
        Core.UpdateClock(clock);
        PublishSnapshots(clock);
        // The request re-syncs the core first (decision 14) and takes the most urgent source: a level, the accumulator,
        // or the advance once S lags the clock by the delay trigger (decision 18). Below the trigger nothing is compacted
        // toward the line.
        const auto task = Core.TakeNextTask(isLocked);
        if (!task) {
            return nullptr;
        }
        return BuildChanges(granule, *task);
    }

    std::vector<std::shared_ptr<TColumnEngineChanges>> DoGetOptimizationTasks(
        std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const override {
        if (auto task = DoGetNextOptimizationTask(granule, dataLocksManager)) {
            return { std::move(task) };
        }
        return {};
    }

    /// Decisions 16 and 18: the highest source priority, the lag of S included, else the remembered priority of
    /// handed-out tasks (decision 14), else nothing: below the delay trigger the table asks for no compaction.
    TOptimizationPriority DoGetUsefulMetric() const override {
        TTimed timed(Counters.PlannerUsefulMetricUs);
        const auto clock = BuildClock();
        Core.UpdateClock(clock);
        PublishSnapshots(clock);
        return Core.GetSchedulingPriority().value_or(TOptimizationPriority::Zero());
    }

    /// PR 1 keeps the engine's rule for a forced compaction, every portion in the line, with the inputs of running tasks
    /// still counted as outside; PR 2 replaces it with S against R (design.md, "ALTER TABLE COMPACT").
    TConclusion<bool> DoCheckNoIntersections() const override {
        return Core.IsSettledForForcedRequest();
    }

    bool DoIsOverloaded() const override {
        TTimed timed(Counters.PlannerOverloadedUs);
        return Core.IsOverloaded();
    }

    void DoActualize(const TInstant /*currentInstant*/) override {
        // Decision 14, revised 2026-10-16: the core learns the clock and the lock state at task requests, nothing here.
    }

    NArrow::NMerger::TIntervalPositions GetBucketPositions() const override {
        return {};
    }

    std::vector<TTaskDescription> DoGetTasksDescription() const override {
        return {};
    }

    TString DoDebugString() const override {
        return DoSerializeToJsonVisual().GetStringRobust();
    }

    NJson::TJsonValue DoSerializeToJsonVisual() const override {
        NJson::TJsonValue info = NJson::JSON_MAP;
        info.InsertValue("1-Name", "TILING-S");
        info.InsertValue("2-Settings", Settings.SerializeToJson());
        info.InsertValue("3-Levels", Core.SerializeToJsonVisual());
        return info;
    }

public:
    TOptimizerPlannerAdapter(const TInternalPathId pathId, const std::shared_ptr<IStoragesManager>& storagesManager,
        const std::shared_ptr<arrow::Schema>& primaryKeysSchema, const TSnapshotTilingSettings& settings,
        const std::optional<ui64>& nodePortionsCountLimit)
        : TBase(pathId, nodePortionsCountLimit)
        , Settings(settings)
        , Counters()
        , Core(settings, Counters)
        , StoragesManager(storagesManager)
        , PrimaryKeysSchema(primaryKeysSchema)
    {
        AFL_VERIFY(StoragesManager);
        AFL_VERIFY(PrimaryKeysSchema);
    }

    ~TOptimizerPlannerAdapter() override {
        Counters.GetCompactedSnapshotCounters().Remove(PlannerId, BuildClock().Now.GetPlanStep() / 1000);
    }
};

}   // namespace

/// Registered as "tiling-s"; the settings travel in the same JSON blob of the planner container as tiling++'s.
class TSnapshotTilingPlannerConstructor: public IOptimizerPlannerConstructor {
private:
    using TBase = IOptimizerPlannerConstructor;
    TSnapshotTilingSettings Settings;

    void DoSerializeToProto(TProto& proto) const override {
        proto.MutableTiling()->SetJson(NJson::WriteJson(Settings.SerializeToJson(), /*formatOutput=*/false));
    }

    bool DoDeserializeFromProto(const TProto& proto) override {
        if (!proto.HasTiling()) {
            YDB_LOG_ERROR("",
                {"error", "cannot parse tiling-s compaction planner from proto"},
                {"proto", proto.DebugString()});
            return false;
        }
        if (!proto.GetTiling().HasJson()) {
            return true;
        }
        NJson::TJsonValue json;
        if (!NJson::ReadJsonFastTree(proto.GetTiling().GetJson(), &json)) {
            YDB_LOG_ERROR("",
                {"error", "cannot parse tiling-s compaction planner from proto"},
                {"description", "the settings are not valid JSON"});
            return false;
        }
        const auto status = Settings.DeserializeFromJson(json);
        if (status.IsFail()) {
            YDB_LOG_ERROR("",
                {"error", "cannot parse tiling-s compaction planner from proto"},
                {"description", status.GetErrorDescription()});
            return false;
        }
        return true;
    }

    TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override {
        return Settings.DeserializeFromJson(jsonInfo);
    }

    bool DoApplyToCurrentObject(IOptimizerPlanner& /*current*/) const override {
        return false;
    }

    TConclusion<std::shared_ptr<IOptimizerPlanner>> DoBuildPlanner(const TBuildContext& context) const override {
        if (const auto status = Settings.Validate(); status.IsFail()) {
            return status;
        }
        YDB_LOG_DEBUG("",
            {"message", "creating tiling-s compaction optimizer"});
        return std::make_shared<TOptimizerPlannerAdapter>(
            context.GetPathId(), context.GetStorages(), context.GetPKSchema(), Settings, GetNodePortionsCountLimit());
    }

public:
    static TString GetClassNameStatic() {
        return "tiling-s";
    }

private:
    static inline const TFactory::TRegistrator<TSnapshotTilingPlannerConstructor> Registrator =
        TFactory::TRegistrator<TSnapshotTilingPlannerConstructor>(GetClassNameStatic());

public:
    TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NSnapshotTiling
