#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/levels.h>
#include <ydb/library/intersection_tree/intersection_tree.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

#include <algorithm>
#include <limits>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

// ---------------------------------------------------------------------------
// TSettings — configurable knobs for the new tiling compaction planner.
//
// JSON keys (all optional, validated on parse):
//   "initial_blob_bytes"    – ui64, minimum total-data budget for level-1 (default 128 MiB)
//   "last_level_bytes"      – ui64, stop adding levels when budget drops below this (default 1 MiB)
//   "k"                     – ui8,  fan-out factor between consecutive levels (default 10)
//   "portion_expected_size" – ui64, target portion size produced by compaction (default 4 MiB)
// ---------------------------------------------------------------------------
struct TSettings {
    // Production defaults — aggressive test values must be set explicitly via JSON.
    // AccumulatorPortionSizeLimit: portions with blob bytes below this threshold are routed
    // to the accumulator level; larger portions go directly to LastLevel / MiddleLevels.
    // Default matches PortionExpectedSize so that freshly-written small portions accumulate
    // and are merged before being promoted.
    ui64 AccumulatorPortionSizeLimit = 4ULL * 1024 * 1024;  // 4 MiB
    ui64 LastLevelBytes      = 10ULL  * 1024 * 1024;   // 10 MiB
    ui8  K                   = 10;
    ui64 PortionExpectedSize = 4ULL  * 1024 * 1024;   // 4 MiB

    // LastLevel settings
    ui64 LastLevelCompactionPortions = 1'000;
    ui64 LastLevelCompactionBytes    = 64ULL * 1024 * 1024;  // 64 MiB
    ui64 LastLevelCandidatePortionsOverload = 10;

    // Accumulator settings
    ui64 AccumulatorCompactionPortions = 1'000;
    ui64 AccumulatorCompactionBytes    = 64ULL * 1024 * 1024;  // 64 MiB
    ui64 AccumulatorTriggerPortions    = 1'000;
    ui64 AccumulatorTriggerBytes       = 2ULL * 1024 * 1024;   // 2 MiB
    ui64 AccumulatorOverloadPortions   = 10'000;
    ui64 AccumulatorOverloadBytes      = 256ULL * 1024 * 1024; // 256 MiB

    // MiddleLevel settings
    ui64 MiddleLevelTriggerHeight  = 10;
    ui64 MiddleLevelOverloadHeight = 15;

    LastLevel::LastLevelSettings MakeLastLevelSettings() const {
        LastLevel::LastLevelSettings s;
        s.Compaction.Portions = LastLevelCompactionPortions;
        s.Compaction.Bytes    = LastLevelCompactionBytes;
        s.CandidatePortionsOverload = LastLevelCandidatePortionsOverload;
        return s;
    }

    Accumulator::AccumulatorSettings MakeAccumulatorSettings() const {
        Accumulator::AccumulatorSettings s;
        s.Compaction.Portions = AccumulatorCompactionPortions;
        s.Compaction.Bytes    = AccumulatorCompactionBytes;
        s.Trigger.Portions    = AccumulatorTriggerPortions;
        s.Trigger.Bytes       = AccumulatorTriggerBytes;
        s.Overload.Portions   = AccumulatorOverloadPortions;
        s.Overload.Bytes      = AccumulatorOverloadBytes;
        return s;
    }

    MiddleLevels::MiddleLevel::MiddleLevelSettings MakeMiddleLevelSettings() const {
        MiddleLevels::MiddleLevel::MiddleLevelSettings s;
        s.TriggerHight  = MiddleLevelTriggerHeight;
        s.OverloadHight = MiddleLevelOverloadHeight;
        return s;
    }

    // Serialise to the TTilingOptimizer proto (stores as JSON blob).
    void SerializeToProto(NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TTilingOptimizer& proto) const {
        NJson::TJsonValue json(NJson::JSON_MAP);
        json["accumulator_portion_size_limit"] = AccumulatorPortionSizeLimit;
        json["last_level_bytes"]      = LastLevelBytes;
        json["k"]                     = (ui64)K;
        json["portion_expected_size"] = PortionExpectedSize;
        // Level settings
        json["last_level_compaction_portions"]         = LastLevelCompactionPortions;
        json["last_level_compaction_bytes"]            = LastLevelCompactionBytes;
        json["last_level_candidate_portions_overload"] = LastLevelCandidatePortionsOverload;
        json["accumulator_compaction_portions"]        = AccumulatorCompactionPortions;
        json["accumulator_compaction_bytes"]           = AccumulatorCompactionBytes;
        json["accumulator_trigger_portions"]           = AccumulatorTriggerPortions;
        json["accumulator_trigger_bytes"]              = AccumulatorTriggerBytes;
        json["accumulator_overload_portions"]          = AccumulatorOverloadPortions;
        json["accumulator_overload_bytes"]             = AccumulatorOverloadBytes;
        json["middle_level_trigger_height"]            = MiddleLevelTriggerHeight;
        json["middle_level_overload_height"]           = MiddleLevelOverloadHeight;
        proto.SetJson(NJson::WriteJson(json, /*formatOutput=*/false));
    }

    TConclusionStatus DeserializeFromProto(
        const NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TTilingOptimizer& proto)
    {
        if (!proto.HasJson()) {
            return TConclusionStatus::Success();
        }
        NJson::TJsonValue jsonInfo;
        if (!NJson::ReadJsonFastTree(proto.GetJson(), &jsonInfo)) {
            return TConclusionStatus::Fail("tiling: cannot parse previously serialised JSON");
        }
        return DeserializeFromJson(jsonInfo);
    }

    TConclusionStatus DeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
        if (!jsonInfo.IsMap()) {
            return TConclusionStatus::Fail("tiling: FEATURES must be a JSON object");
        }
        for (const auto& [name, value] : jsonInfo.GetMapSafe()) {
            if (name == "accumulator_portion_size_limit") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling: accumulator_portion_size_limit must be an unsigned integer");
                }
                AccumulatorPortionSizeLimit = value.GetUInteger();
            } else if (name == "last_level_bytes") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling: last_level_bytes must be an unsigned integer");
                }
                LastLevelBytes = value.GetUInteger();
            } else if (name == "k") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling: k must be an unsigned integer");
                }
                const ui64 kv = value.GetUInteger();
                if (kv < 2 || kv > 255) {
                    return TConclusionStatus::Fail("tiling: k must be in [2, 255]");
                }
                K = static_cast<ui8>(kv);
            } else if (name == "portion_expected_size") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling: portion_expected_size must be an unsigned integer");
                }
                PortionExpectedSize = value.GetUInteger();
            } else if (name == "last_level_compaction_portions") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling: last_level_compaction_portions must be an unsigned integer");
                }
                LastLevelCompactionPortions = value.GetUInteger();
            } else if (name == "last_level_compaction_bytes") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling: last_level_compaction_bytes must be an unsigned integer");
                }
                LastLevelCompactionBytes = value.GetUInteger();
            } else if (name == "last_level_candidate_portions_overload") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling: last_level_candidate_portions_overload must be an unsigned integer");
                }
                LastLevelCandidatePortionsOverload = value.GetUInteger();
            } else if (name == "accumulator_compaction_portions") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling: accumulator_compaction_portions must be an unsigned integer");
                }
                AccumulatorCompactionPortions = value.GetUInteger();
            } else if (name == "accumulator_compaction_bytes") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling: accumulator_compaction_bytes must be an unsigned integer");
                }
                AccumulatorCompactionBytes = value.GetUInteger();
            } else if (name == "accumulator_trigger_portions") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling: accumulator_trigger_portions must be an unsigned integer");
                }
                AccumulatorTriggerPortions = value.GetUInteger();
            } else if (name == "accumulator_trigger_bytes") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling: accumulator_trigger_bytes must be an unsigned integer");
                }
                AccumulatorTriggerBytes = value.GetUInteger();
            } else if (name == "accumulator_overload_portions") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling: accumulator_overload_portions must be an unsigned integer");
                }
                AccumulatorOverloadPortions = value.GetUInteger();
            } else if (name == "accumulator_overload_bytes") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling: accumulator_overload_bytes must be an unsigned integer");
                }
                AccumulatorOverloadBytes = value.GetUInteger();
            } else if (name == "middle_level_trigger_height") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling: middle_level_trigger_height must be an unsigned integer");
                }
                MiddleLevelTriggerHeight = value.GetUInteger();
            } else if (name == "middle_level_overload_height") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling: middle_level_overload_height must be an unsigned integer");
                }
                MiddleLevelOverloadHeight = value.GetUInteger();
            } else {
                // Unknown settings are silently ignored for forward-compatibility
                // (e.g. settings that exist in other tiling variants but not this one).
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                    "event", "tiling_unknown_setting_ignored")("setting", name);
            }
        }
        // if (InitialBlobBytes < LastLevelBytes * K) {
        //     return TConclusionStatus::Fail(
        //         "tiling: initial_blob_bytes must be at least k * last_level_bytes");
        // }
        return TConclusionStatus::Success();
    }
};

// ---------------------------------------------------------------------------

struct TSimpleKeyCompare {
    std::partial_ordering operator()(const NArrow::TSimpleRow& a, const NArrow::TSimpleRow& b) const {
        return a.CompareNotNull(b);
    }
};

class TOptimizerPlanner : public IOptimizerPlanner {
public:
    TOptimizerPlanner(
        const TInternalPathId pathId,
        const std::shared_ptr<IStoragesManager>& storagesManager,
        const std::shared_ptr<arrow::Schema>& primaryKeysSchema,
        const TSettings& settings)
        : IOptimizerPlanner(pathId, std::nullopt)
        , Counters(std::make_shared<TCounters>())
        // Level 0 → accumulator counter (accumulator family, index 0)
        , Accumulator(settings.MakeAccumulatorSettings(), Counters->GetAccumulatorCounters(0))
        // Level 1 → last-level counter (level family, index 1)
        , LastLevel(settings.MakeLastLevelSettings(), Counters->GetLevelCounters(1))
        // Middle levels 2..N → level counters at matching indices
        , MiddleLevels(settings.MakeMiddleLevelSettings(), *Counters)
        , StoragesManager(storagesManager)
        , PrimaryKeysSchema(primaryKeysSchema)
        , Settings(settings)
        , PortionsInfo(std::make_shared<TSimplePortionsGroupInfo>()) {
        AFL_VERIFY(StoragesManager);
        Y_UNUSED(PrimaryKeysSchema);
    }

    TOptimizationPriority DoGetUsefulMetric() const override {
        return std::max( LastLevel.DoGetUsefulMetric(), std::max(MiddleLevels.DoGetUsefulMetric(), Accumulator.DoGetUsefulMetric()));
    }

    virtual bool DoIsOverloaded() const override {
        return DoGetUsefulMetric().IsCritical();
    }

private:
    // Counters must be declared first — LastLevel, MiddleLevels, and Accumulator
    // hold const references into it and are initialised after it.
    std::shared_ptr<TCounters> Counters;

    Accumulator Accumulator;
    LastLevel LastLevel;
    MiddleLevels MiddleLevels;
    THashMap<ui64, ui8> InternalLevel;

    std::shared_ptr<IStoragesManager> StoragesManager;
    std::shared_ptr<arrow::Schema> PrimaryKeysSchema;

    TSettings Settings;
    // ui64 TotalBlobBytes = 0;
    // ui64 TotalRecordsCount = 0;

    std::shared_ptr<TSimplePortionsGroupInfo> PortionsInfo;

    void Actualize() {
        return;
    }

    void RemovePortion(std::shared_ptr<TPortionInfo> p) {
        // EVICTED and INACTIVE portions were never added to InternalLevel (skipped in AddPortion),
        // so attempting to remove them would be a no-op. Skip them here too.
        const auto produced = p->GetProduced();
        if (produced == NPortion::EVICTED || produced == NPortion::INACTIVE ||
            produced == NPortion::UNSPECIFIED || produced == NPortion::COMPACTED) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_remove_portion_skipped")(
                "reason", "not_tracked_type")(
                "portion_id", p->GetPortionId())(
                "produced", (ui32)produced);
            return;
        }

        PortionsInfo->RemovePortion(p);

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
            "event", "tiling_remove_portion")(
            "portion_id", p->GetPortionId())(
            "produced", (ui32)p->GetProduced())(
            "column_blob_bytes", p->GetColumnBlobBytes())(
            "total_blob_bytes", p->GetTotalBlobBytes())(
            "accumulator_size_limit", Settings.AccumulatorPortionSizeLimit)(
            "passes_limit", p->GetColumnBlobBytes() >= Settings.AccumulatorPortionSizeLimit);

        auto lit = InternalLevel.find(p->GetPortionId());
        if (lit == InternalLevel.end()) {
            // Portion was never tracked (e.g. added before planner switch). Log and skip.
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_remove_unknown_portion")(
                "portion_id", p->GetPortionId())(
                "produced", (ui32)produced);
            return;
        }
        if (lit->second == 0) {
            Accumulator.Remove(p);
        } else if (lit->second == 1) {
            LastLevel.Remove(p);
        } else {
            MiddleLevels.Remove(p, lit->second);
        }
        InternalLevel.erase(lit);
    }

    void AddPortion(std::shared_ptr<TPortionInfo> p) {
        switch (p->GetProduced()) {
            case NPortion::EVICTED:
            case NPortion::INACTIVE:
                // Tiered-out or deleted portions: skip silently (same as old tiling planner).
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                    "event", "tiling_add_portion_skipped")(
                    "reason", "evicted_or_inactive")(
                    "portion_id", p->GetPortionId())(
                    "produced", (ui32)p->GetProduced());
                return;
            case NPortion::UNSPECIFIED:
            case NPortion::COMPACTED:
                // Legacy / unexpected values: log and skip rather than crash.
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                    "event", "tiling_add_portion_unexpected_type")(
                    "portion_id", p->GetPortionId())(
                    "produced", (ui32)p->GetProduced());
                return;
            default:
                break;
        }

        PortionsInfo->AddPortion(p);



        if (p->GetColumnBlobBytes() < Settings.AccumulatorPortionSizeLimit) {
            Accumulator.Add(p);
            InternalLevel[p->GetPortionId()] = 0;
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
            "event", "tiling_add_portion_acc")(
            "portion_id", p->GetPortionId())(
            "produced", (ui32)p->GetProduced())(
            "total_raw_bytes", p->GetTotalRawBytes())(
            "total_blob_bytes", p->GetTotalBlobBytes())(
            "accumulator_size_limit", Settings.AccumulatorPortionSizeLimit)(
            "passes_limit", p->GetColumnBlobBytes() >= Settings.AccumulatorPortionSizeLimit);
        } else {
            const ui64 measure = LastLevel.Measure(p);
            // log10(0) is undefined; treat measure==0 as level 1 (LastLevel, no overlaps).
            // Use integer log_K(measure) to avoid floating-point rounding errors
            // (e.g. log(9)/log(3) may be 1.9999... instead of 2.0).
            // We compute floor(log_K(measure)) by repeated division.
            ui8 level = 1;
            if (measure > 0) {
                ui64 threshold = 1;
                while (threshold * Settings.K <= measure) {
                    threshold *= Settings.K;
                    ++level;
                }
            }
            if (level <= 1) {
                LastLevel.Add(p);
                InternalLevel[p->GetPortionId()] = 1;
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_add_portion_last")(
                "portion_id", p->GetPortionId())(
                "produced", (ui32)p->GetProduced())(
                "total_raw_bytes", p->GetTotalRawBytes())(
                "total_blob_bytes", p->GetTotalBlobBytes())(
                "accumulator_size_limit", Settings.AccumulatorPortionSizeLimit)(
                "passes_limit", p->GetColumnBlobBytes() >= Settings.AccumulatorPortionSizeLimit)(
                "measure", measure)(
                "level", level);
            } else {
                MiddleLevels.Add(p, level);
                InternalLevel[p->GetPortionId()] = level;
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_add_portion_middle")(
                "portion_id", p->GetPortionId())(
                "produced", (ui32)p->GetProduced())(
                "total_raw_bytes", p->GetTotalRawBytes())(
                "total_blob_bytes", p->GetTotalBlobBytes())(
                "accumulator_size_limit", Settings.AccumulatorPortionSizeLimit)(
                "passes_limit", p->GetColumnBlobBytes() >= Settings.AccumulatorPortionSizeLimit)(
                "measure", measure)(
                "level", level);
            }
        }
    }

    void DoModifyPortions(const std::vector<std::shared_ptr<TPortionInfo>>& add, const std::vector<std::shared_ptr<TPortionInfo>>& remove) override {
        for (const auto& p : remove) {
            RemovePortion(p);
        }
        for (const auto& p : add) {
            AddPortion(p);
        }
        Actualize();
    }

    std::vector<std::shared_ptr<TColumnEngineChanges>> DoGetOptimizationTasks(
        std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const override {

        auto accumulatorPriority = Accumulator.DoGetUsefulMetric();
        auto lastLevelPriority = LastLevel.DoGetUsefulMetric();
        auto middleLevelsPriority = MiddleLevels.DoGetUsefulMetric();

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
            "event", "tiling_get_optimization_tasks")(
            "accumulator_priority_level", accumulatorPriority.GetLevel())(
            "last_level_priority_level", lastLevelPriority.GetLevel())(
            "middle_levels_priority_level", middleLevelsPriority.GetLevel());

        auto makeConstTasks = [](const std::vector<TPortionInfo::TPtr>& src) {
            std::vector<TPortionInfo::TConstPtr> result;
            result.reserve(src.size());
            for (const auto& p : src) {
                result.push_back(p);
            }
            return result;
        };

        if (lastLevelPriority < accumulatorPriority && middleLevelsPriority < accumulatorPriority) {
            auto tasks = makeConstTasks(Accumulator.GetOptimizationTasks(dataLocksManager));
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_accumulator_tasks")(
                "tasks_size", tasks.size());
            if (tasks.size() > 1) {
                auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, tasks, TSaverContext(StoragesManager));
                // Use sentinel level 3 so that output portions can be identified in AddPortion
                // logs as "accumulator output" — makes it easy to see their sizes and verify
                // they graduate to LastLevel rather than looping back to the accumulator.
                // The actual routing in AddPortion still uses GetColumnBlobBytes() vs AccumulatorPortionSizeLimit.
                result->SetTargetCompactionLevel(0);
                result->SetPortionExpectedSize(Settings.PortionExpectedSize);
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                    "event", "tiling_accumulator_task_created")(
                    "tasks_size", tasks.size())(
                    "target_level", 3)(
                    "portion_expected_size", Settings.PortionExpectedSize);
                return {result};
            }
            // If accumulator is overloaded but we got < 2 tasks, all portions must be locked
            // (compaction already in progress). Log this so we can diagnose stuck compaction.
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_accumulator_no_task_produced")(
                "tasks_size", tasks.size())(
                "accumulator_priority_level", accumulatorPriority.GetLevel());
        }
        else if (lastLevelPriority < middleLevelsPriority) {
            auto tasks = makeConstTasks(MiddleLevels.GetOptimizationTasks(dataLocksManager));
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_middle_levels_tasks")(
                "tasks_size", tasks.size());
            if (tasks.size() > 1) {
                auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, tasks, TSaverContext(StoragesManager));
                result->SetTargetCompactionLevel(0);
                result->SetPortionExpectedSize(Settings.PortionExpectedSize);
                return {result};
            }
        }
        else {
            auto tasks = makeConstTasks(LastLevel.GetOptimizationTasks(dataLocksManager));
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_last_level_tasks")(
                "tasks_size", tasks.size())(
                    "portion_expected_size", Settings.PortionExpectedSize
                );
            // if (tasks.size() > 1) {
                auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, tasks, TSaverContext(StoragesManager));
                result->SetTargetCompactionLevel(1);
                result->SetPortionExpectedSize(Settings.PortionExpectedSize);
                return {result};
            // }
        }

        return {};
    }

    void DoActualize(const TInstant /*currentInstant*/) override {
        Actualize();
    }

    NArrow::NMerger::TIntervalPositions GetBucketPositions() const override {
        return {};
    }

    std::vector<TTaskDescription> DoGetTasksDescription() const override {
        return {};
    }
};

// ---------------------------------------------------------------------------
// TOptimizerPlannerConstructor — registered as "tiling".
// Supports COMPACTION_PLANNER.FEATURES JSON with keys:
//   initial_blob_bytes, last_level_bytes, k, portion_expected_size
// ---------------------------------------------------------------------------
class TOptimizerPlannerConstructor : public IOptimizerPlannerConstructor {
public:
    static TString GetClassNameStatic() {
        return "tiling++";
    }

    TString GetClassName() const override {
        return GetClassNameStatic();
    }

private:
    static inline const TFactory::TRegistrator<TOptimizerPlannerConstructor> Registrator =
        TFactory::TRegistrator<TOptimizerPlannerConstructor>(GetClassNameStatic());

    TSettings Settings;

    void DoSerializeToProto(TProto& proto) const override {
        Settings.SerializeToProto(*proto.MutableTiling());
    }

    bool DoDeserializeFromProto(const TProto& proto) override {
        if (!proto.HasTiling()) {
            return true;
        }
        auto status = Settings.DeserializeFromProto(proto.GetTiling());
        if (!status.IsSuccess()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "error", "cannot parse tiling compaction optimizer from proto")(
                "description", status.GetErrorDescription());
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
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "creating tiling compaction optimizer (intersection-tree)");
        return std::make_shared<TOptimizerPlanner>(context.GetPathId(), context.GetStorages(), context.GetPKSchema(), Settings);
    }
};

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling