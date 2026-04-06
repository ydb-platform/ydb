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
    ui64 InitialBlobBytes    = 1ULL * 1025 * 1024 * 1024;  // 1 GiB
    ui64 LastLevelBytes      = 10ULL  * 1024 * 1024;   // 10 MiB
    ui8  K                   = 10;
    ui64 PortionExpectedSize = 4ULL  * 1024 * 1024;   // 4 MiB

    // Serialise to the TTilingOptimizer proto (stores as JSON blob).
    void SerializeToProto(NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TTilingOptimizer& proto) const {
        NJson::TJsonValue json(NJson::JSON_MAP);
        json["initial_blob_bytes"]    = InitialBlobBytes;
        json["last_level_bytes"]      = LastLevelBytes;
        json["k"]                     = (ui64)K;
        json["portion_expected_size"] = PortionExpectedSize;
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
            if (name == "initial_blob_bytes") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling: initial_blob_bytes must be an unsigned integer");
                }
                InitialBlobBytes = value.GetUInteger();
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
            } else {
                return TConclusionStatus::Fail(TStringBuilder() << "tiling: unknown setting '" << name << "'");
            }
        }
        if (InitialBlobBytes < LastLevelBytes * K) {
            return TConclusionStatus::Fail(
                "tiling: initial_blob_bytes must be at least k * last_level_bytes");
        }
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
        , LastLevel(LastLevel::LastLevelSettings())
        , MiddleLevels(MiddleLevels::MiddleLevel::MiddleLevelSettings())
        , Accumulator(Accumulator::AccumulatorSettings())
        , StoragesManager(storagesManager)
        , PrimaryKeysSchema(primaryKeysSchema)
        , Settings(settings)
        , Counters(std::make_shared<TCounters>())
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
    LastLevel LastLevel;
    MiddleLevels MiddleLevels;
    Accumulator Accumulator;
    THashMap<ui64, ui8> InternalLevel;

    std::shared_ptr<IStoragesManager> StoragesManager;
    std::shared_ptr<arrow::Schema> PrimaryKeysSchema;

    TSettings Settings;
    // ui64 TotalBlobBytes = 0;
    // ui64 TotalRecordsCount = 0;

    std::shared_ptr<TCounters> Counters;
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
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_remove_portion_skipped")(
                "reason", "not_tracked_type")(
                "portion_id", p->GetPortionId())(
                "produced", (ui32)produced);
            return;
        }

        PortionsInfo->RemovePortion(p);

        auto lit = InternalLevel.find(p->GetPortionId());
        if (lit == InternalLevel.end()) {
            // Portion was never tracked (e.g. added before planner switch). Log and skip.
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_remove_unknown_portion")(
                "portion_id", p->GetPortionId())(
                "produced", (ui32)produced);
            return;
        }
        if (InternalLevel.at(p) == 0) {
            Accumulator.Remove(p);
        } else if (InternalLevel.at(p) == 1) {
            LastLevel.Remove(p);
        } else {
            MiddleLevels.Remove(p, InternalLevel.at(p));
        }
        InternalLevel.erase(lit);
    }

    void AddPortion(std::shared_ptr<TPortionInfo> p) {
        switch (p->GetProduced()) {
            case NPortion::EVICTED:
            case NPortion::INACTIVE:
                // Tiered-out or deleted portions: skip silently (same as old tiling planner).
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                    "event", "tiling_add_portion_skipped")(
                    "reason", "evicted_or_inactive")(
                    "portion_id", p->GetPortionId())(
                    "produced", (ui32)p->GetProduced());
                return;
            case NPortion::UNSPECIFIED:
            case NPortion::COMPACTED:
                // Legacy / unexpected values: log and skip rather than crash.
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                    "event", "tiling_add_portion_unexpected_type")(
                    "portion_id", p->GetPortionId())(
                    "produced", (ui32)p->GetProduced());
                return;
            default:
                break;
        }

        PortionsInfo->AddPortion(p);

        if (p->GetColumnBlobBytes() < 512 * 1024 * 1024) {
            Accumulator.Add(p);
            InternalLevel[p->GetPortionId()] = 0;
        } else {
            ui8 level = log10(LastLevel.Measure(p)) + 1;
            if (level <= 1) {
                LastLevel.Add(p);
                InternalLevel[p->GetPortionId()] = 1;
            } else {
                MiddleLevels.Add(p, level);
                InternalLevel[p->GetPortionId()] = level;
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

        if (lastLevelPriority < accumulatorPriority && middleLevelsPriority < accumulatorPriority) {
            auto tasks = Accumulator.GetOptimizationTasks(dataLocksManager);
            if (tasks.size() > 1) {
                return {std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, tasks, TSaverContext(StoragesManager))};
            }
        }
        else if (lastLevelPriority < middleLevelsPriority) {
            auto tasks = MiddleLevels.GetOptimizationTasks(dataLocksManager);
            if (tasks.size() > 1) {
                return {std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, tasks, TSaverContext(StoragesManager))};
            }
        }
        else {
            auto tasks = LastLevel.GetOptimizationTasks(dataLocksManager);
            if (tasks.size() > 1) {
                return {std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, tasks, TSaverContext(StoragesManager))};
            }
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
        return "tiling";
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
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("message", "creating tiling compaction optimizer (intersection-tree)");
        return std::make_shared<TOptimizerPlanner>(context.GetPathId(), context.GetStorages(), context.GetPKSchema(), Settings);
    }
};

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
