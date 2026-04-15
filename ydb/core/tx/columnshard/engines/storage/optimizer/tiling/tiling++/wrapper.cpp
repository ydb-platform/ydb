#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/tiling_impl.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

namespace {

using TCoreCounters = TCounters<TPortionInfo>;
using TCoreTiling = Tiling<NArrow::TSimpleRow, TPortionInfo, TCoreCounters>;

/// JSON layout matches TTilingOptimizer in tiling++.cpp (same proto blob).
struct TPlannerSettings {
    ui64 AccumulatorPortionSizeLimit = 512ULL * 1024;
    ui64 LastLevelBytes = 10ULL * 1024 * 1024;
    ui8 K = 10;
    ui64 PortionExpectedSize = 4ULL * 1024 * 1024;

    ui64 LastLevelCompactionPortions = 1'000;
    ui64 LastLevelCompactionBytes = 64ULL * 1024 * 1024;
    ui64 LastLevelCandidatePortionsOverload = 10;

    ui64 AccumulatorCompactionPortions = 1'000;
    ui64 AccumulatorCompactionBytes = 64ULL * 1024 * 1024;
    ui64 AccumulatorTriggerPortions = 1'000;
    ui64 AccumulatorTriggerBytes = 2ULL * 1024 * 1024;
    ui64 AccumulatorOverloadPortions = 10'000;
    ui64 AccumulatorOverloadBytes = 256ULL * 1024 * 1024;

    ui64 MiddleLevelTriggerHeight = 10;
    ui64 MiddleLevelOverloadHeight = 15;

    TCoreTiling::TLastLevelSettings MakeLastLevelSettings() const {
        TCoreTiling::TLastLevelSettings s;
        s.Compaction.Portions = LastLevelCompactionPortions;
        s.Compaction.Bytes = LastLevelCompactionBytes;
        s.CandidatePortionsOverload = LastLevelCandidatePortionsOverload;
        return s;
    }

    TCoreTiling::TAccumulatorSettings MakeAccumulatorSettings() const {
        TCoreTiling::TAccumulatorSettings s;
        s.Compaction.Portions = AccumulatorCompactionPortions;
        s.Compaction.Bytes = AccumulatorCompactionBytes;
        s.Trigger.Portions = AccumulatorTriggerPortions;
        s.Trigger.Bytes = AccumulatorTriggerBytes;
        s.Overload.Portions = AccumulatorOverloadPortions;
        s.Overload.Bytes = AccumulatorOverloadBytes;
        return s;
    }

    TCoreTiling::TMiddleLevelSettings MakeMiddleLevelSettings() const {
        TCoreTiling::TMiddleLevelSettings s;
        s.TriggerHight = MiddleLevelTriggerHeight;
        s.OverloadHight = MiddleLevelOverloadHeight;
        return s;
    }

    void SerializeToProto(NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TTilingOptimizer& proto) const {
        NJson::TJsonValue json(NJson::JSON_MAP);
        json["accumulator_portion_size_limit"] = AccumulatorPortionSizeLimit;
        json["last_level_bytes"] = LastLevelBytes;
        json["k"] = (ui64)K;
        json["portion_expected_size"] = PortionExpectedSize;
        json["last_level_compaction_portions"] = LastLevelCompactionPortions;
        json["last_level_compaction_bytes"] = LastLevelCompactionBytes;
        json["last_level_candidate_portions_overload"] = LastLevelCandidatePortionsOverload;
        json["accumulator_compaction_portions"] = AccumulatorCompactionPortions;
        json["accumulator_compaction_bytes"] = AccumulatorCompactionBytes;
        json["accumulator_trigger_portions"] = AccumulatorTriggerPortions;
        json["accumulator_trigger_bytes"] = AccumulatorTriggerBytes;
        json["accumulator_overload_portions"] = AccumulatorOverloadPortions;
        json["accumulator_overload_bytes"] = AccumulatorOverloadBytes;
        json["middle_level_trigger_height"] = MiddleLevelTriggerHeight;
        json["middle_level_overload_height"] = MiddleLevelOverloadHeight;
        proto.SetJson(NJson::WriteJson(json, /*formatOutput=*/false));
    }

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TTilingOptimizer& proto) {
        if (!proto.HasJson()) {
            return TConclusionStatus::Success();
        }
        NJson::TJsonValue jsonInfo;
        if (!NJson::ReadJsonFastTree(proto.GetJson(), &jsonInfo)) {
            return TConclusionStatus::Fail("tiling-core: cannot parse previously serialised JSON");
        }
        return DeserializeFromJson(jsonInfo);
    }

    TConclusionStatus DeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
        if (!jsonInfo.IsMap()) {
            return TConclusionStatus::Fail("tiling-core: FEATURES must be a JSON object");
        }
        for (const auto& [name, value] : jsonInfo.GetMapSafe()) {
            if (name == "accumulator_portion_size_limit") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: accumulator_portion_size_limit must be an unsigned integer");
                }
                AccumulatorPortionSizeLimit = value.GetUInteger();
            } else if (name == "last_level_bytes") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: last_level_bytes must be an unsigned integer");
                }
                LastLevelBytes = value.GetUInteger();
            } else if (name == "k") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: k must be an unsigned integer");
                }
                const ui64 kv = value.GetUInteger();
                if (kv < 2 || kv > 255) {
                    return TConclusionStatus::Fail("tiling-core: k must be in [2, 255]");
                }
                K = static_cast<ui8>(kv);
            } else if (name == "portion_expected_size") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: portion_expected_size must be an unsigned integer");
                }
                PortionExpectedSize = value.GetUInteger();
            } else if (name == "last_level_compaction_portions") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: last_level_compaction_portions must be an unsigned integer");
                }
                LastLevelCompactionPortions = value.GetUInteger();
            } else if (name == "last_level_compaction_bytes") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: last_level_compaction_bytes must be an unsigned integer");
                }
                LastLevelCompactionBytes = value.GetUInteger();
            } else if (name == "last_level_candidate_portions_overload") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: last_level_candidate_portions_overload must be an unsigned integer");
                }
                LastLevelCandidatePortionsOverload = value.GetUInteger();
            } else if (name == "accumulator_compaction_portions") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: accumulator_compaction_portions must be an unsigned integer");
                }
                AccumulatorCompactionPortions = value.GetUInteger();
            } else if (name == "accumulator_compaction_bytes") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: accumulator_compaction_bytes must be an unsigned integer");
                }
                AccumulatorCompactionBytes = value.GetUInteger();
            } else if (name == "accumulator_trigger_portions") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: accumulator_trigger_portions must be an unsigned integer");
                }
                AccumulatorTriggerPortions = value.GetUInteger();
            } else if (name == "accumulator_trigger_bytes") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: accumulator_trigger_bytes must be an unsigned integer");
                }
                AccumulatorTriggerBytes = value.GetUInteger();
            } else if (name == "accumulator_overload_portions") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: accumulator_overload_portions must be an unsigned integer");
                }
                AccumulatorOverloadPortions = value.GetUInteger();
            } else if (name == "accumulator_overload_bytes") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: accumulator_overload_bytes must be an unsigned integer");
                }
                AccumulatorOverloadBytes = value.GetUInteger();
            } else if (name == "middle_level_trigger_height") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: middle_level_trigger_height must be an unsigned integer");
                }
                MiddleLevelTriggerHeight = value.GetUInteger();
            } else if (name == "middle_level_overload_height") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: middle_level_overload_height must be an unsigned integer");
                }
                MiddleLevelOverloadHeight = value.GetUInteger();
            } else {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                    "event", "tiling_core_unknown_setting_ignored")("setting", name);
            }
        }
        return TConclusionStatus::Success();
    }
};

TCoreTiling::TilingSettings MakeCoreSettings(const TPlannerSettings& s) {
    TCoreTiling::TilingSettings ts;
    ts.AccumulatorSettings = s.MakeAccumulatorSettings();
    ts.LastLevelSettings = s.MakeLastLevelSettings();
    ts.MiddleLevelSettings = s.MakeMiddleLevelSettings();
    ts.AccumulatorPortionSizeLimit = s.AccumulatorPortionSizeLimit;
    ts.K = s.K;
    ts.MiddleLevelCount = TILING_LAYERS_COUNT;
    return ts;
}

} // namespace

/// IOptimizerPlanner that delegates routing and task selection to Tiling (tiling.cpp).
class TOptimizerPlannerCoreTiling: public IOptimizerPlanner {
public:
    TOptimizerPlannerCoreTiling(
        const TInternalPathId pathId,
        const std::shared_ptr<IStoragesManager>& storagesManager,
        const std::shared_ptr<arrow::Schema>& primaryKeysSchema,
        const TPlannerSettings& settings)
        : IOptimizerPlanner(pathId, std::nullopt)
        , Core(MakeCoreSettings(settings))
        , StoragesManager(storagesManager)
        , PrimaryKeysSchema(primaryKeysSchema)
        , Settings(settings) {
        AFL_VERIFY(StoragesManager);
        Y_UNUSED(PrimaryKeysSchema);
    }

    TOptimizationPriority DoGetUsefulMetric() const override {
        return Core.DoGetUsefulMetric();
    }

    bool DoIsOverloaded() const override {
        return DoGetUsefulMetric().IsCritical();
    }

private:
    TCoreTiling Core;
    std::shared_ptr<IStoragesManager> StoragesManager;
    std::shared_ptr<arrow::Schema> PrimaryKeysSchema;
    TPlannerSettings Settings;

    void DoModifyPortions(
        const std::vector<std::shared_ptr<TPortionInfo>>& add, const std::vector<std::shared_ptr<TPortionInfo>>& remove) override {
        for (const auto& p : remove) {
            Core.RemovePortion(p);
        }
        for (const auto& p : add) {
            Core.AddPortion(p);
        }
        Core.DoActualize();
    }

    std::vector<std::shared_ptr<TColumnEngineChanges>> DoGetOptimizationTasks(
        std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const override {

        const auto isLocked = [dataLocksManager](TPortionInfo::TConstPtr p) -> bool {
            return dataLocksManager &&
                dataLocksManager->IsLocked(*p, NDataLocks::ELockCategory::Compaction).has_value();
        };

        const auto tasks = Core.GetOptimizationTasks(isLocked);
        if (tasks.empty()) {
            return {};
        }
        const auto& portionPtrs = tasks.front().Portions;

        const auto accumulatorPriority = Core.Accumulator.DoGetUsefulMetric();
        const auto lastLevelPriority = Core.LastLevel.DoGetUsefulMetric();
        const auto middleLevelsPriority = Core.GetMiddleUsefulMetric().first;

        const bool pickAccumulator = lastLevelPriority < accumulatorPriority && middleLevelsPriority < accumulatorPriority;
        const bool pickMiddle = !pickAccumulator && lastLevelPriority < middleLevelsPriority;

        if (pickAccumulator || pickMiddle) {
            if (portionPtrs.size() <= 1) {
                return {};
            }
            auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, portionPtrs, TSaverContext(StoragesManager));
            result->SetTargetCompactionLevel(0);
            result->SetPortionExpectedSize(Settings.PortionExpectedSize);
            return {result};
        }

        auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, portionPtrs, TSaverContext(StoragesManager));
        result->SetTargetCompactionLevel(1);
        result->SetPortionExpectedSize(Settings.PortionExpectedSize);
        return {result};
    }

    void DoActualize(const TInstant /*currentInstant*/) override {
    }

    NArrow::NMerger::TIntervalPositions GetBucketPositions() const override {
        return {};
    }

    std::vector<TTaskDescription> DoGetTasksDescription() const override {
        return {};
    }
};

class TOptimizerPlannerCoreTilingConstructor: public IOptimizerPlannerConstructor {
public:
    static TString GetClassNameStatic() {
        return "tiling++";
    }

    TString GetClassName() const override {
        return GetClassNameStatic();
    }

private:
    static inline const TFactory::TRegistrator<TOptimizerPlannerCoreTilingConstructor> Registrator =
        TFactory::TRegistrator<TOptimizerPlannerCoreTilingConstructor>(GetClassNameStatic());

    TPlannerSettings Settings;

    void DoSerializeToProto(TProto& proto) const override {
        Settings.SerializeToProto(*proto.MutableTiling());
    }

    bool DoDeserializeFromProto(const TProto& proto) override {
        if (!proto.HasTiling()) {
            return true;
        }
        const auto status = Settings.DeserializeFromProto(proto.GetTiling());
        if (!status.IsSuccess()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "error", "cannot parse tiling-core compaction optimizer from proto")(
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
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "creating tiling-core compaction optimizer (Tiling)");
        return std::make_shared<TOptimizerPlannerCoreTiling>(
            context.GetPathId(), context.GetStorages(), context.GetPKSchema(), Settings);
    }
};

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
