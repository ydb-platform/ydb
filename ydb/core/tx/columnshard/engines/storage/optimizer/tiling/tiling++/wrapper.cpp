#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/settings.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/tiling.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/actors/core/log.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/hash_set.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

namespace {

using TCoreTiling = Tiling<NArrow::TSimpleRow, TPortionInfo>;

/// JSON layout matches TTilingOptimizer in tiling++.cpp (same proto blob).
struct TPlannerSettings {
    TTilingSettings TilingSettings;
    ui64 LastLevelBytes = 10ULL * 1024 * 1024;
    ui64 PortionExpectedSize = 4ULL * 1024 * 1024;

    void SerializeToProto(NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TTilingOptimizer& proto) const {
        NJson::TJsonValue json(NJson::JSON_MAP);
        json["accumulator_portion_size_limit"] = TilingSettings.AccumulatorPortionSizeLimit;
        json["last_level_bytes"] = LastLevelBytes;
        json["k"] = (ui64)TilingSettings.K;
        json["portion_expected_size"] = PortionExpectedSize;
        json["last_level_compaction_portions"] = TilingSettings.LastLevelSettings.Compaction.Portions;
        json["last_level_compaction_bytes"] = TilingSettings.LastLevelSettings.Compaction.Bytes;
        json["last_level_candidate_portions_overload"] = TilingSettings.LastLevelSettings.CandidatePortionsOverload;
        json["accumulator_compaction_portions"] = TilingSettings.AccumulatorSettings.Compaction.Portions;
        json["accumulator_compaction_bytes"] = TilingSettings.AccumulatorSettings.Compaction.Bytes;
        json["accumulator_trigger_portions"] = TilingSettings.AccumulatorSettings.Trigger.Portions;
        json["accumulator_trigger_bytes"] = TilingSettings.AccumulatorSettings.Trigger.Bytes;
        json["accumulator_overload_portions"] = TilingSettings.AccumulatorSettings.Overload.Portions;
        json["accumulator_overload_bytes"] = TilingSettings.AccumulatorSettings.Overload.Bytes;
        json["middle_level_trigger_height"] = TilingSettings.MiddleLevelSettings.TriggerHight;
        json["middle_level_overload_height"] = TilingSettings.MiddleLevelSettings.OverloadHight;
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
                TilingSettings.AccumulatorPortionSizeLimit = value.GetUInteger();
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
                TilingSettings.K = static_cast<ui8>(kv);
            } else if (name == "portion_expected_size") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: portion_expected_size must be an unsigned integer");
                }
                PortionExpectedSize = value.GetUInteger();
            } else if (name == "last_level_compaction_portions") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: last_level_compaction_portions must be an unsigned integer");
                }
                TilingSettings.LastLevelSettings.Compaction.Portions = value.GetUInteger();
            } else if (name == "last_level_compaction_bytes") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: last_level_compaction_bytes must be an unsigned integer");
                }
                TilingSettings.LastLevelSettings.Compaction.Bytes = value.GetUInteger();
            } else if (name == "last_level_candidate_portions_overload") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: last_level_candidate_portions_overload must be an unsigned integer");
                }
                TilingSettings.LastLevelSettings.CandidatePortionsOverload = value.GetUInteger();
            } else if (name == "accumulator_compaction_portions") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: accumulator_compaction_portions must be an unsigned integer");
                }
                TilingSettings.AccumulatorSettings.Compaction.Portions = value.GetUInteger();
            } else if (name == "accumulator_compaction_bytes") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: accumulator_compaction_bytes must be an unsigned integer");
                }
                TilingSettings.AccumulatorSettings.Compaction.Bytes = value.GetUInteger();
            } else if (name == "accumulator_trigger_portions") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: accumulator_trigger_portions must be an unsigned integer");
                }
                TilingSettings.AccumulatorSettings.Trigger.Portions = value.GetUInteger();
            } else if (name == "accumulator_trigger_bytes") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: accumulator_trigger_bytes must be an unsigned integer");
                }
                TilingSettings.AccumulatorSettings.Trigger.Bytes = value.GetUInteger();
            } else if (name == "accumulator_overload_portions") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: accumulator_overload_portions must be an unsigned integer");
                }
                TilingSettings.AccumulatorSettings.Overload.Portions = value.GetUInteger();
            } else if (name == "accumulator_overload_bytes") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: accumulator_overload_bytes must be an unsigned integer");
                }
                TilingSettings.AccumulatorSettings.Overload.Bytes = value.GetUInteger();
            } else if (name == "middle_level_trigger_height") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: middle_level_trigger_height must be an unsigned integer");
                }
                TilingSettings.MiddleLevelSettings.TriggerHight = value.GetUInteger();
            } else if (name == "middle_level_overload_height") {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail("tiling-core: middle_level_overload_height must be an unsigned integer");
                }
                TilingSettings.MiddleLevelSettings.OverloadHight = value.GetUInteger();
            } else {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                    "event", "tiling_core_unknown_setting_ignored")("setting", name);
            }
        }
        return TConclusionStatus::Success();
    }
};

TTilingSettings MakeCoreSettings(const TPlannerSettings& settings) {
    return settings.TilingSettings;
}

class TOptimizerPlannerAdapter: public IOptimizerPlanner {
private:
    using TBase = IOptimizerPlanner;
    TCounters Counters;
    TCoreTiling Core;
    std::shared_ptr<IStoragesManager> StoragesManager;
    std::shared_ptr<arrow::Schema> PrimaryKeysSchema;
    ui64 PortionExpectedSize;

protected:
    void DoModifyPortions(const std::vector<TPortionInfo::TPtr>& add, const std::vector<TPortionInfo::TPtr>& remove) override {
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
            return dataLocksManager && dataLocksManager->IsLocked(*p, NDataLocks::ELockCategory::Compaction).has_value();
        };

        const auto tasks = Core.GetOptimizationTasks(isLocked);
        if (tasks.empty()) {
            return {};
        }

        const auto& task = tasks.front();
        auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, task.Portions, TSaverContext(StoragesManager));
        result->SetTargetCompactionLevel(task.TargetLevel);
        result->SetPortionExpectedSize(PortionExpectedSize);
        return {result};
    }

    TOptimizationPriority DoGetUsefulMetric() const override {
        return Core.DoGetUsefulMetric();
    }

    bool DoIsOverloaded() const override {
        return Core.DoGetUsefulMetric().IsCritical();
    }

    void DoActualize(const TInstant /*currentInstant*/) override {
    }

    NArrow::NMerger::TIntervalPositions GetBucketPositions() const override {
        return {};
    }

    std::vector<TTaskDescription> DoGetTasksDescription() const override {
        return {};
    }

public:
    TOptimizerPlannerAdapter(
        const TInternalPathId pathId,
        const std::shared_ptr<IStoragesManager>& storagesManager,
        const std::shared_ptr<arrow::Schema>& primaryKeysSchema,
        const TPlannerSettings& settings)
        : TBase(pathId, std::nullopt)
        , Counters()
        , Core(MakeCoreSettings(settings), Counters)
        , StoragesManager(storagesManager)
        , PrimaryKeysSchema(primaryKeysSchema)
        , PortionExpectedSize(settings.PortionExpectedSize) {
        AFL_VERIFY(StoragesManager);
        Y_UNUSED(PrimaryKeysSchema);
    }
};

} // namespace

class TTilingOptimizerPlannerConstructor: public IOptimizerPlannerConstructor {
private:
    using TBase = IOptimizerPlannerConstructor;
    TPlannerSettings Settings;

    void DoSerializeToProto(TProto& proto) const override {
        Settings.SerializeToProto(*proto.MutableTiling());
    }

    bool DoDeserializeFromProto(const TProto& proto) override {
        if (!proto.HasTiling()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse tiling++ compaction optimizer from proto")("proto", proto.DebugString());
            return false;
        }
        auto status = Settings.DeserializeFromProto(proto.GetTiling());
        if (!status.IsSuccess()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse tiling++ compaction optimizer from proto")("description", status.GetErrorDescription());
            return false;
        }
        return true;
    }

    TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override {
        return Settings.DeserializeFromJson(jsonInfo);
    }

    bool DoApplyToCurrentObject(IOptimizerPlanner& current) const override {
        Y_UNUSED(current);
        return false;
    }

    TConclusion<std::shared_ptr<IOptimizerPlanner>> DoBuildPlanner(const TBuildContext& context) const override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "creating tiling++ compaction optimizer");
        return std::make_shared<TOptimizerPlannerAdapter>(context.GetPathId(), context.GetStorages(), context.GetPKSchema(), Settings);
    }

public:
    static TString GetClassNameStatic() {
        return "tiling++";
    }

private:
    static inline const TFactory::TRegistrator<TTilingOptimizerPlannerConstructor> Registrator =
        TFactory::TRegistrator<TTilingOptimizerPlannerConstructor>(GetClassNameStatic());

public:

    TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
