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

#include <util/generic/hash_set.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

namespace {

using TCoreTiling = Tiling<NArrow::TSimpleRow, TPortionInfo>;

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
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
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

/// DEBUG: catch bad ModifyPortions batches before Core mutates counters / InternalLevelForDebug.
void DebugVerifyTilingCoreModifyBatch(
    const std::vector<std::shared_ptr<TPortionInfo>>& add,
    const std::vector<std::shared_ptr<TPortionInfo>>& remove) {
    for (const auto& p : remove) {
        AFL_VERIFY(!!p)("msg", "tiling++ DoModifyPortions: null shared_ptr in remove");
    }
    for (const auto& p : add) {
        AFL_VERIFY(!!p)("msg", "tiling++ DoModifyPortions: null shared_ptr in add");
    }
    THashSet<ui64> removeIds;
    removeIds.reserve(remove.size());
    for (const auto& p : remove) {
        const ui64 id = p->GetPortionId();
        if (removeIds.contains(id)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "msg", "tiling++ DoModifyPortions: duplicate portion_id in remove (second remove would skew counters)")(
                "portion_id", id)("produced", static_cast<ui32>(p->GetProduced()));
        }
        AFL_VERIFY(!removeIds.contains(id))("portion_id", id)("msg", "tiling++ duplicate portion_id in remove");
        removeIds.insert(id);
    }
    THashSet<ui64> addIds;
    addIds.reserve(add.size());
    for (const auto& p : add) {
        const ui64 id = p->GetPortionId();
        if (addIds.contains(id)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "msg", "tiling++ DoModifyPortions: duplicate portion_id in add (double Add would skew counters)")(
                "portion_id", id)("produced", static_cast<ui32>(p->GetProduced()));
        }
        AFL_VERIFY(!addIds.contains(id))("portion_id", id)("msg", "tiling++ duplicate portion_id in add");
        addIds.insert(id);
    }
}

/// DEBUG: portion must live in exactly one sub-level; `stored_*` comes from InternalLevelForDebug.
void TilingCoreDebugVerifyPortionHome(
    const TCoreTiling& core,
    TPortionInfo::TConstPtr p,
    ui8 storedLevel,
    ui64 storedWidth,
    bool onRemove) {
    const bool inAcc = core.Accumulator.HasPortion(p);
    const bool inLast = core.LastLevel.HasPortion(p);
    ui32 middleHits = 0;
    ui64 middleKey = 0;
    for (const auto& [k, ml] : core.MiddleLevels) {
        if (ml.HasPortion(p)) {
            ++middleHits;
            middleKey = k;
        }
    }
    const ui32 homes = static_cast<ui32>(inAcc) + static_cast<ui32>(inLast) + middleHits;
    const ui32 produced = static_cast<ui32>(p->GetProduced());
    if (homes != 1) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
            "msg", "tiling++: portion must live in exactly one sub-level")(
            "on_remove", onRemove)("portion_id", p->GetPortionId())("produced", produced)(
            "stored_level", (ui32)storedLevel)("stored_width", storedWidth)(
            "in_accumulator", inAcc)("in_last", inLast)("middle_hits", middleHits);
    }
    AFL_VERIFY(homes == 1)("portion_id", p->GetPortionId())("stored_level", storedLevel)("produced", produced);

    if (storedLevel == 0) {
        if (!(inAcc && !inLast && !middleHits)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "msg", "tiling++: stored_level=accumulator but physical placement disagrees")(
                "on_remove", onRemove)("portion_id", p->GetPortionId())("produced", produced)(
                "in_accumulator", inAcc)("in_last", inLast)("middle_hits", middleHits);
        }
        AFL_VERIFY(inAcc)("portion_id", p->GetPortionId())("stored_level", storedLevel)("produced", produced);
        AFL_VERIFY(!inLast)("portion_id", p->GetPortionId())("produced", produced);
        AFL_VERIFY(!middleHits)("portion_id", p->GetPortionId())("produced", produced);
    } else if (storedLevel == 1) {
        if (!(inLast && !inAcc && !middleHits)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "msg", "tiling++: stored_level=last but physical placement disagrees")(
                "on_remove", onRemove)("portion_id", p->GetPortionId())("produced", produced)(
                "in_accumulator", inAcc)("in_last", inLast)("middle_hits", middleHits);
        }
        AFL_VERIFY(inLast)("portion_id", p->GetPortionId())("stored_level", storedLevel)("produced", produced);
        AFL_VERIFY(!inAcc)("portion_id", p->GetPortionId())("produced", produced);
        AFL_VERIFY(!middleHits)("portion_id", p->GetPortionId())("produced", produced);
    } else {
        if (!(middleHits == 1 && middleKey == storedLevel && !inAcc && !inLast)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "msg", "tiling++: stored_level=middle but physical placement disagrees")(
                "on_remove", onRemove)("portion_id", p->GetPortionId())("produced", produced)(
                "stored_level", (ui32)storedLevel)("physical_middle", middleKey)(
                "in_accumulator", inAcc)("in_last", inLast)("middle_hits", middleHits);
        }
        AFL_VERIFY(middleHits == 1)("portion_id", p->GetPortionId())("stored_level", storedLevel)("produced", produced);
        AFL_VERIFY(middleKey == storedLevel)("portion_id", p->GetPortionId())(
            "stored_level", (ui32)storedLevel)("physical_middle", middleKey)("produced", produced);
        AFL_VERIFY(!inAcc)("portion_id", p->GetPortionId())("produced", produced);
        AFL_VERIFY(!inLast)("portion_id", p->GetPortionId())("produced", produced);
    }
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
        DebugVerifyTilingCoreModifyBatch(add, remove);
        for (const auto& p : remove) {
            const ui64 portionId = p->GetPortionId();
            const auto lit = Core.InternalLevelForDebug.find(portionId);
            if (lit == Core.InternalLevelForDebug.end()) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                    "event", "tiling++_portion_remove")("phase", "before_core_remove")(
                    "problem", "portion_id_missing_from_InternalLevelForDebug")("portion_id", portionId)(
                    "produced", static_cast<ui32>(p->GetProduced()));
                AFL_VERIFY(false)("reason", "Remove unknown portion");
            }
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling++_portion_remove")("phase", "before_core_remove")("portion_id", portionId)(
                "produced", static_cast<ui32>(p->GetProduced()))("level", (ui32)lit->second.Level)(
                "width", lit->second.Width);
            TilingCoreDebugVerifyPortionHome(Core, p, lit->second.Level, lit->second.Width, true);
            Core.RemovePortion(p);
        }
        for (const auto& p : add) {
            const ui64 portionId = p->GetPortionId();
            const auto existingIt = Core.InternalLevelForDebug.find(portionId);
            if (existingIt != Core.InternalLevelForDebug.end()) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                    "event", "tiling++_portion_add")("phase", "before_core_add")(
                    "problem", "portion_already_exists_in_optimizer")("portion_id", portionId)(
                    "produced", static_cast<ui32>(p->GetProduced()))(
                    "existing_level", (ui32)existingIt->second.Level)("existing_width", existingIt->second.Width)(
                    "blob_bytes", p->GetTotalBlobBytes())(
                    "in_accumulator", Core.Accumulator.HasPortion(p))(
                    "in_last", Core.LastLevel.HasPortion(p));
            }
            Core.AddPortion(p);
            const auto& placed = Core.InternalLevelForDebug.at(p->GetPortionId());
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling++_portion_add")("phase", "after_core_add")("portion_id", p->GetPortionId())(
                "produced", static_cast<ui32>(p->GetProduced()))("level", (ui32)placed.Level)("width", placed.Width)(
                "blob_bytes", p->GetTotalBlobBytes());
            TilingCoreDebugVerifyPortionHome(Core, p, placed.Level, placed.Width, false);
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
        const auto& task = tasks.front();

        auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, task.Portions, TSaverContext(StoragesManager));
        result->SetTargetCompactionLevel(task.TargetLevel);
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
        return std::make_shared<TOptimizerPlannerCoreTiling>(
            context.GetPathId(), context.GetStorages(), context.GetPKSchema(), Settings);
    }
};

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
