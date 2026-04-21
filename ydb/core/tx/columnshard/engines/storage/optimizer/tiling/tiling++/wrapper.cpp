#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
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
        Y_UNUSED(context);
        return TConclusionStatus::Fail("tiling++ planner constructor is not implemented");
    }

public:
    static TString GetClassNameStatic() {
        return "TILING";
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
