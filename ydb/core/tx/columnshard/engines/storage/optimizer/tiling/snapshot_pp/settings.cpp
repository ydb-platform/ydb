#include "settings.h"

#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>

#include <util/string/builder.h>

#include <type_traits>

namespace NKikimr::NOlap::NStorageOptimizer::NSnapshotTiling {

TConclusionStatus TSnapshotTilingSettings::Validate() const {
    if (WidthThreshold < 1) {
        return TConclusionStatus::Fail("tiling-s: width_threshold must be at least 1");
    }
    if (TreeLevelsCount < 1 || TreeLevelsCount > NTiling::TILING_LAYERS_COUNT) {
        return TConclusionStatus::Fail(TStringBuilder() << "tiling-s: tree_levels_count must be in [1, " << NTiling::TILING_LAYERS_COUNT << "]");
    }
    if (TriggerHeight < 2 || OverloadHeight <= TriggerHeight) {
        return TConclusionStatus::Fail("tiling-s: trigger_height must be at least 2 and below overload_height");
    }
    if (AccumulatorTriggerPortions < 2 || AccumulatorOverloadPortions <= AccumulatorTriggerPortions) {
        return TConclusionStatus::Fail("tiling-s: accumulator_trigger_portions must be at least 2 and below accumulator_overload_portions");
    }
    if (TaskPortions < 2) {
        return TConclusionStatus::Fail("tiling-s: task_portions must be at least 2");
    }
    if (PortionExpectedSize == 0) {
        return TConclusionStatus::Fail("tiling-s: portion_expected_size must be positive");
    }
    if (AccumulatorPortionSizeLimit > PortionExpectedSize) {
        return TConclusionStatus::Fail("tiling-s: accumulator_portion_size_limit must not exceed portion_expected_size");
    }
    if (TaskBytes < static_cast<ui64>(MinTaskPortionsFactor) * PortionExpectedSize) {
        return TConclusionStatus::Fail(TStringBuilder() << "tiling-s: task_bytes (" << TaskBytes << ") must hold at least "
                                                        << MinTaskPortionsFactor << " expected portions of " << PortionExpectedSize
                                                        << " bytes; raise task_bytes or lower portion_expected_size");
    }
    if (SettleTime == TDuration::Zero()) {
        return TConclusionStatus::Fail("tiling-s: settle_time must be positive");
    }
    if (DelayTrigger == TDuration::Zero() || DelayOverload <= DelayTrigger) {
        return TConclusionStatus::Fail("tiling-s: delay_trigger_seconds must be positive and below delay_overload_seconds");
    }
    return TConclusionStatus::Success();
}

NJson::TJsonValue TSnapshotTilingSettings::SerializeToJson() const {
    NJson::TJsonValue json(NJson::JSON_MAP);
    json["accumulator_portion_size_limit"] = AccumulatorPortionSizeLimit;
    json["width_threshold"] = static_cast<ui64>(WidthThreshold);
    json["tree_levels_count"] = static_cast<ui64>(TreeLevelsCount);
    json["trigger_height"] = TriggerHeight;
    json["overload_height"] = OverloadHeight;
    json["accumulator_trigger_portions"] = AccumulatorTriggerPortions;
    json["accumulator_overload_portions"] = AccumulatorOverloadPortions;
    json["task_bytes"] = TaskBytes;
    json["task_portions"] = TaskPortions;
    json["portion_expected_size"] = PortionExpectedSize;
    json["min_task_portions_factor"] = static_cast<ui64>(MinTaskPortionsFactor);
    json["settle_time_seconds"] = SettleTime.Seconds();
    json["delay_trigger_seconds"] = DelayTrigger.Seconds();
    json["delay_overload_seconds"] = DelayOverload.Seconds();
    json["compaction_threads"] = static_cast<ui64>(CompactionThreads);
    json["max_priority_gap"] = static_cast<ui64>(MaxPriorityGap);
    return json;
}

TConclusionStatus TSnapshotTilingSettings::DeserializeFromJson(const NJson::TJsonValue& json) {
    if (!json.IsMap()) {
        return TConclusionStatus::Fail("tiling-s: FEATURES must be a JSON object");
    }
    const auto readUnsigned = [](const TString& name, const NJson::TJsonValue& value, const ui64 max, auto& field) {
        if (!value.IsUInteger()) {
            return TConclusionStatus::Fail(TStringBuilder() << "tiling-s: " << name << " must be an unsigned integer");
        }
        const ui64 parsed = value.GetUInteger();
        if (parsed > max) {
            return TConclusionStatus::Fail(TStringBuilder() << "tiling-s: " << name << " must not exceed " << max);
        }
        field = static_cast<std::remove_reference_t<decltype(field)>>(parsed);
        return TConclusionStatus::Success();
    };
    for (const auto& [name, value] : json.GetMapSafe()) {
        TConclusionStatus status = TConclusionStatus::Success();
        if (name == "accumulator_portion_size_limit") {
            status = readUnsigned(name, value, Max<ui64>(), AccumulatorPortionSizeLimit);
        } else if (name == "width_threshold") {
            status = readUnsigned(name, value, Max<ui8>(), WidthThreshold);
        } else if (name == "tree_levels_count") {
            status = readUnsigned(name, value, Max<ui8>(), TreeLevelsCount);
        } else if (name == "trigger_height") {
            status = readUnsigned(name, value, Max<ui64>(), TriggerHeight);
        } else if (name == "overload_height") {
            status = readUnsigned(name, value, Max<ui64>(), OverloadHeight);
        } else if (name == "accumulator_trigger_portions") {
            status = readUnsigned(name, value, Max<ui64>(), AccumulatorTriggerPortions);
        } else if (name == "accumulator_overload_portions") {
            status = readUnsigned(name, value, Max<ui64>(), AccumulatorOverloadPortions);
        } else if (name == "task_bytes") {
            status = readUnsigned(name, value, Max<ui64>(), TaskBytes);
        } else if (name == "task_portions") {
            status = readUnsigned(name, value, Max<ui64>(), TaskPortions);
        } else if (name == "portion_expected_size") {
            status = readUnsigned(name, value, Max<ui64>(), PortionExpectedSize);
        } else if (name == "min_task_portions_factor") {
            status = readUnsigned(name, value, Max<ui32>(), MinTaskPortionsFactor);
        } else if (name == "settle_time_seconds") {
            ui64 seconds = 0;
            status = readUnsigned(name, value, Max<ui64>() / 1'000'000, seconds);
            if (status.IsSuccess()) {
                SettleTime = TDuration::Seconds(seconds);
            }
        } else if (name == "delay_trigger_seconds") {
            ui64 seconds = 0;
            status = readUnsigned(name, value, Max<ui64>() / 1'000'000, seconds);
            if (status.IsSuccess()) {
                DelayTrigger = TDuration::Seconds(seconds);
            }
        } else if (name == "delay_overload_seconds") {
            ui64 seconds = 0;
            status = readUnsigned(name, value, Max<ui64>() / 1'000'000, seconds);
            if (status.IsSuccess()) {
                DelayOverload = TDuration::Seconds(seconds);
            }
        } else if (name == "compaction_threads") {
            status = readUnsigned(name, value, Max<ui32>(), CompactionThreads);
        } else if (name == "max_priority_gap") {
            status = readUnsigned(name, value, Max<ui32>(), MaxPriorityGap);
        } else if (name == "node_portions_count_limit" || name == "weight_kff") {
            // The planner container's own keys (IOptimizerPlannerConstructor::DeserializeFromJson).
        } else {
            return TConclusionStatus::Fail(TStringBuilder() << "tiling-s: unknown setting " << name);
        }
        if (status.IsFail()) {
            return status;
        }
    }
    return Validate();
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NSnapshotTiling
