#include "settings.h"

#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSnapshotTiling {

TConclusionStatus TSnapshotTilingSettings::Validate() const {
    if (WidthThreshold < 1) {
        return TConclusionStatus::Fail("tiling-s: width_threshold must be at least 1");
    }
    if (TreeLevelsCount < 1 || TreeLevelsCount > NTiling::TILING_LAYERS_COUNT) {
        return TConclusionStatus::Fail(
            TStringBuilder() << "tiling-s: tree_levels_count must be in [1, " << NTiling::TILING_LAYERS_COUNT << "]");
    }
    if (TriggerHeight < 2 || OverloadHeight <= TriggerHeight) {
        return TConclusionStatus::Fail("tiling-s: trigger_height must be at least 2 and below overload_height");
    }
    if (AccumulatorTriggerPortions < 2 || AccumulatorOverloadPortions <= AccumulatorTriggerPortions) {
        return TConclusionStatus::Fail(
            "tiling-s: accumulator_trigger_portions must be at least 2 and below accumulator_overload_portions");
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
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NSnapshotTiling
