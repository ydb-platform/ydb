#pragma once

#include <ydb/library/conclusion/status.h>

#include <library/cpp/json/writer/json_value.h>
#include <util/datetime/base.h>
#include <util/system/types.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSnapshotTiling {

/// Parameters of the leveled interval planner (design.md, "Parameters").
struct TSnapshotTilingSettings {
    /// Portions below this blob size go to the accumulator.
    ui64 AccumulatorPortionSizeLimit = 512ULL * 1024;
    /// Width threshold T: wider than T against the level below stays on the current level.
    ui8 WidthThreshold = 10;
    /// Interval-tree levels, numbered from 2 up to 1 + TreeLevelsCount.
    ui8 TreeLevelsCount = 10;
    /// Height at which a tree level produces a task (inclusive).
    ui64 TriggerHeight = 10;
    /// Height at which a tree level reports overload (inclusive).
    ui64 OverloadHeight = 15;
    /// Accumulator task trigger and overload, in portions.
    ui64 AccumulatorTriggerPortions = 1'000;
    ui64 AccumulatorOverloadPortions = 10'000;
    /// Budget of each task kind, as a cap (revised 2026-11-07 at the author's request: one budget per kind, raw bytes,
    /// 256 MB). Bytes are raw, uncompressed: what a task holds in memory and what the compaction memory limiter charges;
    /// the small-portion rule above stays in blob bytes, the on-disk size. The two-input minimum may exceed the bytes.
    /// The accumulator takes ten times the portions, its inputs being small.
    ui64 AccumulatorTaskBytes = 256ULL * 1024 * 1024;
    ui64 LevelTaskBytes = 256ULL * 1024 * 1024;
    ui64 SnapshotTaskBytes = 256ULL * 1024 * 1024;
    ui64 AccumulatorTaskPortions = 10'000;
    ui64 LevelTaskPortions = 1'000;
    ui64 SnapshotTaskPortions = 1'000;
    /// How far the dry sweep behind an advance's IdealBytes goes, in snapshot byte budgets (2026-11-08): the sweep is
    /// diagnostic and would otherwise walk every portion the window chains to, a whole table on a log load; beyond the cap
    /// the ideal reads as "at least" and the task is counted as capped.
    ui32 IdealSweepBudgets = 8;
    /// Expected blob size of an output portion; every byte budget must hold at least MinTaskPortionsFactor of them, a
    /// floor rather than the guarantee of decision 15 now that the budgets are raw bytes: raw is never below blob.
    ui64 PortionExpectedSize = 4ULL * 1024 * 1024;
    ui32 MinTaskPortionsFactor = 16;
    /// Settle time L: the advance ignores younger portions, a forced request waits until R is this old.
    TDuration SettleTime = TDuration::Seconds(10);
    /// Lag of the compacted snapshot S behind the clock from which the advance is a prioritized source, level 1, and the
    /// lag at which it is critical and the planner reports overload, like a level between its trigger and overload heights
    /// (decision 18, revised 2026-10-21).
    TDuration DelayTrigger = TDuration::Minutes(15);
    TDuration DelayOverload = TDuration::Hours(1);
    ui32 CompactionThreads = 2;
    /// Node gap rule for height and accumulator tasks; the advance is exempt.
    ui32 MaxPriorityGap = 3;

    ui8 TopLevel() const {
        return static_cast<ui8>(1 + TreeLevelsCount);
    }

    TConclusionStatus Validate() const;

    /// The keys are the field names in snake case; durations end in _seconds: settle_time_seconds, delay_trigger_seconds,
    /// delay_overload_seconds; task_bytes and task_portions set the budget of all three task kinds at once.
    NJson::TJsonValue SerializeToJson() const;

    /// Strict: an unknown key is an error, except the two keys of the planner container itself, node_portions_count_limit
    /// and weight_kff, which the container reads before it hands the same object down (optimizer.h). Ends with Validate,
    /// so that a configuration that breaks decision 15 is rejected when the settings are evaluated.
    TConclusionStatus DeserializeFromJson(const NJson::TJsonValue& json);
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NSnapshotTiling
