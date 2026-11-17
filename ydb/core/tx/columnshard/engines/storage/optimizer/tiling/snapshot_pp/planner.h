#pragma once

#include "settings.h"

#include <ydb/core/tx/columnshard/common/portion.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/library/intersection_tree/intersection_tree.h>

#include <library/cpp/json/writer/json_value.h>
#include <util/generic/function_ref.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/set.h>
#include <util/string/builder.h>

#include <algorithm>
#include <bitset>
#include <concepts>
#include <iterator>
#include <optional>
#include <tuple>
#include <vector>

namespace NKikimr::NOlap::NStorageOptimizer::NSnapshotTiling {

/// Level numbering as in tiling++: 0 is the accumulator, 1 is the line, 2 and up are interval-tree levels.
constexpr ui8 ACCUMULATOR_LEVEL = 0;
constexpr ui8 LINE_LEVEL = 1;
constexpr ui8 FIRST_TREE_LEVEL = 2;

/// Order by (end key, id); heterogeneous lookup by a key finds the first portion ending at or after it.
template <std::totally_ordered TKey, typename TPortion>
struct TByEndKey {
    using is_transparent = void;

    bool operator()(const typename TPortion::TConstPtr& left, const typename TPortion::TConstPtr& right) const {
        return std::tuple(left->IndexKeyEnd(), left->GetPortionId()) < std::tuple(right->IndexKeyEnd(), right->GetPortionId());
    }

    bool operator()(const typename TPortion::TConstPtr& left, const TKey& right) const {
        return left->IndexKeyEnd() < right;
    }

    bool operator()(const TKey& left, const typename TPortion::TConstPtr& right) const {
        return left < right->IndexKeyEnd();
    }
};

/// Order by (start key, end key, id): the sweep order of an advance window among the portions starting inside it
/// (2026-11-12); heterogeneous lookup by a key finds the first portion starting at or after it.
template <std::totally_ordered TKey, typename TPortion>
struct TByStartKey {
    using is_transparent = void;

    bool operator()(const typename TPortion::TConstPtr& left, const typename TPortion::TConstPtr& right) const {
        const auto leftStart = left->IndexKeyStart();
        const auto rightStart = right->IndexKeyStart();
        if (leftStart < rightStart) {
            return true;
        }
        if (rightStart < leftStart) {
            return false;
        }
        const auto leftEnd = left->IndexKeyEnd();
        const auto rightEnd = right->IndexKeyEnd();
        if (leftEnd < rightEnd) {
            return true;
        }
        if (rightEnd < leftEnd) {
            return false;
        }
        return left->GetPortionId() < right->GetPortionId();
    }

    bool operator()(const typename TPortion::TConstPtr& left, const TKey& right) const {
        return left->IndexKeyStart() < right;
    }

    bool operator()(const TKey& left, const typename TPortion::TConstPtr& right) const {
        return left < right->IndexKeyStart();
    }
};

/// Order of the set behind S (decision 3): (RecordSnapshotMin, end key, id). The snapshots decide nearly every comparison,
/// so the end keys, which the engine's accessors build as rows on every call, are read only when they tie (2026-11-12).
template <typename TPortion>
struct TBySnapshot {
    template <typename TLeft, typename TRight>
    bool operator()(const TLeft& left, const TRight& right) const {
        const auto leftSnapshot = left->RecordSnapshotMin();
        const auto rightSnapshot = right->RecordSnapshotMin();
        if (leftSnapshot < rightSnapshot) {
            return true;
        }
        if (rightSnapshot < leftSnapshot) {
            return false;
        }
        const auto leftEnd = left->IndexKeyEnd();
        const auto rightEnd = right->IndexKeyEnd();
        if (leftEnd < rightEnd) {
            return true;
        }
        if (rightEnd < leftEnd) {
            return false;
        }
        return left->GetPortionId() < right->GetPortionId();
    }
};

/// The clock as the adapter supplies it, the node's time as a plan step (decision 6, revised 2026-10-16): SettledBefore
/// is Now minus the settle time L; the adapter computes it, because only the adapter knows the snapshot arithmetic.
template <std::totally_ordered TSnapshot>
struct TClock {
    /// The engine's snapshot has no default constructor but a Zero(); the tests use plain integers. A zero clock settles
    /// nothing, so no advance target exists until the adapter has set the clock.
    static TSnapshot Origin() {
        if constexpr (requires { TSnapshot::Zero(); }) {
            return TSnapshot::Zero();
        } else {
            return TSnapshot{};
        }
    }

    TSnapshot Now = Origin();
    TSnapshot SettledBefore = Origin();
};

enum class ETaskKind: ui8 {
    /// Read the inputs, merge them, write new portions.
    Merge,
    /// Change the inputs' level to the line without rewriting them (decision 26).
    MetadataMove,
};

enum class ETaskReason: ui8 {
    Accumulator,
    Level,
    AdvanceMerge,
    AdvancePrefix,
    AdvanceMove,
};

/// Why the composition of a task stopped taking portions (2026-11-07): nothing more to take, the portion cap, the byte
/// cap, or, for an advance, a line portion another task holds. Counted per task kind under TilingCompactionOptimizer.
enum class EStopCause: ui8 {
    Exhausted,
    PortionsLimit,
    BytesLimit,
    Held,
};

/// What the core hands back; the adapter turns it into engine changes.
template <std::totally_ordered TKey, typename TPortion>
struct TTask {
    /// A cut of the merged output at Key; the key itself goes to the slice on its left or on its right.
    struct TCheckPoint {
        TKey Key;
        bool KeyBelongsToLeft = true;

        bool operator<(const TCheckPoint& other) const {
            return std::tie(Key, KeyBelongsToLeft) < std::tie(other.Key, other.KeyBelongsToLeft);
        }

        bool operator==(const TCheckPoint& other) const {
            return !(*this < other) && !(other < *this);
        }
    };

    std::vector<typename TPortion::TConstPtr> Portions;
    /// Cuts of the merged output, sorted; a slice between two cuts is placed on its own by the arrival rule.
    std::vector<TCheckPoint> CheckPoints;
    ETaskKind Kind = ETaskKind::Merge;
    ETaskReason Reason = ETaskReason::Level;
    /// The unit the task was built from: 0 the accumulator, 2 and up a tree level, 1 the line for advance moves.
    ui8 SourceLevel = 0;
    /// Persisted as the outputs' CompactionLevel; only 1 has a meaning for placement.
    ui8 TargetLevel = 0;
    TOptimizationPriority Priority = TOptimizationPriority::Zero();
    /// Raw bytes of the inputs (2026-11-07; blob bytes before): the size the task holds in memory.
    ui64 Bytes = 0;
    /// For an advance merge, the raw bytes the task would have had without the budget: the window swept on past the cut
    /// (2026-10-26); equal to Bytes when the window was complete, zero for other kinds and reasons.
    ui64 IdealBytes = 0;
    /// The dry sweep behind IdealBytes stopped at its cap with candidates left (2026-11-08): IdealBytes is a lower bound.
    bool IdealCapped = false;
    EStopCause StopCause = EStopCause::Exhausted;
};

/// The leveled interval planner of design.md: one class owning the accumulator, the tree levels and the line.
/// TPortion must provide GetPortionId, IndexKeyStart, IndexKeyEnd, GetTotalBlobBytes, GetTotalRawBytes, GetCompactionLevel,
/// GetProduced and RecordSnapshotMin; the unit tests use a plain struct, the engine uses TPortionInfo.
template <std::totally_ordered TKey, typename TPortion, std::totally_ordered TSnapshot>
class TSnapshotTiling {
public:
    using TPtr = typename TPortion::TPtr;
    using TConstPtr = typename TPortion::TConstPtr;
    using TSettings = TSnapshotTilingSettings;
    using TClock = NSnapshotTiling::TClock<TSnapshot>;
    using TEndSorted = TSet<TConstPtr, TByEndKey<TKey, TPortion>>;
    using TStartSorted = TSet<TConstPtr, TByStartKey<TKey, TPortion>>;
    using TSnapshotSorted = TSet<TConstPtr, TBySnapshot<TPortion>>;
    using TTask = NSnapshotTiling::TTask<TKey, TPortion>;
    using TIsLocked = TFunctionRef<bool(const TConstPtr&)>;

    struct TEntry {
        TPtr Portion;
        ui8 Level = ACCUMULATOR_LEVEL;
        /// Input of a running task (decision 14): out of its level's structures, still in the registry;
        /// the line keeps it visible so that nothing is placed over it.
        bool HandedOut = false;
    };

    struct TTreeLevel {
        /// Unlocked portions by (end key, id): the width walk (decision 10).
        TEndSorted Portions;
        /// The same by (start key, end key, id): the advance window reads the portions starting inside it in sweep order
        /// (2026-11-12).
        TStartSorted Starts;
        /// Unlocked portions: height, overload and stack queries (decision 14).
        TIntersectionTree<TKey, ui64> Tree;
    };

    /// The units a batch of changes touched; their counters are refreshed once, at the end of the batch (2026-11-12).
    using TTouchedLevels = std::bitset<256>;

    TSnapshotTiling(TSettings settings, const NTiling::TCounters& counters)
        : Settings(std::move(settings))
        , Counters(counters)
        , TreeLevels(static_cast<size_t>(Settings.TopLevel()) + 1)
    {
        const auto status = Settings.Validate();
        AFL_VERIFY(status.IsSuccess())("error", status.GetErrorMessage());
    }

    const TSettings& GetSettings() const {
        return Settings;
    }

    /// Engine entry point: one batch of removals and additions. The first batch is the bootstrap load (decisions 11 to 13).
    void ModifyPortions(const std::vector<TPtr>& add, const std::vector<TConstPtr>& remove) {
        UnstallAdvance();
        TTouchedLevels touched;
        for (const auto& p : remove) {
            RemovePortion(p, touched);
        }
        if (FirstLoad) {
            FirstLoad = false;
            InitialAddPortions(add, touched);
        } else {
            for (const auto& p : add) {
                AddPortion(p, touched);
            }
        }
        RefreshCounters(touched);
    }

    /// The clock moves the delay source (decision 18), so the node's buckets are refreshed here as well as on portion changes.
    void UpdateClock(const TClock& clock) {
        Clock = clock;
        PublishHighestLevel();
    }

    /// Inputs of a task that is no longer running and that the engine has not removed were aborted: restore them. Runs at
    /// the start of every task request, where the engine passes its locks manager; the scheduling pass reads priorities
    /// without lock information, so the remembered priority of the handed-out tasks keeps such a table in that pass
    /// until the next request (decision 14).
    void Resync(TIsLocked isLocked) {
        std::vector<ui64> restore;
        for (const auto& [id, p] : HandedOut) {
            if (!isLocked(p)) {
                restore.push_back(id);
            }
        }
        if (restore.empty()) {
            return;
        }
        TTouchedLevels touched;
        for (const ui64 id : restore) {
            Restore(id, touched);
        }
        RefreshCounters(touched);
        UnstallAdvance();
        ResetAdvanceScan();
    }

    /// The next task from the accumulator, a tree level or, once S lags the clock by the delay trigger, the advance as a
    /// prioritized source (decision 18, revised 2026-10-21); handed out at once: its inputs leave the unlocked structures
    /// and come back only through Resync, which runs first (decision 14). Nothing at or above a trigger means no task;
    /// the idle advance is a separate entry point.
    std::optional<TTask> TakeNextTask(TIsLocked isLocked) {
        const TLockMemo memo(isLocked);
        const TIsLocked cached(memo);
        Resync(cached);

        // The sources at their triggers in priority order, composed one at a time until one succeeds (2026-11-12): before,
        // every source was composed and the best picked, so a full accumulator task was built on every request only to lose
        // to a level or to the gap rule. Equal priorities keep the old order: the tree levels from the bottom, then the
        // accumulator, then the advance. A level or the accumulator below the node's gap is not composed; the advance below
        // the gap still is, so that a blocked one stalls as before (decision 18), and is discarded afterwards.
        struct TSource {
            TOptimizationPriority Priority;
            /// A tree level, ACCUMULATOR_LEVEL, or LINE_LEVEL for the advance.
            ui8 Level = 0;
        };

        std::vector<TSource> sources;
        for (ui8 level = FIRST_TREE_LEVEL; level <= Settings.TopLevel(); ++level) {
            if (const auto priority = LevelPriority(level)) {
                sources.push_back(TSource{ .Priority = *priority, .Level = level });
            }
        }
        if (const auto priority = AccumulatorPriority()) {
            sources.push_back(TSource{ .Priority = *priority, .Level = ACCUMULATOR_LEVEL });
        }
        if (const auto priority = DelayPriority()) {
            sources.push_back(TSource{ .Priority = *priority, .Level = LINE_LEVEL });
        }
        std::stable_sort(sources.begin(), sources.end(), [](const TSource& left, const TSource& right) {
            return right.Priority < left.Priority;
        });
        const i64 maxOverloadLevel = Counters.GetTilingCounters().Portions->GetMaxOverloadLevel();
        const auto belowGap = [&](const TOptimizationPriority& priority) {
            return priority.GetLevel() + static_cast<i64>(Settings.MaxPriorityGap) < maxOverloadLevel;
        };
        bool heldBack = false;
        for (const TSource& source : sources) {
            std::optional<TTask> task;
            TConstPtr advanceTarget;
            if (source.Level == LINE_LEVEL) {
                if (auto advance = BuildAdvanceTask(cached)) {
                    task = std::move(advance->first);
                    task->Priority = source.Priority;
                    advanceTarget = advance->second;
                } else {
                    StallAdvance();
                }
            } else if (belowGap(source.Priority)) {
                heldBack = true;
                continue;
            } else if (source.Level == ACCUMULATOR_LEVEL) {
                task = BuildAccumulatorTask(cached);
            } else {
                task = BuildLevelTask(source.Level, cached);
            }
            if (!task) {
                continue;
            }
            if (belowGap(task->Priority)) {
                heldBack = true;
                continue;
            }
            Counters.GeneratedTasksPriorityLevel->Collect(task->Priority.GetLevel());
            if (advanceTarget) {
                RaisePlacementThreshold(advanceTarget->RecordSnapshotMin());
                RecordAdvance(*task);
            }
            RecordStop(*task);
            HandOut(*task);
            return task;
        }
        if (heldBack) {
            Counters.TasksSkippedByPriorityGap->Inc();
        }
        return std::nullopt;
    }

    /// The advance regardless of the delay trigger (decisions 19 to 21, 26): the first portion of the set that is at least
    /// L old, not held by a task and unlocked, composed into one task and handed out at once. The adapter takes the advance
    /// through TakeNextTask once S lags the clock by the trigger (decision 18); this entry point serves the tests and, in
    /// PR 2, a forced request. A target whose mandatory input is held by another task is skipped and retried later.
    std::optional<TTask> TakeAdvanceTask(TIsLocked isLocked) {
        const TLockMemo memo(isLocked);
        const TIsLocked cached(memo);
        Resync(cached);
        auto advance = BuildAdvanceTask(cached);
        if (!advance) {
            StallAdvance();
            return std::nullopt;
        }
        RaisePlacementThreshold(advance->second->RecordSnapshotMin());
        RecordAdvance(advance->first);
        RecordStop(advance->first);
        HandOut(advance->first);
        return std::move(advance->first);
    }

    /// The memory shortage of the advance (2026-10-26): what an uncut window would have cost, as a histogram over every
    /// advance merge, with the shortfall against the budget and the number of cut windows as counters.
    void RecordAdvance(const TTask& task) const {
        if (task.Kind != ETaskKind::Merge) {
            return;
        }
        Counters.AdvanceIdealTaskBytes->Collect(static_cast<i64>(task.IdealBytes));
        if (task.IdealCapped) {
            Counters.AdvanceIdealCapped->Inc();
        }
        if (task.IdealBytes > task.Bytes) {
            Counters.AdvanceBudgetCuts->Inc();
            if (task.IdealBytes > Settings.SnapshotTaskBytes) {
                Counters.AdvanceMemoryShortageBytes->Add(task.IdealBytes - Settings.SnapshotTaskBytes);
            }
        }
    }

    /// Why the handed-out task stopped taking portions, counted per task kind (2026-11-07).
    void RecordStop(const TTask& task) const {
        const NTiling::TTaskStopCounters& counters = StopCountersFor(task);
        switch (task.StopCause) {
            case EStopCause::Exhausted:
                counters.Exhausted->Inc();
                break;
            case EStopCause::PortionsLimit:
                counters.PortionsLimit->Inc();
                break;
            case EStopCause::BytesLimit:
                counters.BytesLimit->Inc();
                break;
            case EStopCause::Held:
                counters.Held->Inc();
                break;
        }
    }

    const NTiling::TTaskStopCounters& StopCountersFor(const TTask& task) const {
        switch (task.Reason) {
            case ETaskReason::Accumulator:
                return *Counters.AccumulatorStops;
            case ETaskReason::Level:
                return *Counters.LevelStops;
            case ETaskReason::AdvanceMerge:
            case ETaskReason::AdvancePrefix:
            case ETaskReason::AdvanceMove:
                return *Counters.SnapshotStops;
        }
        return *Counters.SnapshotStops;
    }

    /// Whether the advance has a settled target at all, whatever the lag of S.
    bool HasAdvanceTarget() const {
        return HasAdvanceTarget(Clock.SettledBefore);
    }

    /// The same against a clock the caller supplies: the priority read is const and must not touch the core's clock.
    bool HasAdvanceTarget(const TSnapshot& settledBefore) const {
        return !IsLoneSmallTable() && !OutsideUnlocked.empty() && !(settledBefore < (*OutsideUnlocked.begin())->RecordSnapshotMin());
    }

    /// Decision 18, completed 2026-10-28: a delay source that was asked for a task and could compose none, every settled
    /// target blocked by held inputs, stops counting as a source and as overload until the set or the locks change;
    /// otherwise the lag it cannot act on would keep the node's critical bucket raised and the gap rule would hold back
    /// every other shard on the node. The gauges keep showing the real lag.
    void StallAdvance() {
        if (!AdvanceStalled) {
            AdvanceStalled = true;
            PublishHighestLevel();
        }
    }

    void UnstallAdvance() {
        if (AdvanceStalled) {
            AdvanceStalled = false;
            PublishHighestLevel();
        }
    }

    bool IsAdvanceStalled() const {
        return AdvanceStalled;
    }

    /// The highest priority among the sources at or above their triggers, the lag of S included; nothing when none is
    /// (decisions 16 and 18).
    std::optional<TOptimizationPriority> GetTaskPriority() const {
        std::optional<TOptimizationPriority> best;
        const auto consider = [&best](const std::optional<TOptimizationPriority>& candidate) {
            if (candidate && (!best || *best < *candidate)) {
                best = candidate;
            }
        };
        for (ui8 level = FIRST_TREE_LEVEL; level <= Settings.TopLevel(); ++level) {
            consider(LevelPriority(level));
        }
        consider(AccumulatorPriority());
        consider(DelayPriority());
        return best;
    }

    /// The priority the shard schedules by (decision 14): the highest unit priority, or, while the planner holds
    /// handed-out portions, at least the priority remembered from the task that took them, so that the pass after an
    /// abort still asks the queue and the next task request can restore the inputs. The lag of S is one of the sources
    /// (decision 18).
    std::optional<TOptimizationPriority> GetSchedulingPriority() const {
        auto best = GetTaskPriority();
        if (PendingPriority && (!best || *best < *PendingPriority)) {
            best = PendingPriority;
        }
        return best;
    }

    /// Overload on the unlocked view (decisions 14 and 25): a level at its edge with no task running for its portions, the
    /// accumulator at its edge, or S lagging the clock by the delay overload (decision 18).
    bool IsOverloaded() const {
        if (Accumulator.size() >= Settings.AccumulatorOverloadPortions) {
            return true;
        }
        if (!AdvanceStalled && GetDelaySeconds() >= Settings.DelayOverload.Seconds()) {
            return true;
        }
        for (ui8 level = FIRST_TREE_LEVEL; level <= Settings.TopLevel(); ++level) {
            if (GetHeight(level) >= Settings.OverloadHeight) {
                return true;
            }
        }
        return false;
    }

    ui64 GetHandedOutSize() const {
        return HandedOut.size();
    }

    const TClock& GetClock() const {
        return Clock;
    }

    void AddPortion(const TPtr& p) {
        TTouchedLevels touched;
        AddPortion(p, touched);
        RefreshCounters(touched);
    }

    void RemovePortion(const TConstPtr& p) {
        TTouchedLevels touched;
        RemovePortion(p, touched);
        RefreshCounters(touched);
    }

    /// S for forced requests (decision 1): the oldest portion outside the line, in-flight inputs included; nothing when
    /// there is nothing to count, and the reader supplies the fallback, the shard its outdated step minus L (decision 6).
    std::optional<TSnapshot> GetForcedCompactedSnapshot() const {
        if (IsSettledForForcedRequest()) {
            return std::nullopt;
        }
        return (*Outside.begin())->RecordSnapshotMin();
    }

    /// Decision 21, completed 2026-10-28: a table whose only portion is small has nothing to merge it with. It is settled
    /// for both readings of S: a forced request gets NOT_NEEDED and no delay accrues. Otherwise its lag grows without
    /// bound, marks the shard overloaded and, through the node's buckets and the gap rule, holds back every other shard
    /// on the node, which is what the tiny TPC-H tables did for five hours on 2026-10-27.
    bool IsLoneSmallTable() const {
        return Line.empty() && Registry.size() == 1 && IsSmall(*Registry.begin()->second.Portion);
    }

    /// The completion rule of PR 1 for a forced request: nothing outside the line, or a lone small table.
    bool IsSettledForForcedRequest() const {
        return Outside.empty() || IsLoneSmallTable();
    }

    /// S for the queue key and the age buckets (decision 6): the oldest portion outside the line that no task holds;
    /// nothing when there is none, and the adapter supplies the node's clock minus L as the fallback.
    std::optional<TSnapshot> GetSchedulingCompactedSnapshot() const {
        if (OutsideUnlocked.empty() || IsLoneSmallTable()) {
            return std::nullopt;
        }
        return (*OutsideUnlocked.begin())->RecordSnapshotMin();
    }

    /// The lag of the scheduling reading of S behind the clock, in seconds (decision 18): zero when nothing unlocked is
    /// outside the line, so a running task's inputs count for nothing, as for the levels.
    ui64 GetDelaySeconds() const {
        const auto s = GetSchedulingCompactedSnapshot();
        if (!s || !(*s < Clock.Now)) {
            return 0;
        }
        return SnapshotDistanceSeconds(*s, Clock.Now);
    }

    /// Y (decision 1): empty means zero, the sentinel of an empty bootstrap.
    const std::optional<TSnapshot>& GetPlacementThreshold() const {
        return Y;
    }

    /// Called with the RecordSnapshotMin of every advance target (decision 19).
    void RaisePlacementThreshold(const TSnapshot& snapshot) {
        if (!Y || *Y < snapshot) {
            Y = snapshot;
        }
    }

    std::optional<ui8> GetLevelOf(const ui64 portionId) const {
        const auto it = Registry.find(portionId);
        if (it == Registry.end()) {
            return std::nullopt;
        }
        return it->second.Level;
    }

    ui64 GetRegistrySize() const {
        return Registry.size();
    }

    ui64 GetLineSize() const {
        return Line.size();
    }

    ui64 GetAccumulatorSize() const {
        return Accumulator.size();
    }

    ui64 GetTreeLevelSize(const ui8 level) const {
        return TreeLevelAt(level).Portions.size();
    }

    ui64 GetOutsideSize() const {
        return Outside.size();
    }

    /// Height of a tree level: the largest number of unlocked portions covering one key.
    ui64 GetHeight(const ui8 level) const {
        return static_cast<ui64>(std::max<i32>(0, TreeLevelAt(level).Tree.GetMaxCount()));
    }

    /// Width of a portion against a tree level (decision 10), with the number of examined portions for tests.
    ui64 MeasureWidth(const TPortion& p, const ui8 level, ui64* examined = nullptr) const {
        return Width(p, TreeLevelAt(level), examined);
    }

    /// Structural invariants, checked by the tests after every step and available to debug builds.
    TConclusionStatus CheckInvariants() const {
        ui64 inStructures = Line.size() + Accumulator.size();
        for (ui8 level = FIRST_TREE_LEVEL; level <= Settings.TopLevel(); ++level) {
            inStructures += TreeLevelAt(level).Portions.size();
        }
        ui64 handedOutOffStructures = 0;
        ui64 outsideExpected = 0;
        ui64 outsideUnlockedExpected = 0;
        for (const auto& [id, entry] : Registry) {
            if (!entry.Portion || entry.Portion->GetPortionId() != id) {
                return TConclusionStatus::Fail(TStringBuilder() << "registry entry " << id << " holds another portion");
            }
            if (HandedOut.contains(id) != entry.HandedOut) {
                return TConclusionStatus::Fail(TStringBuilder() << "handed-out flag and set disagree for portion " << id);
            }
            const bool expectedInStructure = !entry.HandedOut || entry.Level == LINE_LEVEL;
            if (entry.HandedOut && entry.Level != LINE_LEVEL) {
                ++handedOutOffStructures;
            }
            if (entry.Level != LINE_LEVEL) {
                ++outsideExpected;
                if (!Outside.contains(entry.Portion)) {
                    return TConclusionStatus::Fail(TStringBuilder() << "portion " << id << " is outside the line but not in the S set");
                }
                if (!entry.HandedOut) {
                    ++outsideUnlockedExpected;
                    if (!OutsideUnlocked.contains(entry.Portion)) {
                        return TConclusionStatus::Fail(
                            TStringBuilder() << "portion " << id << " is unlocked outside the line but not in the unlocked S set");
                    }
                } else if (OutsideUnlocked.contains(entry.Portion)) {
                    return TConclusionStatus::Fail(TStringBuilder() << "handed-out portion " << id << " is still in the unlocked S set");
                }
            }
            bool found = false;
            switch (entry.Level) {
                case ACCUMULATOR_LEVEL:
                    found = Accumulator.contains(entry.Portion);
                    break;
                case LINE_LEVEL:
                    found = Line.contains(entry.Portion);
                    break;
                default:
                    if (entry.Level > Settings.TopLevel()) {
                        return TConclusionStatus::Fail(TStringBuilder() << "portion " << id << " has level " << (ui32)entry.Level);
                    }
                    found = TreeLevelAt(entry.Level).Portions.contains(entry.Portion);
                    if (found != TreeLevelAt(entry.Level).Starts.contains(entry.Portion)) {
                        return TConclusionStatus::Fail(
                            TStringBuilder() << "portion " << id << " is in only one of the two sets of level " << (ui32)entry.Level);
                    }
                    break;
            }
            if (found != expectedInStructure) {
                return TConclusionStatus::Fail(TStringBuilder() << "portion " << id << " membership in level " << (ui32)entry.Level << " is "
                                                                << found << ", expected " << expectedInStructure);
            }
        }
        if (inStructures + handedOutOffStructures != Registry.size()) {
            return TConclusionStatus::Fail(TStringBuilder() << "structures hold " << inStructures << " portions plus " << handedOutOffStructures
                                                            << " handed out, registry " << Registry.size());
        }
        if (outsideExpected != Outside.size()) {
            return TConclusionStatus::Fail(TStringBuilder() << "S set holds " << Outside.size() << " portions, expected " << outsideExpected);
        }
        if (outsideUnlockedExpected != OutsideUnlocked.size()) {
            return TConclusionStatus::Fail(
                TStringBuilder() << "unlocked S set holds " << OutsideUnlocked.size() << " portions, expected " << outsideUnlockedExpected);
        }
        if (PendingPriority.has_value() != !HandedOut.empty()) {
            return TConclusionStatus::Fail(TStringBuilder()
                                           << "remembered priority and handed-out set disagree: " << HandedOut.size() << " handed out, priority "
                                           << (PendingPriority ? PendingPriority->DebugString() : "none"));
        }
        const TPortion* previous = nullptr;
        for (const auto& p : Line) {
            if (previous && !(previous->IndexKeyEnd() < p->IndexKeyStart())) {
                return TConclusionStatus::Fail(
                    TStringBuilder() << "line portions " << previous->GetPortionId() << " and " << p->GetPortionId() << " intersect");
            }
            previous = p.get();
        }
        for (ui8 level = FIRST_TREE_LEVEL; level <= Settings.TopLevel(); ++level) {
            const auto& treeLevel = TreeLevelAt(level);
            if (treeLevel.Portions.empty() != (treeLevel.Tree.GetMaxCount() == 0)) {
                return TConclusionStatus::Fail(TStringBuilder() << "level " << (ui32)level << " set and tree disagree on emptiness");
            }
            if (treeLevel.Portions.size() != treeLevel.Starts.size()) {
                return TConclusionStatus::Fail(TStringBuilder() << "level " << (ui32)level << " holds " << treeLevel.Portions.size()
                                                                << " portions by end key and " << treeLevel.Starts.size() << " by start key");
            }
        }
        return TConclusionStatus::Success();
    }

    NJson::TJsonValue SerializeToJsonVisual() const {
        NJson::TJsonValue result(NJson::JSON_MAP);
        result.InsertValue("accumulator", Accumulator.size());
        result.InsertValue("line", Line.size());
        NJson::TJsonValue levels(NJson::JSON_ARRAY);
        for (ui8 level = FIRST_TREE_LEVEL; level <= Settings.TopLevel(); ++level) {
            NJson::TJsonValue item(NJson::JSON_MAP);
            item.InsertValue("level", (ui64)level);
            item.InsertValue("portions", TreeLevelAt(level).Portions.size());
            item.InsertValue("height", (ui64)std::max<i32>(0, TreeLevelAt(level).Tree.GetMaxCount()));
            levels.AppendValue(std::move(item));
        }
        result.InsertValue("levels", std::move(levels));
        result.InsertValue("outside", Outside.size());
        result.InsertValue("handed_out", HandedOut.size());
        result.InsertValue("pending_priority", PendingPriority ? PendingPriority->DebugString() : TString("none"));
        const auto forced = GetForcedCompactedSnapshot();
        const auto scheduling = GetSchedulingCompactedSnapshot();
        result.InsertValue("s_forced", forced ? TString(TStringBuilder() << *forced) : TString("none"));
        result.InsertValue("s_scheduling", scheduling ? TString(TStringBuilder() << *scheduling) : TString("none"));
        result.InsertValue("y", Y ? TString(TStringBuilder() << *Y) : TString("none"));
        return result;
    }

private:
    TSettings Settings;
    const NTiling::TCounters& Counters;

    THashMap<ui64, TEntry> Registry;
    TEndSorted Line;
    TEndSorted Accumulator;
    std::vector<TTreeLevel> TreeLevels;
    /// Every portion outside the line, ordered by (RecordSnapshotMin, end key, id): the forced reading of S (decision 1).
    TSnapshotSorted Outside;
    /// The same without the inputs of running tasks: the scheduling reading of S, the delay and the advance targets
    /// (decision 14). Kept apart so that no reader walks past a handed-out prefix (2026-10-27).
    TSnapshotSorted OutsideUnlocked;
    /// Inputs of running tasks, by id (decision 14).
    THashMap<ui64, TConstPtr> HandedOut;
    /// The highest priority among the tasks whose inputs are still handed out, at least level 0 with weight 1, the floor
    /// an advance task needs since its core priority is zero; empty exactly when nothing is handed out (decision 14).
    std::optional<TOptimizationPriority> PendingPriority;
    std::optional<TSnapshot> Y;
    TClock Clock;
    bool FirstLoad = true;
    /// Decision 18 (2026-10-28): set when the advance was asked and composed nothing, cleared by any portion change or
    /// restored input.
    bool AdvanceStalled = false;
    /// Where the advance's scan of the settled targets resumes (2026-11-12): every target before it, in the order of the
    /// set behind S, was found blocked by the last scan, by a held line portion covering its window's left border, by a
    /// missing neighbour or by a lock, and stays blocked until something that could free it happens: a removal, a restored
    /// input, an addition to the line, or an addition that sorts before the position; each of those drops the position.
    /// The fresh writes that arrive several times a second sort after it and keep it, where before every request scanned
    /// every blocked target again. A lock released by none of those, an aborted eviction, say, is noticed at the next one.
    /// Read and set by the const composition path.
    mutable std::optional<TConstPtr> AdvanceScanFrom;

    static bool IsTracked(const TPortion& p) {
        switch (p.GetProduced()) {
            case NPortion::EVICTED:
                // A portion in a cold tier is outside the planner and outside the completion guarantee.
                return false;
            case NPortion::INACTIVE:
            case NPortion::UNSPECIFIED:
            case NPortion::COMPACTED:
                AFL_VERIFY(false)("reason", "tiling-s_unexpected_portion_type")("type", (ui32)p.GetProduced());
                return false;
            default:
                return true;
        }
    }

    TTreeLevel& TreeLevelAt(const ui8 level) {
        AFL_VERIFY(level >= FIRST_TREE_LEVEL && level <= Settings.TopLevel())("level", (ui32)level);
        return TreeLevels[level];
    }

    const TTreeLevel& TreeLevelAt(const ui8 level) const {
        AFL_VERIFY(level >= FIRST_TREE_LEVEL && level <= Settings.TopLevel())("level", (ui32)level);
        return TreeLevels[level];
    }

    const NTiling::TLevelCounters& LevelCounters(const ui8 level) const {
        if (level == ACCUMULATOR_LEVEL) {
            return Counters.GetAccumulatorCounters(0);
        }
        if (level == LINE_LEVEL) {
            return Counters.GetLastLevelCounters();
        }
        return Counters.GetLevelCounters(level - FIRST_TREE_LEVEL);
    }

    bool IsSmall(const TPortion& p) const {
        return p.GetTotalBlobBytes() < Settings.AccumulatorPortionSizeLimit;
    }

    void AddPortion(const TPtr& p, TTouchedLevels& touched) {
        if (!IsTracked(*p)) {
            return;
        }
        const ui64 id = p->GetPortionId();
        AFL_VERIFY(!Registry.contains(id))("portion_id", id)("reason", "tiling-s_portion_already_exists");
        Place(p, touched);
    }

    void RemovePortion(const TConstPtr& p, TTouchedLevels& touched) {
        if (!IsTracked(*p)) {
            return;
        }
        const ui64 id = p->GetPortionId();
        const auto it = Registry.find(id);
        AFL_VERIFY(it != Registry.end())("portion_id", id)("reason", "tiling-s_remove_unknown_portion");
        Erase(it, touched);
        // The removed portion may be the held line portion, or the locked target, that blocked the advance's scan.
        ResetAdvanceScan();
    }

    void ResetAdvanceScan() const {
        AdvanceScanFrom.reset();
    }

    /// The lock state is fixed for one synchronous request, and the same portion is asked about by the re-sync, the level
    /// stacks, the top-ups and the sweeps: each answer of the engine's locks manager, a walk over its process locks, is
    /// kept for the request (2026-11-12).
    class TLockMemo {
        const TIsLocked IsLocked;
        mutable THashMap<ui64, bool> Answers;

    public:
        explicit TLockMemo(TIsLocked isLocked)
            : IsLocked(isLocked)
        {
        }

        bool operator()(const TConstPtr& p) const {
            const auto [it, inserted] = Answers.try_emplace(p->GetPortionId(), false);
            if (inserted) {
                it->second = IsLocked(p);
            }
            return it->second;
        }
    };

    /// Levels 1 to 10 from the trigger to the overload edge, both inclusive (decision 16); at and above the edge the
    /// unit is critical, level 10, and the raw count stays as the weight so that the heavier backlog ranks first.
    static TOptimizationPriority NormalizePriority(const ui64 trigger, const ui64 overload, const ui64 value) {
        const auto priority = TOptimizationPriority::Normalize(trigger, overload, value);
        return priority.IsCritical() ? TOptimizationPriority::Critical(static_cast<i64>(value)) : priority;
    }

    std::optional<TOptimizationPriority> LevelPriority(const ui8 level) const {
        const ui64 height = GetHeight(level);
        if (height < Settings.TriggerHeight) {
            return std::nullopt;
        }
        return NormalizePriority(Settings.TriggerHeight, Settings.OverloadHeight, height);
    }

    std::optional<TOptimizationPriority> AccumulatorPriority() const {
        if (Accumulator.size() < Settings.AccumulatorTriggerPortions) {
            return std::nullopt;
        }
        return NormalizePriority(Settings.AccumulatorTriggerPortions, Settings.AccumulatorOverloadPortions, Accumulator.size());
    }

    /// Plan steps are milliseconds in the engine's snapshot; the tests use plain integers and read them as seconds.
    static ui64 SnapshotDistanceSeconds(const TSnapshot& from, const TSnapshot& to) {
        if constexpr (requires { from.GetPlanStep(); }) {
            return (to.GetPlanStep() - from.GetPlanStep()) / 1000;
        } else {
            return static_cast<ui64>(to - from);
        }
    }

    /// Decision 18, revised 2026-10-21: from the delay trigger the advance is a prioritized source, level 1 at the trigger
    /// rising linearly to critical at the delay overload edge, like a level between its trigger and overload heights.
    std::optional<TOptimizationPriority> DelayPriority() const {
        if (AdvanceStalled) {
            return std::nullopt;
        }
        const ui64 delay = GetDelaySeconds();
        if (delay < Settings.DelayTrigger.Seconds()) {
            return std::nullopt;
        }
        return NormalizePriority(Settings.DelayTrigger.Seconds(), Settings.DelayOverload.Seconds(), delay);
    }

    struct TBudget {
        ui64 Bytes = 0;
        ui64 Portions = 0;
    };

    /// The budget of the task's kind (2026-11-07): the accumulator, the levels and the snapshot advance each have their own.
    TBudget BudgetFor(const TTask& task) const {
        switch (task.Reason) {
            case ETaskReason::Accumulator:
                return TBudget{ .Bytes = Settings.AccumulatorTaskBytes, .Portions = Settings.AccumulatorTaskPortions };
            case ETaskReason::Level:
                return TBudget{ .Bytes = Settings.LevelTaskBytes, .Portions = Settings.LevelTaskPortions };
            case ETaskReason::AdvanceMerge:
            case ETaskReason::AdvancePrefix:
            case ETaskReason::AdvanceMove:
                return TBudget{ .Bytes = Settings.SnapshotTaskBytes, .Portions = Settings.SnapshotTaskPortions };
        }
        AFL_VERIFY(false)("reason", (ui32)task.Reason);
        return TBudget{};
    }

    /// Whether the candidate fits the budget of the task's kind, in raw bytes (2026-11-07); when it does not, the task
    /// remembers which cap stopped it.
    bool FitsBudget(TTask& task, const TPortion& candidate) const {
        const TBudget budget = BudgetFor(task);
        if (task.Portions.size() >= budget.Portions) {
            task.StopCause = EStopCause::PortionsLimit;
            return false;
        }
        if (task.Bytes + candidate.GetTotalRawBytes() > budget.Bytes) {
            task.StopCause = EStopCause::BytesLimit;
            return false;
        }
        return true;
    }

    /// Whether the task can take nothing more under the budget of its kind, whatever the candidate (2026-11-12): checked
    /// before a scan, so that a full task does not walk the portions it would reject for other reasons one by one.
    bool IsFull(TTask& task) const {
        const TBudget budget = BudgetFor(task);
        if (task.Portions.size() >= budget.Portions) {
            task.StopCause = EStopCause::PortionsLimit;
            return true;
        }
        if (task.Bytes >= budget.Bytes) {
            task.StopCause = EStopCause::BytesLimit;
            return true;
        }
        return false;
    }

    static void Append(TTask& task, const TConstPtr& p) {
        task.Bytes += p->GetTotalRawBytes();
        task.Portions.push_back(p);
    }

    /// Decision 15: the unlocked stack at the level's maximum, oldest first, capped by the budget, at least two
    /// portions; then the top-up of decision 17.
    std::optional<TTask> BuildLevelTask(const ui8 level, TIsLocked isLocked) const {
        const auto priority = LevelPriority(level);
        if (!priority) {
            return std::nullopt;
        }
        const auto& treeLevel = TreeLevelAt(level);
        std::vector<TConstPtr> stack;
        auto range = treeLevel.Tree.GetMaxRange();
        if (range) {
            range.ForEachValue([&](const ui64 id) {
                const auto& p = Registry.at(id).Portion;
                if (!isLocked(p)) {
                    stack.push_back(p);
                }
                return true;
            });
        }
        if (stack.size() < 2) {
            return std::nullopt;
        }
        std::sort(stack.begin(), stack.end(), TBySnapshot<TPortion>());
        TTask task;
        task.Kind = ETaskKind::Merge;
        task.Reason = ETaskReason::Level;
        task.SourceLevel = level;
        task.TargetLevel = level;
        task.Priority = *priority;
        for (const auto& p : stack) {
            if (task.Portions.size() >= 2 && !FitsBudget(task, *p)) {
                break;
            }
            Append(task, p);
        }
        TopUp(task, level, isLocked);
        return task;
    }

    /// Decision 17: portions of other tree levels whose whole key range lies inside the task's union range join
    /// while the budget lasts, lower levels first; the line and the accumulator never join.
    void TopUp(TTask& task, const ui8 ownLevel, TIsLocked isLocked) const {
        if (IsFull(task)) {
            return;
        }
        TKey unionStart = task.Portions.front()->IndexKeyStart();
        TKey unionEnd = task.Portions.front()->IndexKeyEnd();
        for (const auto& p : task.Portions) {
            if (p->IndexKeyStart() < unionStart) {
                unionStart = p->IndexKeyStart();
            }
            if (unionEnd < p->IndexKeyEnd()) {
                unionEnd = p->IndexKeyEnd();
            }
        }
        for (ui8 level = FIRST_TREE_LEVEL; level <= Settings.TopLevel(); ++level) {
            if (level == ownLevel) {
                continue;
            }
            const auto& portions = TreeLevelAt(level).Portions;
            for (auto it = portions.lower_bound(unionStart); it != portions.end() && !(unionEnd < (*it)->IndexKeyEnd()); ++it) {
                const TConstPtr& p = *it;
                if (p->IndexKeyStart() < unionStart || isLocked(p)) {
                    continue;
                }
                if (!FitsBudget(task, *p)) {
                    return;
                }
                Append(task, p);
            }
        }
    }

    /// The accumulator task: at the trigger, up to the budget, in end-key order so that neighbours meet.
    std::optional<TTask> BuildAccumulatorTask(TIsLocked isLocked) const {
        const auto priority = AccumulatorPriority();
        if (!priority) {
            return std::nullopt;
        }
        TTask task;
        task.Kind = ETaskKind::Merge;
        task.Reason = ETaskReason::Accumulator;
        task.SourceLevel = ACCUMULATOR_LEVEL;
        task.TargetLevel = ACCUMULATOR_LEVEL;
        task.Priority = *priority;
        for (const auto& p : Accumulator) {
            if (task.Portions.size() >= 2 && IsFull(task)) {
                break;
            }
            if (isLocked(p)) {
                continue;
            }
            if (task.Portions.size() >= 2 && !FitsBudget(task, *p)) {
                break;
            }
            Append(task, p);
        }
        if (task.Portions.size() < 2) {
            return std::nullopt;
        }
        return task;
    }

    /// The advance task and its target, not yet handed out: the first settled, unlocked portion of the set that no task
    /// holds and whose composition succeeds (decisions 19 to 21). The scan resumes where the last one found its target or
    /// gave up (2026-11-12): everything before is blocked until the set changes in a way that could free it, see
    /// AdvanceScanFrom.
    std::optional<std::pair<TTask, TConstPtr>> BuildAdvanceTask(TIsLocked isLocked) const {
        auto it = AdvanceScanFrom ? OutsideUnlocked.lower_bound(*AdvanceScanFrom) : OutsideUnlocked.begin();
        for (; it != OutsideUnlocked.end(); ++it) {
            const TConstPtr& candidate = *it;
            if (Clock.SettledBefore < candidate->RecordSnapshotMin()) {
                break;   // ordered by snapshot: everything from here on is younger than L
            }
            if (isLocked(candidate)) {
                continue;
            }
            if (auto task = ComposeAdvance(Registry.at(candidate->GetPortionId()), isLocked)) {
                AdvanceScanFrom = candidate;
                return std::make_pair(std::move(*task), candidate);
            }
        }
        // Blocked up to the first unsettled target, or to the end: the last one is looked at again, cheaply, so that
        // the position stays inside the set.
        if (it != OutsideUnlocked.end()) {
            AdvanceScanFrom = *it;
        } else if (!OutsideUnlocked.empty()) {
            AdvanceScanFrom = *std::prev(it);
        }
        return std::nullopt;
    }

    using TCheckPoint = typename TTask::TCheckPoint;

    std::optional<TTask> ComposeAdvance(const TEntry& entry, TIsLocked isLocked) const {
        if (entry.Level == ACCUMULATOR_LEVEL) {
            return ComposeSmallAdvance(entry.Portion, isLocked);
        }
        return ComposeLargeAdvance(entry.Portion, entry.Level, isLocked);
    }

    /// The first neighbour always fits (decision 20, as the two-portion minimum); later ones only within the budget.
    bool TryAppend(TTask& task, const TConstPtr& p, bool& neighbourTaken) const {
        if (neighbourTaken && !FitsBudget(task, *p)) {
            return false;
        }
        Append(task, p);
        neighbourTaken = true;
        return true;
    }

    /// Line portions stay in their set while handed out, so availability checks both.
    bool IsAvailable(const TConstPtr& p, TIsLocked isLocked) const {
        return !Registry.at(p->GetPortionId()).HandedOut && !isLocked(p);
    }

    /// The key window of an advance (decision 20, revised 2026-10-26): [Left, Right] inclusive when the sweep ran out of
    /// intersecting portions, [Left, Cut) when the budget or a portion held by another task stopped it.
    struct TWindow {
        TKey Left;
        TKey Right;
        std::optional<TKey> Cut;
        bool Complete = true;
        bool LineTaken = false;
        /// The bytes of the window swept on without the budget: the task's bytes when nothing was cut.
        ui64 IdealBytes = 0;
        /// The dry sweep reached its cap with candidates left (2026-11-08): IdealBytes is a lower bound.
        bool IdealCapped = false;
    };

    struct TCandidate {
        TConstPtr Portion;
        /// The start clamped to the window's left border: the order of the sweep.
        TKey EffectiveStart;
        /// The end key and the id, read once when the candidate is made (2026-11-08): the engine's key accessors build a
        /// row on every call, and the comparison below runs on every step.
        TKey End;
        ui64 Id = 0;
        bool Line = false;
    };

    /// The order of the sweep: the clamped start, line portions first at equal starts, then the end key and the id, which
    /// makes the order total and the walk deterministic. Written out rather than as tuples so that it copies no key.
    static bool SweepsBefore(const TCandidate& a, const TCandidate& b) {
        if (a.EffectiveStart < b.EffectiveStart) {
            return true;
        }
        if (b.EffectiveStart < a.EffectiveStart) {
            return false;
        }
        if (a.Line != b.Line) {
            return a.Line;
        }
        if (a.End < b.End) {
            return true;
        }
        if (b.End < a.End) {
            return false;
        }
        return a.Id < b.Id;
    }

    /// The candidates of a sweep in sweep order, read lazily (2026-11-12) from three kinds of source: the stack covering the
    /// left border, read once from every level's tree and sorted; the line from the left border on; and every level's
    /// start-ordered set from just beyond the left border. Each source yields its portions in sweep order, so the next
    /// candidate is the least of the sources' heads that starts inside the window. A head beyond the window waits and comes
    /// out in order once the window has grown to it: everything taken before started no further right than the old border.
    /// A step costs the sources, a dozen at most, not the candidates. Before (2026-11-08), every portion intersecting the
    /// window was pushed into a heap before the first was taken, so a wide target over thousands of small portions paid for
    /// all of them to take sixteen, and every growth of the window read the whole stack at its new border again, which
    /// made a sweep over nested spans quadratic.
    class TSweepCursors {
        const TSnapshotTiling& Owner;
        const TKey Left;
        const ui64 TargetId;
        const TIsLocked IsLocked;
        std::vector<TCandidate> Stack;
        size_t StackNext = 0;
        typename TEndSorted::const_iterator LineNext;
        std::optional<TCandidate> LineHead;
        std::vector<typename TStartSorted::const_iterator> StartNext;
        std::vector<std::optional<TCandidate>> StartHeads;

        TCandidate Make(const TConstPtr& p, const bool line) const {
            TKey effectiveStart = p->IndexKeyStart();
            if (effectiveStart < Left) {
                effectiveStart = Left;
            }
            return TCandidate{ .Portion = p, .EffectiveStart = std::move(effectiveStart), .End = p->IndexKeyEnd(), .Id = p->GetPortionId(),
                .Line = line };
        }

        /// Line portions come out whatever their state and are judged when taken: a held one cuts the window.
        void FillLine() {
            if (!LineHead && LineNext != Owner.Line.end()) {
                LineHead = Make(*LineNext, true);
                ++LineNext;
            }
        }

        /// Tree portions come out unlocked only, and never the target.
        void FillStart(const size_t index) {
            auto& head = StartHeads[index];
            auto& next = StartNext[index];
            const TStartSorted& starts = Owner.TreeLevelAt(static_cast<ui8>(FIRST_TREE_LEVEL + index)).Starts;
            while (!head && next != starts.end()) {
                const TConstPtr& p = *next;
                ++next;
                if (p->GetPortionId() != TargetId && !IsLocked(p)) {
                    head = Make(p, false);
                }
            }
        }

    public:
        TSweepCursors(const TSnapshotTiling& owner, const TKey& left, const ui64 targetId, TIsLocked isLocked)
            : Owner(owner)
            , Left(left)
            , TargetId(targetId)
            , IsLocked(isLocked)
            , LineNext(owner.Line.lower_bound(left))
        {
            for (ui8 level = FIRST_TREE_LEVEL; level <= Owner.Settings.TopLevel(); ++level) {
                const TTreeLevel& treeLevel = Owner.TreeLevelAt(level);
                StartNext.push_back(treeLevel.Starts.upper_bound(left));
                StartHeads.emplace_back();
                if (treeLevel.Portions.empty()) {
                    continue;
                }
                if (auto range = treeLevel.Tree.FindRange(left)) {
                    range.ForEachValue([&](const ui64 id) {
                        if (id != TargetId) {
                            const TConstPtr& p = Owner.Registry.at(id).Portion;
                            if (!IsLocked(p)) {
                                Stack.push_back(Make(p, false));
                            }
                        }
                        return true;
                    });
                }
            }
            std::sort(Stack.begin(), Stack.end(), SweepsBefore);
        }

        /// The next candidate starting at or before the border, in sweep order; nothing when every remaining one starts
        /// beyond it.
        std::optional<TCandidate> Next(const TKey& right) {
            TCandidate* best = nullptr;
            // The source of the least eligible head: -2 the stack, -1 the line, otherwise the index of a tree level.
            i64 source = 0;
            const auto offer = [&](TCandidate& candidate, const i64 from) {
                if (!(right < candidate.EffectiveStart) && (!best || SweepsBefore(candidate, *best))) {
                    best = &candidate;
                    source = from;
                }
            };
            if (StackNext < Stack.size()) {
                offer(Stack[StackNext], -2);
            }
            FillLine();
            if (LineHead) {
                offer(*LineHead, -1);
            }
            for (size_t index = 0; index < StartHeads.size(); ++index) {
                FillStart(index);
                if (StartHeads[index]) {
                    offer(*StartHeads[index], static_cast<i64>(index));
                }
            }
            if (!best) {
                return std::nullopt;
            }
            TCandidate taken = std::move(*best);
            if (source == -2) {
                ++StackNext;
            } else if (source == -1) {
                LineHead.reset();
            } else {
                StartHeads[source].reset();
            }
            return taken;
        }
    };

    /// How far the dry sweep behind IdealBytes goes: ideal_sweep_budgets snapshot byte budgets, saturating.
    ui64 IdealSweepLimit() const {
        const ui64 budget = Settings.SnapshotTaskBytes;
        const ui64 factor = Settings.IdealSweepBudgets;
        return budget > Max<ui64>() / factor ? Max<ui64>() : budget * factor;
    }

    /// How many candidates the dry sweep considers at most: ideal_sweep_budgets snapshot portion budgets, saturating
    /// (2026-11-12). The byte cap alone, eight budgets of 256 MB, let it walk tens of thousands of small portions.
    ui64 IdealSweepSteps() const {
        const ui64 budget = Settings.SnapshotTaskPortions;
        const ui64 factor = Settings.IdealSweepBudgets;
        return budget > Max<ui64>() / factor ? Max<ui64>() : budget * factor;
    }

    /// Decision 20, revised 2026-10-26: the window starts at the target, or at the first line portion the target
    /// intersects, and takes every unlocked portion of the line and of every tree level that intersects it, in order of
    /// start key, line portions first at equal starts. A taken portion that reaches further right grows the window, so
    /// overlapping neighbours chain until a gap or the budget. The sweep stops at the first portion that does not fit or
    /// that another task holds, and the window is cut at its start: everything inside is absorbed whole and free of line
    /// portions, so the slice inside enters the line, and the parts beyond are remainders placed by the arrival rule. A
    /// held line portion covering the left border blocks the target. When the portion that does not fit covers the left
    /// border itself no cut leaves a non-empty window, so the window closes at the end of the first line portion, the only
    /// one inside, before the first line portion when none was taken yet, or at the target's end without the line.
    /// The candidates come from cursors in sweep order (TSweepCursors, 2026-11-12): a step costs the sources, not the
    /// portions intersecting the window.
    std::optional<TWindow> SweepWindow(TTask& task, const TConstPtr& target, TIsLocked isLocked) const {
        const TKey start = target->IndexKeyStart();
        const TKey end = target->IndexKeyEnd();
        TWindow window{ .Left = start, .Right = end };
        std::optional<TKey> firstLineStart;
        if (const auto first = Line.lower_bound(start); first != Line.end() && !(end < (*first)->IndexKeyStart())) {
            firstLineStart = (*first)->IndexKeyStart();
            if (*firstLineStart < window.Left) {
                window.Left = *firstLineStart;
            }
            // A held line portion covering the left border blocks the target. It is the first candidate the sweep would
            // take, so it is judged before any candidate is gathered (2026-11-12): behind one held line portion there may
            // be thousands of blocked targets, and each was scanned on every request.
            if (!(window.Left < *firstLineStart) && !IsAvailable(*first, isLocked)) {
                return std::nullopt;
            }
        }
        TSweepCursors cursors(*this, window.Left, target->GetPortionId(), isLocked);
        bool neighbourTaken = false;
        std::optional<TKey> lineEnd;
        std::optional<TCandidate> overBudget;
        while (auto candidate = cursors.Next(window.Right)) {
            const TConstPtr& p = candidate->Portion;
            if (candidate->Line && !IsAvailable(p, isLocked)) {
                if (!(window.Left < p->IndexKeyStart())) {
                    return std::nullopt;
                }
                window.Cut = p->IndexKeyStart();
                window.Complete = false;
                task.StopCause = EStopCause::Held;
                break;
            }
            if (!TryAppend(task, p, neighbourTaken)) {
                window.Cut = p->IndexKeyStart();
                window.Complete = false;
                overBudget = std::move(candidate);
                break;
            }
            if (candidate->Line) {
                window.LineTaken = true;
                if (!lineEnd || *lineEnd < candidate->End) {
                    lineEnd = candidate->End;
                }
            }
            if (window.Right < candidate->End) {
                window.Right = candidate->End;
            }
        }
        // The memory shortage (2026-10-26): sweep on without taking anything, to learn what the uncut window would cost,
        // up to ideal_sweep_budgets budgets of bytes (2026-11-08) and of portions (2026-11-12): the walk is diagnostic and
        // must not cost the write path a table.
        window.IdealBytes = task.Bytes;
        if (overBudget) {
            const ui64 bytesLimit = IdealSweepLimit();
            const ui64 stepsLimit = IdealSweepSteps();
            ui64 steps = 0;
            TKey dryRight = window.Right;
            const auto consider = [&](const TCandidate& candidate) {
                ++steps;
                window.IdealBytes += candidate.Portion->GetTotalRawBytes();
                if (dryRight < candidate.End) {
                    dryRight = candidate.End;
                }
            };
            consider(*overBudget);
            while (const auto candidate = cursors.Next(dryRight)) {
                if (window.IdealBytes >= bytesLimit || steps >= stepsLimit) {
                    window.IdealCapped = true;
                    break;
                }
                if (candidate->Line && !IsAvailable(candidate->Portion, isLocked)) {
                    break;
                }
                consider(*candidate);
            }
        }
        if (window.Cut && !(window.Left < *window.Cut)) {
            window.Cut.reset();
            if (lineEnd) {
                window.Right = *lineEnd;
            } else if (firstLineStart && window.Left < *firstLineStart) {
                window.Cut = *firstLineStart;
            } else {
                window.Right = end;
            }
        }
        return window;
    }

    /// The window's borders as checkpoints: the slice inside is absorbed whole and enters the line; a cut window is a prefix.
    static void ApplyWindow(TTask& task, const TWindow& window) {
        task.CheckPoints.push_back(TCheckPoint{ .Key = window.Left, .KeyBelongsToLeft = false });
        if (window.Cut) {
            task.CheckPoints.push_back(TCheckPoint{ .Key = *window.Cut, .KeyBelongsToLeft = false });
        } else {
            task.CheckPoints.push_back(TCheckPoint{ .Key = window.Right, .KeyBelongsToLeft = true });
        }
        if (!window.Complete) {
            task.Reason = ETaskReason::AdvancePrefix;
        }
    }

    /// Drops cuts at the task's outer borders, which would produce empty slices, then sorts and deduplicates.
    static void FinalizeCheckPoints(TTask& task) {
        if (task.CheckPoints.empty()) {
            return;
        }
        TKey spanStart = task.Portions.front()->IndexKeyStart();
        TKey spanEnd = task.Portions.front()->IndexKeyEnd();
        for (const auto& p : task.Portions) {
            if (p->IndexKeyStart() < spanStart) {
                spanStart = p->IndexKeyStart();
            }
            if (spanEnd < p->IndexKeyEnd()) {
                spanEnd = p->IndexKeyEnd();
            }
        }
        std::vector<TCheckPoint> kept;
        for (const auto& cut : task.CheckPoints) {
            if (!cut.KeyBelongsToLeft && !(spanStart < cut.Key)) {
                continue;
            }
            if (cut.KeyBelongsToLeft && !(cut.Key < spanEnd)) {
                continue;
            }
            kept.push_back(cut);
        }
        std::sort(kept.begin(), kept.end());
        kept.erase(std::unique(kept.begin(), kept.end()), kept.end());
        task.CheckPoints = std::move(kept);
    }

    /// A normal-size target (decisions 20 and 26, revised 2026-10-26): the window swept from the target; a target that
    /// intersects nothing anywhere is a metadata move. The output carries the line's level when line portions were taken,
    /// so the slice inside the window enters the line whatever its snapshot; otherwise it keeps the target's level and
    /// the arrival rule admits the slice that carries the target's rows, old enough and free of the line.
    std::optional<TTask> ComposeLargeAdvance(const TConstPtr& target, const ui8 level, TIsLocked isLocked) const {
        TTask task;
        task.Kind = ETaskKind::Merge;
        task.Reason = ETaskReason::AdvanceMerge;
        task.SourceLevel = level;
        task.TargetLevel = level;
        Append(task, target);
        const auto window = SweepWindow(task, target, isLocked);
        if (!window) {
            return std::nullopt;
        }
        if (task.Portions.size() == 1) {
            task.Kind = ETaskKind::MetadataMove;
            task.Reason = ETaskReason::AdvanceMove;
            task.TargetLevel = LINE_LEVEL;
            return task;
        }
        if (window->LineTaken) {
            task.TargetLevel = LINE_LEVEL;
        }
        ApplyWindow(task, *window);
        FinalizeCheckPoints(task);
        task.IdealBytes = std::max(window->IdealBytes, task.Bytes);
        task.IdealCapped = window->IdealCapped;
        return task;
    }

    /// The nearest line portion to a range that intersects none: the successor in end-key order, then the predecessor.
    TConstPtr NearestLinePortion(const TKey& start, TIsLocked isLocked) const {
        const auto successor = Line.lower_bound(start);
        if (successor != Line.end() && IsAvailable(*successor, isLocked)) {
            return *successor;
        }
        if (successor != Line.begin()) {
            const auto predecessor = std::prev(successor);
            if (IsAvailable(*predecessor, isLocked)) {
                return *predecessor;
            }
        }
        return nullptr;
    }

    /// The nearest normal-size portion on any tree level: the successor with the smallest end key, then the
    /// predecessor with the largest end key.
    TConstPtr NearestTreePortion(const TKey& start, TIsLocked isLocked) const {
        TConstPtr successor;
        TConstPtr predecessor;
        const auto cmp = TByEndKey<TKey, TPortion>();
        for (ui8 level = FIRST_TREE_LEVEL; level <= Settings.TopLevel(); ++level) {
            const auto& portions = TreeLevelAt(level).Portions;
            auto it = portions.lower_bound(start);
            for (auto s = it; s != portions.end(); ++s) {
                if (isLocked(*s)) {
                    continue;
                }
                if (!successor || cmp(*s, successor)) {
                    successor = *s;
                }
                break;
            }
            for (auto p = it; p != portions.begin();) {
                --p;
                if (isLocked(*p)) {
                    continue;
                }
                if (!predecessor || cmp(predecessor, *p)) {
                    predecessor = *p;
                }
                break;
            }
        }
        return successor ? successor : predecessor;
    }

    /// A table without any normal-size portion (decision 21): the target with its nearest small portions in end-key
    /// order, successor and predecessor in turn, within the budget.
    void TakeNearestSmall(TTask& task, const TConstPtr& target, bool& neighbourTaken, TIsLocked isLocked) const {
        const auto self = Accumulator.find(target);
        AFL_VERIFY(self != Accumulator.end())("portion_id", target->GetPortionId());
        auto successor = std::next(self);
        auto predecessor = self;
        bool more = true;
        while (more) {
            more = false;
            while (successor != Accumulator.end() && isLocked(*successor)) {
                ++successor;
            }
            if (successor != Accumulator.end()) {
                if (!TryAppend(task, *successor, neighbourTaken)) {
                    return;
                }
                ++successor;
                more = true;
            }
            while (predecessor != Accumulator.begin() && isLocked(*std::prev(predecessor))) {
                --predecessor;
            }
            if (predecessor != Accumulator.begin()) {
                --predecessor;
                if (!TryAppend(task, *predecessor, neighbourTaken)) {
                    return;
                }
                more = true;
            }
        }
    }

    /// A small target (decision 21, revised 2026-10-26): the window swept from the target, line and tree portions alike;
    /// when nothing intersects, the nearest line portion without a separating cut, else the nearest normal-size portion
    /// on any level, else the nearest small portions; then the accumulator portions inside the task's span and before the
    /// cut, as many as fit.
    std::optional<TTask> ComposeSmallAdvance(const TConstPtr& target, TIsLocked isLocked) const {
        const TKey start = target->IndexKeyStart();
        const TKey end = target->IndexKeyEnd();
        TTask task;
        task.Kind = ETaskKind::Merge;
        task.Reason = ETaskReason::AdvanceMerge;
        task.SourceLevel = ACCUMULATOR_LEVEL;
        task.TargetLevel = LINE_LEVEL;
        Append(task, target);
        const auto window = SweepWindow(task, target, isLocked);
        if (!window) {
            return std::nullopt;
        }
        std::optional<TKey> limit;
        if (task.Portions.size() > 1) {
            ApplyWindow(task, *window);
            limit = window->Cut;
        } else {
            bool neighbourTaken = false;
            if (!Line.empty()) {
                const TConstPtr neighbour = NearestLinePortion(start, isLocked);
                if (!neighbour) {
                    return std::nullopt;
                }
                Append(task, neighbour);
                neighbourTaken = true;
            } else if (const TConstPtr neighbour = NearestTreePortion(start, isLocked)) {
                Append(task, neighbour);
                neighbourTaken = true;
            } else {
                task.TargetLevel = ACCUMULATOR_LEVEL;
                TakeNearestSmall(task, target, neighbourTaken, isLocked);
                if (task.Portions.size() < 2) {
                    return std::nullopt;
                }
                task.IdealBytes = task.Bytes;
                return task;
            }
        }

        TKey spanStart = start;
        TKey spanEnd = end;
        for (const auto& p : task.Portions) {
            if (p->IndexKeyStart() < spanStart) {
                spanStart = p->IndexKeyStart();
            }
            if (spanEnd < p->IndexKeyEnd()) {
                spanEnd = p->IndexKeyEnd();
            }
        }
        for (auto it = Accumulator.lower_bound(spanStart); it != Accumulator.end() && !(spanEnd < (*it)->IndexKeyEnd()); ++it) {
            const TConstPtr& p = *it;
            if (IsFull(task) || (limit && !(p->IndexKeyEnd() < *limit))) {
                break;
            }
            if (p == target || p->IndexKeyStart() < spanStart || isLocked(p)) {
                continue;
            }
            if (!FitsBudget(task, *p)) {
                break;
            }
            Append(task, p);
        }
        FinalizeCheckPoints(task);
        task.IdealBytes = std::max(window->IdealBytes, task.Bytes);
        task.IdealCapped = window->IdealCapped;
        return task;
    }

    /// Decision 14: the inputs leave their levels' unlocked structures, and the task's priority is remembered until they
    /// are removed or restored, so that the scheduling pass after an abort still reaches a task request.
    void HandOut(const TTask& task) {
        TTouchedLevels touched;
        for (const auto& p : task.Portions) {
            HandOut(p->GetPortionId(), touched);
        }
        RefreshCounters(touched);
        const TOptimizationPriority floor = TOptimizationPriority::Optimization(1);
        const TOptimizationPriority& priority = floor < task.Priority ? task.Priority : floor;
        if (!PendingPriority || *PendingPriority < priority) {
            PendingPriority = priority;
        }
    }

    /// Decision 14: an input leaves its level's unlocked structures; the line keeps it visible; the set behind S
    /// keeps it for the forced-request reading.
    void HandOut(const ui64 id, TTouchedLevels& touched) {
        auto& entry = Registry.at(id);
        AFL_VERIFY(!entry.HandedOut)("portion_id", id)("reason", "tiling-s_handed_out_twice");
        entry.HandedOut = true;
        AFL_VERIFY(HandedOut.emplace(id, entry.Portion).second)("portion_id", id);
        if (entry.Level != LINE_LEVEL) {
            AFL_VERIFY(OutsideUnlocked.erase(entry.Portion))("portion_id", id);
            Detach(entry.Portion, entry.Level);
            touched.set(entry.Level);
        }
    }

    void Restore(const ui64 id, TTouchedLevels& touched) {
        auto& entry = Registry.at(id);
        AFL_VERIFY(entry.HandedOut)("portion_id", id)("reason", "tiling-s_restore_not_handed_out");
        entry.HandedOut = false;
        AFL_VERIFY(HandedOut.erase(id))("portion_id", id);
        ForgetPendingPriorityWhenIdle();
        if (entry.Level != LINE_LEVEL) {
            AFL_VERIFY(OutsideUnlocked.insert(entry.Portion).second)("portion_id", id);
            Attach(entry.Portion, entry.Level);
            touched.set(entry.Level);
        }
    }

    void ForgetPendingPriorityWhenIdle() {
        if (HandedOut.empty()) {
            PendingPriority.reset();
        }
    }

    void Attach(const TPtr& p, const ui8 level) {
        const ui64 id = p->GetPortionId();
        switch (level) {
            case ACCUMULATOR_LEVEL:
                AFL_VERIFY(Accumulator.insert(p).second)("portion_id", id);
                break;
            case LINE_LEVEL:
                AFL_VERIFY(Line.insert(p).second)("portion_id", id);
                break;
            default: {
                auto& treeLevel = TreeLevelAt(level);
                AFL_VERIFY(treeLevel.Portions.insert(p).second)("portion_id", id);
                AFL_VERIFY(treeLevel.Starts.insert(p).second)("portion_id", id);
                treeLevel.Tree.Add(id, p->IndexKeyStart(), p->IndexKeyEnd());
                break;
            }
        }
    }

    void Detach(const TConstPtr& p, const ui8 level) {
        const ui64 id = p->GetPortionId();
        switch (level) {
            case ACCUMULATOR_LEVEL:
                AFL_VERIFY(Accumulator.erase(p))("portion_id", id);
                break;
            case LINE_LEVEL:
                AFL_VERIFY(Line.erase(p))("portion_id", id);
                break;
            default: {
                auto& treeLevel = TreeLevelAt(level);
                AFL_VERIFY(treeLevel.Portions.erase(p))("portion_id", id);
                AFL_VERIFY(treeLevel.Starts.erase(p))("portion_id", id);
                treeLevel.Tree.Remove(id);
                break;
            }
        }
    }

    /// Height and overload per unit, and the planner's highest unit level in the shared tiling category, which the gap
    /// rule and the node back-off read: its bucket 10 is non-empty exactly while some planner on the node is at its
    /// overload edge on the unlocked view (decision 25). Once per batch of changes (2026-11-12): a task's thousand inputs
    /// leave their unit in one step, and the counters are read from other threads at their own pace, so nobody misses the
    /// intermediate readings.
    void RefreshCounters(const TTouchedLevels& touched) const {
        if (touched.none()) {
            return;
        }
        for (ui32 level = 0; level <= Settings.TopLevel(); ++level) {
            if (touched.test(level)) {
                RefreshLevelCounters(static_cast<ui8>(level));
            }
        }
        PublishHighestLevel();
    }

    void RefreshLevelCounters(const ui8 level) const {
        const auto& counters = LevelCounters(level);
        if (level == ACCUMULATOR_LEVEL) {
            const auto priority = AccumulatorPriority();
            counters.Portions->SetHeight(Accumulator.size());
            counters.Portions->SetOverload(priority ? priority->GetLevel() : 0);
        } else if (level == LINE_LEVEL) {
            counters.Portions->SetHeight(Line.size());
            counters.Portions->SetOverload(0);
        } else {
            const auto priority = LevelPriority(level);
            counters.Portions->SetHeight(GetHeight(level));
            counters.Portions->SetOverload(priority ? priority->GetLevel() : 0);
        }
    }

    /// The planner's highest source level in the shared tiling category (decision 25); the delay source moves with the
    /// clock, so this runs on clock updates as well as on portion changes.
    void PublishHighestLevel() const {
        const auto highest = GetTaskPriority();
        Counters.GetTilingCounters().Portions->SetOverload(highest ? highest->GetLevel() : 0);
    }

    /// Decision 7: old enough means RecordSnapshotMin at or below Y.
    bool IsOldEnough(const TPortion& p) const {
        return Y && !(*Y < p.RecordSnapshotMin());
    }

    bool IntersectsLine(const TPortion& p) const {
        return IntersectsLine(p.IndexKeyStart(), p.IndexKeyEnd());
    }

    bool IntersectsLine(const TKey& start, const TKey& end) const {
        return IntersectsDisjointSet(Line, start, end);
    }

    /// For a set of pairwise disjoint portions the first one ending at or after the start is the only candidate.
    static bool IntersectsDisjointSet(const TEndSorted& portions, const TKey& start, const TKey& end) {
        const auto it = portions.lower_bound(start);
        return it != portions.end() && !(end < (*it)->IndexKeyStart());
    }

    /// The arrival rule (Placing an arriving portion; decision 26 for a committed metadata move).
    void Place(const TPtr& p, TTouchedLevels& touched) {
        // A portion that arrives with the line's level is a committed metadata move (or a line portion at bootstrap).
        const bool committedMove = p->GetCompactionLevel() == LINE_LEVEL;
        ui8 level = ACCUMULATOR_LEVEL;
        if (committedMove && !IntersectsLine(*p)) {
            level = LINE_LEVEL;
        } else if (IsSmall(*p)) {
            level = ACCUMULATOR_LEVEL;
        } else if (IsOldEnough(*p) && !IntersectsLine(*p)) {
            level = LINE_LEVEL;
        } else {
            level = ChooseTreeLevel(*p);
        }
        if (committedMove) {
            (level == LINE_LEVEL ? Counters.AdvanceMoveLanded : Counters.AdvanceMoveReturned)->Inc();
        }
        // The advance's scan resumes past this arrival only when it cannot free a blocked target: one that sorts before
        // the position is a target the scan has not judged, one in the line may be the neighbour a small target lacked.
        if (AdvanceScanFrom && (level == LINE_LEVEL || TBySnapshot<TPortion>()(p, *AdvanceScanFrom))) {
            ResetAdvanceScan();
        }
        Insert(p, level, touched);
    }

    /// Rule 3, the descent by width (decision 10): start at the top, stay on level k when the portion is wider than T
    /// against level k - 1, otherwise descend; an empty level has width zero; level 2 is where the descent stops.
    ui8 ChooseTreeLevel(const TPortion& p) const {
        for (ui8 level = Settings.TopLevel(); level > FIRST_TREE_LEVEL; --level) {
            if (Width(p, TreeLevelAt(level - 1)) > Settings.WidthThreshold) {
                return level;
            }
        }
        return FIRST_TREE_LEVEL;
    }

    /// The greedy maximum non-intersecting subset of the level's portions that intersect p, in end-key order: the portions
    /// ending inside p's range, then the stack covering p's end key read from the tree, with an early exit at T + 1.
    /// Every skipped portion covers the last kept end key, so at most (T + 1) * (H + 1) + H portions are examined.
    ui64 Width(const TPortion& p, const TTreeLevel& level, ui64* examined = nullptr) const {
        const TKey start = p.IndexKeyStart();
        const TKey end = p.IndexKeyEnd();
        ui64 kept = 0;
        ui64 seen = 0;
        std::optional<TKey> lastKeptEnd;
        const auto consider = [&](const TPortion& candidate) {
            ++seen;
            if (!lastKeptEnd || *lastKeptEnd < candidate.IndexKeyStart()) {
                lastKeptEnd = candidate.IndexKeyEnd();
                ++kept;
            }
            return kept > Settings.WidthThreshold;
        };
        for (auto it = level.Portions.lower_bound(start); it != level.Portions.end() && !(end < (*it)->IndexKeyEnd()); ++it) {
            if (consider(**it)) {
                break;
            }
        }
        if (kept <= Settings.WidthThreshold) {
            std::vector<TConstPtr> tail;
            auto range = level.Tree.FindRange(end);
            if (range) {
                range.ForEachValue([&](const ui64 id) {
                    const auto& candidate = Registry.at(id).Portion;
                    if (end < candidate->IndexKeyEnd()) {
                        tail.push_back(candidate);
                    }
                    return true;
                });
            }
            std::sort(tail.begin(), tail.end(), TByEndKey<TKey, TPortion>());
            for (const auto& candidate : tail) {
                if (consider(*candidate)) {
                    break;
                }
            }
        }
        if (examined) {
            *examined = seen;
        }
        return kept;
    }

    /// Bootstrap (decisions 11 and 12): small portions to the accumulator, the rest in snapshot order into the line
    /// until the first one that does not fit, then the tree levels.
    void InitialAddPortions(const std::vector<TPtr>& add, TTouchedLevels& touched) {
        std::vector<TPtr> large;
        for (const auto& p : add) {
            if (!IsTracked(*p)) {
                continue;
            }
            if (IsSmall(*p)) {
                Insert(p, ACCUMULATOR_LEVEL, touched);
            } else {
                large.push_back(p);
            }
        }
        std::sort(large.begin(), large.end(), TBySnapshot<TPortion>());
        bool stopped = false;
        std::vector<TPtr> rest;
        for (const auto& p : large) {
            if (!stopped && !IntersectsLine(*p)) {
                Insert(p, LINE_LEVEL, touched);
                Y = p->RecordSnapshotMin();
            } else {
                stopped = true;
                rest.push_back(p);
            }
        }
        FillTreeLevelsAtBootstrap(std::move(rest), touched);
    }

    /// Steps 4 and 5 of the bootstrap, seed-and-fill (decision 10): for each level from 2 up, seed a non-intersecting
    /// layer in snapshot order, then admit every remaining portion at most T wide against that level; the rest go up.
    /// Portions left after the top level stay on the top level and show as its height.
    void FillTreeLevelsAtBootstrap(std::vector<TPtr> rest, TTouchedLevels& touched) {
        for (ui8 level = FIRST_TREE_LEVEL; level <= Settings.TopLevel() && !rest.empty(); ++level) {
            const auto& treeLevel = TreeLevelAt(level);
            std::vector<TPtr> skipped;
            for (const auto& p : rest) {
                if (!IntersectsDisjointSet(treeLevel.Portions, p->IndexKeyStart(), p->IndexKeyEnd())) {
                    Insert(p, level, touched);
                } else {
                    skipped.push_back(p);
                }
            }
            std::vector<TPtr> next;
            for (const auto& p : skipped) {
                if (Width(*p, treeLevel) <= Settings.WidthThreshold) {
                    Insert(p, level, touched);
                } else {
                    next.push_back(p);
                }
            }
            if (level == Settings.TopLevel()) {
                for (const auto& p : next) {
                    Insert(p, level, touched);
                }
                next.clear();
            }
            rest = std::move(next);
        }
    }

    void Insert(const TPtr& p, const ui8 level, TTouchedLevels& touched) {
        const ui64 id = p->GetPortionId();
        AFL_VERIFY(Registry.emplace(id, TEntry{ .Portion = p, .Level = level }).second)("portion_id", id);
        Attach(p, level);
        touched.set(level);
        if (level != LINE_LEVEL) {
            AFL_VERIFY(Outside.insert(p).second)("portion_id", id);
            AFL_VERIFY(OutsideUnlocked.insert(p).second)("portion_id", id);
        }
        if constexpr (std::is_same_v<TPortion, NOlap::TPortionInfo>) {
            LevelCounters(level).Portions->AddPortion(p);
        }
    }

    void Erase(const typename THashMap<ui64, TEntry>::iterator it, TTouchedLevels& touched) {
        const TEntry entry = it->second;
        Registry.erase(it);
        const TConstPtr p = entry.Portion;
        const ui64 id = p->GetPortionId();
        if (entry.Level != LINE_LEVEL) {
            AFL_VERIFY(Outside.erase(p))("portion_id", id);
            if (!entry.HandedOut) {
                AFL_VERIFY(OutsideUnlocked.erase(p))("portion_id", id);
            }
        }
        if (entry.HandedOut) {
            AFL_VERIFY(HandedOut.erase(id))("portion_id", id);
            ForgetPendingPriorityWhenIdle();
        }
        if (!entry.HandedOut || entry.Level == LINE_LEVEL) {
            Detach(p, entry.Level);
            touched.set(entry.Level);
        }
        if constexpr (std::is_same_v<TPortion, NOlap::TPortionInfo>) {
            LevelCounters(entry.Level).Portions->RemovePortion(p);
        }
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NSnapshotTiling
