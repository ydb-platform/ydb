#pragma once

#include "flat_comp.h"
#include "flat_page_gstat.h"

#include <library/cpp/time_provider/time_provider.h>
#include <ydb/core/tablet_flat/util_fmt_line.h>

namespace NKikimr {
namespace NTable {
namespace NCompGen {

    class TGenCompactionParams final : public TCompactionParams {
    public:
        void Describe(IOutputStream& out) const noexcept override;

    public:
        ui32 Generation = Max<ui32>();
    };

    class TGenCompactionStrategy final
        : public ICompactionStrategy
    {
    public:
        TGenCompactionStrategy(
            ui32 table,
            ICompactionBackend* backend,
            IResourceBroker* broker,
            ITimeProvider* time,
            NUtil::ILogger* logger,
            TString taskNameSuffix);

        ~TGenCompactionStrategy();

        void Start(TCompactionState state) override;
        void Stop() override;

        void ReflectSchema() override;
        void ReflectRemovedRowVersions() override;
        void UpdateCompactions() override;
        ui64 GetBackingSize() override;
        ui64 GetBackingSize(ui64 ownerTabletId) override;
        ui64 BeginMemCompaction(TTaskId taskId, TSnapEdge edge, ui64 forcedCompactionId) override;
        bool ScheduleBorrowedCompaction() override;
        void AllowBorrowedGarbageCompaction() override;
        ui64 GetLastFinishedForcedCompactionId() const override { return FinishedForcedGenCompactionId; }
        TInstant GetLastFinishedForcedCompactionTs() const override { return FinishedForcedGenCompactionTs; }
        TCompactionChanges CompactionFinished(
            ui64 compactionId,
            THolder<TCompactionParams> params,
            THolder<TCompactionResult> result) override;
        void PartMerged(TPartView part, ui32 level) override;
        void PartMerged(TIntrusiveConstPtr<TColdPart> part, ui32 level) override;
        TCompactionChanges PartsRemoved(TArrayRef<const TLogoBlobID> parts) override;
        TCompactionState SnapshotState() override;
        TCompactionChanges ApplyChanges() override;
        bool AllowForcedCompaction() override;
        void OutputHtml(IOutputStream& out) override;

    public:
        enum class EState {
            Free,
            Pending,
            PendingBackground,
            Compacting,
        };

        enum class EForcedState {
            None,
            Pending,
            Compacting,
        };

        enum class EDesiredMode {
            None,
            Normal,
            Background,
        };

    private:
        struct TStats {
            ui64 BackingSize = 0;
            ui64 TotalRows = 0;
            ui64 DroppedRows = 0;

            TStats() = default;

            explicit TStats(const TPartView& partView) {
                for (const auto& slice : *partView.Slices) {
                    // N.B. very old slice data may have Max<ui64>() rows
                    // Make sure TotalRows never overflows in such rare cases
                    ui64 delta = Min(slice.Rows(), Max<ui64>() - TotalRows);
                    TotalRows += delta;
                }

                ui64 partRows = partView->Stat.Rows;
                ui64 partDrops = partView->Stat.Drops;

                // Assume worst case: every drop has a hidden row that won't go away
                partDrops -= Min(partDrops, partView->Stat.HiddenRows);

                if (TotalRows >= partRows) {
                    // N.B. this also handles Max<ui64>() if it ever happens
                    TotalRows = partRows;
                    DroppedRows = partDrops;
                    BackingSize = partView->BackingSize();
                } else if (TotalRows > 0) {
                    // Calculate proportional dropped rows based on used rows
                    DroppedRows = (partDrops * TotalRows + partRows - 1) / partRows;
                    // Calculate proportional data size based on used rows
                    BackingSize = (partView->BackingSize() * TotalRows + partRows - 1) / partRows;
                }
            }

            TStats(ui64 backingSize, ui64 totalRows, ui64 droppedRows)
                : BackingSize(backingSize)
                , TotalRows(totalRows)
                , DroppedRows(droppedRows)
            { }

            void Reset() {
                BackingSize = 0;
                TotalRows = 0;
                DroppedRows = 0;
            }

            TStats& operator+=(const TStats& o) {
                BackingSize += o.BackingSize;
                TotalRows += o.TotalRows;
                DroppedRows += o.DroppedRows;
                return *this;
            }

            TStats& operator-=(const TStats& o) {
                BackingSize -= o.BackingSize;
                TotalRows -= o.TotalRows;
                DroppedRows -= o.DroppedRows;
                return *this;
            }

            ui32 DroppedRowsPercent() const {
                ui64 rows = TotalRows > DroppedRows ? TotalRows - DroppedRows : 0;
                if (DroppedRows > 0 && DroppedRows >= rows) {
                    return 100;
                } else if (DroppedRows > 0 && rows > 0) {
                    return DroppedRows * 100 / rows;
                } else {
                    return 0;
                }
            }
        };

        struct TPartInfo {
            TPartInfo(TPartView partView)
                : PartView(std::move(partView))
                , Label(PartView->Label)
                , Epoch(PartView->Epoch)
                , Stats(PartView)
            { }

            const TPartView PartView;
            const TLogoBlobID Label;
            const TEpoch Epoch;
            const TStats Stats;

            inline bool operator<(const TPartInfo& other) const {
                if (other.Epoch != Epoch) {
                    return other.Epoch < Epoch;
                }
                return other.Label < Label;
            }
        };

        struct TCompactionTask {
            TTaskId TaskId = 0;
            ui32 Priority = 0;
            TInstant SubmissionTimestamp;
            ui64 CompactionId = 0;
        };

        struct TGeneration {
            TList<TPartInfo> Parts;
            EState State = EState::Free;
            TCompactionTask Task;

            ui64 PartEpochCount = 0;

            size_t CompactingTailParts = 0;

            size_t TakenHeadParts = 0;
            ui64 TakenHeadBackingSize = 0;
            ui64 TakenHeadPartEpochCount = 0;

            TStats Stats;
            THashMap<ui64, TStats> StatsPerTablet;
            float OverloadFactor = 0.0;

            TPartInfo& PushFront(TPartView partView) noexcept;
            TPartInfo& PushBack(TPartView partView) noexcept;
            void PopFront() noexcept;
            void PopBack() noexcept;
        };

        struct TFinalState {
            EState State = EState::Free;
            TCompactionTask Task;
        };

        struct TPartAggregator;
        struct TExtraState;

    private:
        void SubmitTask(TCompactionTask& task, TString type, ui32 priority, ui32 generation);
        void UpdateTask(TCompactionTask& task, TString type, ui32 priority);

        void BeginGenCompaction(TTaskId taskId, ui32 generation);

        void OnForcedGenCompactionDone();

        ui32 ComputeBackgroundPriority(
            ui32 generation,
            const TCompactionPolicy::TGenerationPolicy& policy,
            TInstant now) const;

        ui32 ComputeBackgroundPriority(
            const TGenCompactionStrategy::TCompactionTask& task,
            const TCompactionPolicy::TBackgroundPolicy& policy,
            ui32 percentage,
            TInstant now) const;

        void CheckOverload(ui32 generation);
        void CheckGeneration(ui32 generation);
        EDesiredMode DesiredMode(ui32 generation) const;
        bool NeedToCompactAfterSplit(ui32 generation) const;

        ui64 PrepareCompaction(
            TTaskId taskId,
            TSnapEdge edge,
            ui32 generation,
            TExtraState& extra);

        void UpdateStats();
        void UpdateOverload();

        bool NeedToForceCompact(ui32 generation) const {
            return (ForcedState == EForcedState::Pending && ForcedGeneration == generation);
        }

        ui32 DroppedBytesPercent() const;
        bool NeedToStartForceCompaction() const;
        bool MaybeAutoStartForceCompaction();

    private:
        ui32 const Table;
        ICompactionBackend* const Backend;
        IResourceBroker* const Broker;
        ITimeProvider* const Time;
        NUtil::ILogger* const Logger;
        const TString TaskNameSuffix;

        TIntrusiveConstPtr<TCompactionPolicy> Policy;

        ui64 MemCompactionId = 0;
        ui64 FinalCompactionId = 0;
        ui32 FinalCompactionLevel = 0;
        size_t FinalCompactionTaken = 0;
        EForcedState ForcedState = EForcedState::None;
        ui64 ForcedMemCompactionId = 0;
        ui32 ForcedGeneration = 0;

        ui64 CurrentForcedGenCompactionId = 0;
        ui64 NextForcedGenCompactionId = 0;
        ui64 FinishedForcedGenCompactionId = 0;
        TInstant FinishedForcedGenCompactionTs;

        TVector<TGeneration> Generations;
        TFinalState FinalState;
        TList<TPartInfo> FinalParts;
        TList<TIntrusiveConstPtr<TColdPart>> ColdParts;
        THashMap<TLogoBlobID, ui32> KnownParts;
        TStats Stats;
        THashMap<ui64, TStats> StatsPerTablet;
        NPage::TGarbageStatsAgg GarbageStatsAgg;

        bool CompactBorrowedGarbageAllowed = false;
    };

}
}
}
