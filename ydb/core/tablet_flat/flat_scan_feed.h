#pragma once

#include "flat_scan_iface.h"
#include "flat_scan_lead.h"
#include "flat_part_iface.h"
#include "flat_iterator.h"
#include "flat_table_subset.h"

namespace NKikimr {
namespace NTable {

    class TFeed {
    public:
        TFeed(IScan *scan, const TSubset &subset, TRowVersion snapshot = TRowVersion::Max())
            : Scan(scan)
            , Subset(subset)
            , SnapshotVersion(snapshot)
            , VersionScan(dynamic_cast<IVersionScan*>(scan))
        {

        }

        void Resume(EScan op) noexcept
        {
            Y_DEBUG_ABORT_UNLESS(op == EScan::Feed || op == EScan::Reset);

            OnPause = false;

            if (op == EScan::Reset) {
                Iter = nullptr;
            }
        }

        EReady Process() noexcept
        {
            if (OnPause) {
                return EReady::Page;
            } else if (Iter == nullptr) {
                Lead.Clear();

                if (Seeks == Max<ui64>()) {
                    Seeks = 0;
                }

                auto op = Scan->Seek(Lead, Seeks);
                VersionState = EVersionState::BeginKey;

                OnPause = (op == EScan::Sleep);

                switch (op) {
                    case EScan::Feed:
                        if (!Reset()) {
                            return EReady::Page;
                        }
                        return EReady::Data;

                    case EScan::Sleep:
                        if (Lead) {
                            Reset();
                        }
                        return EReady::Page;

                    case EScan::Final:
                        return EReady::Gone;

                    case EScan::Reset:
                        Y_ABORT("Unexpected EScan::Reset from IScan::Seek(...)");
                }

                Y_ABORT("Unexpected EScan result from IScan::Seek(...)");
            } else if (Seek()) {
                return NotifyPageFault();
            } else {
                EReady ready;

                switch (VersionState) {
                    case EVersionState::BeginKey:
                        ready = Iter->Next(Conf.NoErased ? ENext::Data : VersionScan ? ENext::Uncommitted : ENext::All);
                        Skipped += std::exchange(Iter->Stats.DeletedRowSkips, 0);
                        break;

                    case EVersionState::SkipUncommitted:
                        Y_DEBUG_ABORT_UNLESS(VersionScan);
                        ready = Iter->SkipUncommitted();
                        switch (ready) {
                            case EReady::Page:
                                break;
                            case EReady::Data:
                                if (Iter->IsUncommitted()) {
                                    VersionState = EVersionState::FeedDelta;
                                } else {
                                    VersionState = EVersionState::EndDeltasThenFeed;
                                }
                                break;
                            case EReady::Gone:
                                VersionState = EVersionState::EndDeltasAndKey;
                                ready = EReady::Data;
                                break;
                        }
                        break;

                    case EVersionState::SkipVersion:
                        Y_DEBUG_ABORT_UNLESS(VersionScan);
                        ready = Iter->SkipToRowVersion(NextRowVersion);
                        switch (ready) {
                            case EReady::Page:
                                break;
                            case EReady::Data:
                                VersionState = EVersionState::Feed;
                                break;
                            case EReady::Gone:
                                VersionState = EVersionState::EndKey;
                                ready = EReady::Data;
                                break;
                        }
                        break;

                    // These are callback states and don't affect the iterator
                    case EVersionState::BeginDeltas:
                    case EVersionState::FeedDelta:
                    case EVersionState::EndDeltasThenFeed:
                    case EVersionState::EndDeltasAndKey:
                    case EVersionState::Feed:
                    case EVersionState::EndKey:
                        Y_DEBUG_ABORT_UNLESS(VersionScan);
                        ready = EReady::Data;
                        break;
                }

                if (ready == EReady::Page) {
                    return NotifyPageFault();
                } else if (ready == EReady::Gone) {
                    return NotifyExhausted();
                } else {
                    EScan op;
                    switch (VersionState) {
                        case EVersionState::BeginKey:
                            if (!VersionScan) {
                                Seen++;
                                op = Scan->Feed(Iter->GetKey().Cells(), Iter->Row());
                            } else {
                                Y_DEBUG_ABORT_UNLESS(!Conf.NoErased, "VersionScan with Conf.NoErased == true is unexpected");
                                op = VersionScan->BeginKey(Iter->GetKey().Cells());
                                if (Iter->IsUncommitted()) {
                                    VersionState = EVersionState::BeginDeltas;
                                } else {
                                    VersionState = EVersionState::Feed;
                                }
                            }
                            break;

                        case EVersionState::BeginDeltas:
                            Y_DEBUG_ABORT_UNLESS(Iter->IsUncommitted());
                            op = VersionScan->BeginDeltas();
                            VersionState = EVersionState::FeedDelta;
                            break;

                        case EVersionState::FeedDelta: {
                            Seen++;
                            Y_DEBUG_ABORT_UNLESS(Iter->IsUncommitted());
                            ui64 txId = Iter->GetUncommittedTxId();
                            if (!Subset.RemovedTransactions.Contains(txId)) {
                                op = VersionScan->Feed(Iter->Row(), txId);
                            } else {
                                op = EScan::Feed;
                            }
                            VersionState = EVersionState::SkipUncommitted;
                            break;
                        }

                        case EVersionState::EndDeltasThenFeed:
                            op = VersionScan->EndDeltas();
                            VersionState = EVersionState::Feed;
                            break;

                        case EVersionState::EndDeltasAndKey:
                            op = VersionScan->EndDeltas();
                            VersionState = EVersionState::EndKey;
                            break;

                        case EVersionState::Feed: {
                            Seen++;
                            TRowVersion rowVersion = Iter->GetRowVersion();
                            op = VersionScan->Feed(Iter->Row(), rowVersion);
                            if (rowVersion) {
                                VersionState = EVersionState::SkipVersion;
                                NextRowVersion = --rowVersion;
                            } else {
                                VersionState = EVersionState::EndKey;
                            }
                            break;
                        }

                        case EVersionState::EndKey:
                            op = VersionScan->EndKey();
                            VersionState = EVersionState::BeginKey;
                            break;

                        case EVersionState::SkipUncommitted:
                            Y_ABORT("Unexpected callback state SkipUncommitted");
                        case EVersionState::SkipVersion:
                            Y_ABORT("Unexpected callback state SkipVersion");
                    }

                    OnPause = (op == EScan::Sleep);

                    switch (op) {
                        case EScan::Feed:
                            return EReady::Data;

                        case EScan::Sleep:
                            return EReady::Page;

                        case EScan::Reset:
                            // Will trigger seeking code again
                            Iter = nullptr;
                            return EReady::Data;

                        case EScan::Final:
                            return EReady::Gone;
                    }

                    Y_ABORT("Unexpected EScan result from IScan::Feed(...)");
                }
            }
        }

    protected:
        IScan* DetachScan() noexcept
        {
            return std::exchange(const_cast<IScan*&>(Scan), nullptr);
        }

        bool IsPaused() const noexcept
        {
            return OnPause;
        }

        EReady ImplicitPageFault() noexcept
        {
            if (Iter) {
                // Implicit page fault during iteration
                return NotifyPageFault();
            }

            // Implicit page fault while waiting for iterator, which may
            // happen after EScan::Reset at a very unlucky time. We have
            // to wait for a proper chance to call Seek() above.
            return EReady::Data;
        }

    private:
        virtual IPages* MakeEnv() noexcept = 0;

        virtual TPartView LoadPart(const TIntrusiveConstPtr<TColdPart>& part) noexcept = 0;

        EReady NotifyPageFault() noexcept
        {
            EScan op = Scan->PageFault();

            OnPause = (op == EScan::Sleep);

            switch (op) {
                case EScan::Feed:
                case EScan::Sleep:
                    return EReady::Page;

                case EScan::Reset:
                    Iter = nullptr;
                    return EReady::Data;

                case EScan::Final:
                    return EReady::Gone;
            }

            Y_ABORT("Unexpected EScan result from IScan::PageFault(...)");
        }

        EReady NotifyExhausted() noexcept
        {
            Iter = nullptr;

            EScan op = Scan->Exhausted();

            OnPause = (op == EScan::Sleep);

            switch (op) {
                case EScan::Reset:
                    return EReady::Data;

                case EScan::Sleep:
                    return EReady::Page;

                case EScan::Final:
                    return EReady::Gone;

                case EScan::Feed:
                    Y_ABORT("Unexpected EScan::Feed from IScan::Exhausted(...)");
            }

            Y_ABORT("Unexpected EScan result from IScan::Exhausted(...)");
        }

        bool Reset() noexcept
        {
            Seeks++;

            Y_ABORT_UNLESS(Lead, "Cannot seek with invalid lead");

            auto keyDefaults = Subset.Scheme->Keys;

            Y_ABORT_UNLESS(Lead.Key.GetCells().size() <= keyDefaults->Size(), "TLead key is too large");

            Iter = new TTableIter(Subset.Scheme.Get(), Lead.Tags, -1, SnapshotVersion, Subset.CommittedTransactions);

            CurrentEnv = MakeEnv();

            for (auto &mem: Subset.Frozen)
                Iter->Push(
                    TMemIter::Make(
                        *mem, mem.Snapshot, Lead.Key.GetCells(), Lead.Relation, keyDefaults, &Iter->Remap, CurrentEnv));

            if (Lead.StopKey.GetCells()) {
                if (Lead.StopKeyInclusive) {
                    Iter->StopAfter(Lead.StopKey.GetCells());
                } else {
                    Iter->StopBefore(Lead.StopKey.GetCells());
                }
            }

            if (!LoadColdParts()) {
                SeekState = ESeekState::LoadColdParts;
                return false;
            }

            PrepareBoots();
            SeekState = ESeekState::SeekBoots;
            return true;
        }

        bool LoadColdParts() noexcept
        {
            LoadedParts.clear();
            LoadingParts = 0;
            if (Subset.ColdParts) {
                for (const auto& part : Subset.ColdParts) {
                    auto partView = LoadPart(part);
                    if (partView) {
                        LoadedParts.push_back(std::move(partView));
                        continue;
                    }
                    ++LoadingParts;
                }
            }

            return LoadingParts == 0;
        }

        void PrepareBoots() noexcept
        {
            auto keyDefaults = Subset.Scheme->Keys;

            // Assume subset is immutable and only construct levels once
            if (!Levels && (Subset.Flatten || LoadedParts)) {
                Levels.Reset(new TLevels(keyDefaults));
                TVector<const TPartView*> parts;
                parts.reserve(Subset.Flatten.size() + LoadedParts.size());
                for (const auto& partView : Subset.Flatten) {
                    Y_ABORT_UNLESS(partView.Part, "Missing part in subset");
                    Y_ABORT_UNLESS(partView.Slices, "Missing part slices in subset");
                    parts.push_back(&partView);
                }
                for (const auto& partView : LoadedParts) {
                    Y_ABORT_UNLESS(partView.Part, "Missing part in subset");
                    Y_ABORT_UNLESS(partView.Slices, "Missing part slices in subset");
                    parts.push_back(&partView);
                }
                std::sort(parts.begin(), parts.end(),
                    [](const TPartView* a, const TPartView* b) {
                        return a->Epoch() < b->Epoch();
                    });
                for (const auto* partView : parts) {
                    Levels->AddContiguous(partView->Part, partView->Slices);
                }
            }

            Boots.clear();
            if (Levels) {
                Boots.reserve(Levels->size());
                for (auto &run: *Levels) {
                    Boots.push_back(
                        MakeHolder<TRunIter>(run, Lead.Tags, keyDefaults, CurrentEnv));
                }
            }
        }

        bool SeekBoots() noexcept
        {
            if (Boots) {
                auto saved = Boots.begin();
                auto current = Boots.begin();
                while (current != Boots.end()) {
                    auto *iter = current->Get();

                    switch (iter->Seek(Lead.Key.GetCells(), Lead.Relation)) {
                        case EReady::Page:
                            if (saved != current) {
                                *saved = std::move(*current);
                            }
                            ++saved;
                            ++current;
                            break;

                        case EReady::Data:
                            Iter->Push(std::move(*current));
                            ++current;
                            break;

                        case EReady::Gone:
                            ++current;
                            break;

                        default:
                            Y_ABORT("Unexpected Seek result");
                    }
                }

                if (saved != Boots.end()) {
                    Boots.erase(saved, Boots.end());
                }
            }

            return Boots.empty();
        }

        /**
         * @return true on page fault
         */
        bool Seek() noexcept
        {
            switch (SeekState) {
                case ESeekState::LoadColdParts:
                    if (!LoadColdParts()) {
                        return true;
                    }
                    SeekState = ESeekState::PrepareBoots;
                    [[fallthrough]];
                case ESeekState::PrepareBoots:
                    PrepareBoots();
                    SeekState = ESeekState::SeekBoots;
                    [[fallthrough]];
                case ESeekState::SeekBoots:
                    if (!SeekBoots()) {
                        return true;
                    }
                    SeekState = ESeekState::Finished;
                    [[fallthrough]];
                case ESeekState::Finished:
                    break;
            }

            Y_DEBUG_ABORT_UNLESS(LoadingParts == 0);
            Y_DEBUG_ABORT_UNLESS(Boots.empty());
            return false;
        }

    protected:
        IScan * const Scan;
        ui64 Seen = 0;
        ui64 Skipped = 0;
        IScan::TConf Conf;
        const TSubset &Subset;
        const TRowVersion SnapshotVersion;

    private:
        enum class ESeekState {
            LoadColdParts,
            PrepareBoots,
            SeekBoots,
            Finished,
        };

        enum class EVersionState {
            BeginKey,
            BeginDeltas,
            FeedDelta,
            SkipUncommitted,
            EndDeltasThenFeed,
            EndDeltasAndKey,
            Feed,
            SkipVersion,
            EndKey,
        };

        IVersionScan * const VersionScan;
        EVersionState VersionState = EVersionState::BeginKey;
        TRowVersion NextRowVersion;

    private:
        using TBoots = TVector<THolder<TRunIter>>;

        IPages* CurrentEnv = nullptr;

        TVector<TPartView> LoadedParts;
        size_t LoadingParts = 0;

        THolder<TLevels> Levels;
        TLead Lead;
        TBoots Boots;
        TAutoPtr<TTableIter> Iter;
        ui64 Seeks = Max<ui64>();
        ESeekState SeekState;
        bool OnPause = false;
    };

}
}
