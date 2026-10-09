#pragma once

#include "flat_table_committed.h"

#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <algorithm>

namespace NKikimr {
namespace NTable {

    /**
     * Closed ranges of savepoint seq nums of a transaction that were rolled back.
     * Ranges are kept sorted, non-overlapping and non-adjacent.
     */
    class TSavepointSeqNumRanges {
    public:
        struct TRange {
            ui32 From;
            ui32 To;

            bool operator==(const TRange&) const = default;
        };

        /**
         * What a single Add changed: the merged range was inserted at Index in
         * place of the Replaced ranges. Usually holds just a few ranges, so it's
         * much cheaper to keep for rollback than a copy of the whole set.
         */
        struct TAddUndo {
            bool Changed = false;
            size_t Index = 0;
            TVector<TRange> Replaced;
        };

    public:
        /**
         * Adds [from, to], returns true when the set has changed.
         * Fills undo, if provided, with what is needed to revert this Add.
         */
        bool Add(ui32 from, ui32 to, TAddUndo* undo = nullptr) {
            Y_ENSURE(from <= to, "Invalid savepoint seq num range [" << from << ", " << to << "]");

            // The first range that ends at or after from - 1, i.e. may overlap or touch the new one
            auto it = std::lower_bound(Ranges.begin(), Ranges.end(), from,
                [](const TRange& range, ui32 value) {
                    return ui64(range.To) + 1 < value;
                });

            if (it != Ranges.end() && it->From <= from && to <= it->To) {
                // Already covered
                if (undo) {
                    undo->Changed = false;
                }
                return false;
            }

            // Merge all ranges that overlap or touch [from, to]
            auto last = it;
            while (last != Ranges.end() && last->From <= ui64(to) + 1) {
                from = Min(from, last->From);
                to = Max(to, last->To);
                ++last;
            }

            if (undo) {
                undo->Changed = true;
                undo->Index = it - Ranges.begin();
                undo->Replaced.assign(it, last);
            }

            it = Ranges.erase(it, last);
            Ranges.insert(it, TRange{ from, to });
            return true;
        }

        /**
         * Reverts an Add, undos must be applied in the reverse order of their Adds
         */
        void Undo(const TAddUndo& undo) {
            if (!undo.Changed) {
                return;
            }

            Y_ENSURE(undo.Index < Ranges.size(), "Savepoint seq num ranges undo is out of bounds");
            auto it = Ranges.erase(Ranges.begin() + undo.Index);
            Ranges.insert(it, undo.Replaced.begin(), undo.Replaced.end());
        }

        /**
         * Adds all ranges of other, returns true when the set has changed
         */
        bool Add(const TSavepointSeqNumRanges& other) {
            bool changed = false;
            for (const auto& range : other.Ranges) {
                changed |= Add(range.From, range.To);
            }
            return changed;
        }

        bool Contains(ui32 seqNum) const {
            auto it = std::lower_bound(Ranges.begin(), Ranges.end(), seqNum,
                [](const TRange& range, ui32 value) {
                    return range.To < value;
                });
            return it != Ranges.end() && it->From <= seqNum;
        }

        bool Empty() const {
            return Ranges.empty();
        }

        const TVector<TRange>& GetRanges() const {
            return Ranges;
        }

        bool operator==(const TSavepointSeqNumRanges&) const = default;

        friend IOutputStream& operator<<(IOutputStream& out, const TSavepointSeqNumRanges& ranges) {
            out << "{";
            bool first = true;
            for (const auto& range : ranges.Ranges) {
                out << (first ? " " : ", ") << "[" << range.From << ", " << range.To << "]";
                first = false;
            }
            return out << " }";
        }

    private:
        TVector<TRange> Ranges;
    };

    /**
     * Removed operations (savepoint seq num ranges) by TxId, see TDatabase::RemoveTxOps
     */
    using TRemovedTxOps = absl::flat_hash_map<ui64, TSavepointSeqNumRanges>;

    /**
     * A simple copy-on-write wrapper for TRemovedTxOps, so iterators and
     * subsets may keep a consistent snapshot while the table changes
     */
    class TRemovedTxOpsMap {
    private:
        struct TState final : public TThrRefBase {
            TRemovedTxOps Ops;
        };

    public:
        using const_iterator = TRemovedTxOps::const_iterator;

    public:
        TRemovedTxOpsMap() = default;

        explicit operator bool() const {
            return State_ && !State_->Ops.empty();
        }

        const TSavepointSeqNumRanges* Find(ui64 txId) const {
            if (State_) {
                auto it = State_->Ops.find(txId);
                if (it != State_->Ops.end()) {
                    return &it->second;
                }
            }
            return nullptr;
        }

        /**
         * Returns true when the operation of txId with savepointSeqNum is removed
         */
        bool Contains(ui64 txId, ui32 savepointSeqNum) const {
            const auto* ranges = Find(txId);
            return ranges && ranges->Contains(savepointSeqNum);
        }

        /**
         * Returns ranges of txId for modification, adds empty ranges when missing
         */
        TSavepointSeqNumRanges& Mutable(ui64 txId) {
            return Unshare().Ops[txId];
        }

        bool Erase(ui64 txId) {
            if (State_ && State_->Ops.contains(txId)) {
                Unshare().Ops.erase(txId);
                return true;
            } else {
                return false;
            }
        }

        size_t Size() const {
            return State_ ? State_->Ops.size() : 0;
        }

    public:
        const_iterator begin() const {
            if (State_) {
                const TState& state = *State_;
                return state.Ops.begin();
            } else {
                return { };
            }
        }

        const_iterator end() const {
            if (State_) {
                const TState& state = *State_;
                return state.Ops.end();
            } else {
                return { };
            }
        }

    private:
        TState& Unshare() {
            if (!State_) {
                State_ = MakeIntrusive<TState>();
            } else if (State_->RefCount() > 1) {
                State_ = MakeIntrusive<TState>(*State_);
            }
            return *State_;
        }

    private:
        TIntrusivePtr<TState> State_;
    };

    /**
     * A transaction map that additionally skips removed operations
     */
    class TRemovedTxOpsTransactionMap final : public ITransactionMap {
    private:
        TRemovedTxOpsTransactionMap(ITransactionMapPtr base, TRemovedTxOpsMap removed)
            : Base(std::move(base))
            , Removed(std::move(removed))
        { }

    public:
        const TRowVersion* Find(ui64 txId) const override {
            return Base.Find(txId);
        }

        bool IsSkippedSavepointSeqNum(ui64 txId, ui32 savepointSeqNum) const override {
            return Removed.Contains(txId, savepointSeqNum)
                || Base.IsSkippedSavepointSeqNum(txId, savepointSeqNum);
        }

        /**
         * Returns base unchanged when there are no removed operations
         */
        static ITransactionMapPtr Create(ITransactionMapPtr base, const TRemovedTxOpsMap& removed) {
            if (!removed) {
                return base;
            }
            return new TRemovedTxOpsTransactionMap(std::move(base), removed);
        }

    private:
        const ITransactionMapPtr Base;
        const TRemovedTxOpsMap Removed;
    };

}
}
