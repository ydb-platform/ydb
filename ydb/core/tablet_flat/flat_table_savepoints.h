#pragma once

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

    public:
        /**
         * Adds [from, to], returns true when the set has changed
         */
        bool Add(ui32 from, ui32 to) {
            Y_ENSURE(from <= to, "Invalid savepoint seq num range [" << from << ", " << to << "]");

            // The first range that ends at or after from - 1, i.e. may overlap or touch the new one
            auto it = std::lower_bound(Ranges.begin(), Ranges.end(), from,
                [](const TRange& range, ui32 value) {
                    return ui64(range.To) + 1 < value;
                });

            if (it != Ranges.end() && it->From <= from && to <= it->To) {
                // Already covered
                return false;
            }

            // Merge all ranges that overlap or touch [from, to]
            auto last = it;
            while (last != Ranges.end() && last->From <= ui64(to) + 1) {
                from = Min(from, last->From);
                to = Max(to, last->To);
                ++last;
            }

            it = Ranges.erase(it, last);
            Ranges.insert(it, TRange{ from, to });
            return true;
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
     * Rolled back savepoint seq num ranges by TxId
     */
    using TRolledBackTxOps = absl::flat_hash_map<ui64, TSavepointSeqNumRanges>;

}
}
