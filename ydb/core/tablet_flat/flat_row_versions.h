#pragma once

#include <ydb/core/base/row_version.h>

#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NTable {

    class TRowVersionRanges {
        struct TItem {
            TRowVersion Lower; // Inclusive
            TRowVersion Upper; // Non-inclusive
        };

        struct TMergeLower {
            TRowVersion Lower; // Desired lower bound
        };

        struct TMergeUpper {
            TRowVersion Upper; // Desired upper bound
        };

        struct TCompare {
            typedef void is_transparent;

            bool operator()(const TItem& a, const TItem& b) const {
                // Returns true when a < b (and a does not intersect b)
                return a.Upper <= b.Lower;
            }

            bool operator()(const TItem& a, const TMergeLower& b) const {
                // Returns true when a < b (and a cannot be merged with b)
                return a.Upper < b.Lower;
            }

            bool operator()(const TMergeUpper& a, const TItem& b) const {
                // Returns true when a < b (and a cannot be merged with b)
                return a.Upper < b.Lower;
            }
        };

        using TItems = TSet<TItem, TCompare>;

    public:
        using const_iterator = TItems::const_iterator;
        using iterator = const_iterator;
        using value_type = TItem;

        const_iterator begin() const { return Items.begin(); }
        const_iterator end() const { return Items.end(); }

        size_t size() const {
            return Items.size();
        }

        bool empty() const {
            return Items.empty();
        }

        explicit operator bool() const {
            return !empty();
        }

        /**
         * Adds [lower, upper) to version ranges
         *
         * Returns false if this range already exists
         */
        bool Add(const TRowVersion& lower, const TRowVersion& upper) {
            if (Y_UNLIKELY(!(lower < upper))) {
                return false; // empty range cannot be added
            }

            // Find the first range we could potentially merge with
            // That is immediately after range where it->Upper < lower is true
            auto it = Items.lower_bound(TMergeLower{ lower });

            // Case 1: cannot be merged, insert as the last range
            // Case 2: cannot be merged, insert just before it
            if (it == Items.end() || upper < it->Lower) {
                Items.insert(it, TItem{ lower, upper });
                return true;
            }

            // We know that it and [lower, upper) can be merged (lower <= it->Upper)
            // We have to find the first item we cannot merge (upper < end->Lower)
            auto end = Items.upper_bound(TMergeUpper{ upper });
            Y_DEBUG_ABORT_UNLESS(end != it); // sanity check

            // Becomes true when we make any changes
            bool changed = false;

            // See if we're spanning over multiple existing ranges
            auto begin = std::next(it);
            if (begin != end) {
                // Remember the upper bound
                TRowVersion last = std::prev(end)->Upper;
                // Erase all items except it
                Items.erase(begin, end);
                // It's now safe to update the upper bound
                const_cast<TItem&>(*it).Upper = last;
                changed = true;
            }

            if (lower < it->Lower) {
                const_cast<TItem&>(*it).Lower = lower;
                changed = true;
            }

            if (it->Upper < upper) {
                const_cast<TItem&>(*it).Upper = upper;
                changed = true;
            }

            return changed;
        }

        /**
         * Returns true if ranges contain [lower, upper)
         */
        bool Contains(const TRowVersion& lower, const TRowVersion& upper) const {
            if (Y_UNLIKELY(upper < lower)) {
                return false; // ignore invalid ranges
            }

            auto it = Items.lower_bound(TMergeLower{ lower });
            if (it == Items.end() || lower < it->Lower || it->Upper < upper) {
                return false;
            }

            return true;
        }

        /**
         * Searches for item [lower, upper) where lower <= rowVersion <= upper
         *
         * Returns lower when found, original rowVersion otherwise
         */
        TRowVersion AdjustDown(TRowVersion rowVersion) const {
            auto it = Items.lower_bound(TMergeLower{ rowVersion });
            if (it == Items.end() || rowVersion < it->Lower) {
                return rowVersion;
            }

            // We know that rowVersion <= it->Upper
            return it->Lower;
        }

        TString ToString() const;

    public:
        /**
         * Helper for walking in a skewed btree over some flat array
         *
         * 1) every 4 consecutive elements form a node with 4 implied children
         * 2) underlying array stores elements in their bfs order over a tree
         * 3) the last element in a node is max for a subtree (no right child)
         * 4) the last node may have less than 4 elements (and no children)
         * 5) implied children don't exist if their index is out of bounds
         *
         * The result is a cache oblivious data structure, optimized for lower bound search.
         *
         * When used with TRowVersion each node uses 16 x 4 = 64 bytes, i.e. a single cache line.
         */
        class TSteppedCookieAllocator {
        public:
            explicit TSteppedCookieAllocator(size_t size)
                : Size(size)
                , Index(LeftMostChild(0, size))
            {
            }

            static size_t Parent(size_t index) {
                // node = index / 4
                // parent node = (node - 1) / 4
                // parent index = (parent node) * 4 + node % 4
                // most of these multiplications and divisions go away
                return (index >> 2) - 1;
            }

            static size_t LeftChild(size_t index) {
                // node = index / 4
                // child node = node * 4 + 1 + (index % 4)
                // child index = (child node) * 4
                // most of these multiplications and divisions go away
                return (index + 1) << 2;
            }

            static bool IsRoot(size_t index) {
                return (index >> 2) == 0;
            }

            static bool IsLastChild(size_t index) {
                return (index & 3) == 3;
            }

            static size_t LeftMostChild(size_t index, size_t size) {
                for (;;) {
                    size_t left = LeftChild(index);
                    if (left >= size) {
                        return index;
                    }
                    index = left;
                }
            }

            static size_t FindNextIndex(size_t index, size_t size) {
                Y_DEBUG_ABORT_UNLESS(index < size);

                if (!IsLastChild(index) && (index + 1) < size) {
                    // Go to the next item in current node
                    return LeftMostChild(index + 1, size);
                }

                // Go to the next item in parent node
                return Parent(index);
            }

            struct TSimpleLess {
                typedef void is_transparent;

                template<class T1, class T2>
                inline bool operator()(const T1& a, const T2& b) const {
                    return a < b;
                }
            };

            template<class TContainer, class TNeedle, class TCmp = TSimpleLess>
            static size_t LowerBound(const TContainer& items, const TNeedle& needle, const TCmp& cmp = TCmp()) {
                size_t found = items.size();

                size_t node = 0;
                while (node < items.size()) {
                    for (size_t index = 0; index <= 4; ++index) {
                        if (index == 4 || node + index == items.size()) {
                            return found; // end of search
                        }

                        if (!cmp(items[node + index], needle)) {
                            found = node + index;
                            node = LeftChild(node + index);
                            break;
                        }
                    }
                }

                return found;
            }

            explicit operator bool() const {
                return Index < Size;
            }

            size_t Current() const {
                return Index;
            }

            void MoveNext() {
                Index = FindNextIndex(Index, Size);
            }

        private:
            const size_t Size;
            size_t Index;
        };

    public:
        /**
         * TSnapshot is a compact read-only snapshot of TRowVersionRanges
         */
        class TSnapshot {
        public:
            TSnapshot() = default;

            explicit TSnapshot(const TRowVersionRanges& ranges) {
                if (ranges) {
                    // Construct a vector optimized for binary search
                    Lower.resize(ranges.size());
                    Upper.resize(ranges.size());
                    TSteppedCookieAllocator steppedCookieAllocator(ranges.size());
                    for (const auto& item : ranges) {
                        Y_DEBUG_ABORT_UNLESS(steppedCookieAllocator);
                        Lower[steppedCookieAllocator.Current()] = item.Lower;
                        Upper[steppedCookieAllocator.Current()] = item.Upper;
                        steppedCookieAllocator.MoveNext();
                    }
                    Y_DEBUG_ABORT_UNLESS(!steppedCookieAllocator);
                }
            }

            explicit operator bool() const {
                return !Upper.empty();
            }

            /**
             * Similar to TRowVersionRanges::AdjustDown
             */
            TRowVersion AdjustDown(const TRowVersion& rowVersion) const {
                size_t index = TSteppedCookieAllocator::LowerBound(Upper, rowVersion);
                if (index == Upper.size()) {
                    return rowVersion;
                }

                const auto& lower = Lower[index];
                if (rowVersion < lower) {
                    return rowVersion;
                }

                // We know that rowVersion <= upper
                return lower;
            }

        private:
            TVector<TRowVersion> Lower;
            TVector<TRowVersion> Upper;
        };

        /**
         * Makes a read-only snapshot of version ranges
         */
        TSnapshot Snapshot() const {
            return TSnapshot(*this);
        }

    private:
        TItems Items;
    };

}
}
