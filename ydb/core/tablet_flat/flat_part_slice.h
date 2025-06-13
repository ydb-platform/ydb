#pragma once

#include "flat_table_part.h"
#include "flat_part_screen.h"
#include "util_fmt_desc.h"

#include <util/generic/list.h>
#include <util/generic/set.h>

namespace NKikimr {
namespace NTable {

    /**
     * A very simple comparator for part keys (extended with schema defaults)
     */
    int ComparePartKeys(TCellsRef left, TCellsRef right, const TKeyCellDefaults &keyDefaults) noexcept;

    /**
     * Bounds for a range of keys
     */
    struct TBounds {
        TSerializedCellVec FirstKey; /* empty = -inf, always inclusive */
        TSerializedCellVec LastKey;  /* empty = +inf, always exclusive */
        bool FirstInclusive;
        bool LastInclusive;

        TBounds()
            : FirstInclusive(true)
            , LastInclusive(false)
        {
        }

        TBounds(TSerializedCellVec firstKey,
                TSerializedCellVec lastKey,
                bool firstInclusive,
                bool lastInclusive)
            : FirstKey(std::move(firstKey))
            , LastKey(std::move(lastKey))
            , FirstInclusive(firstInclusive)
            , LastInclusive(lastInclusive)
        {
        }

        void Describe(IOutputStream& out, const TKeyCellDefaults& keyDefaults) const noexcept;

        /**
         * Returns true if a is less than b without any intersections
         */
        static bool LessByKey(const TBounds& a, const TBounds& b, const TKeyCellDefaults& keyDefaults) noexcept;

        /**
         * Compares search key and bounds first key
         *
         * Missing search key cells are treated as +inf
         */
        static int CompareSearchKeyFirstKey(
                TArrayRef<const TCell> key,
                const TBounds& bounds,
                const TKeyCellDefaults& keyDefaults) noexcept;

        /**
         * Compares bounds last key and search key
         *
         * Missing search key cells are treated as +inf
         */
        static int CompareLastKeySearchKey(
                const TBounds& bounds,
                TArrayRef<const TCell> key,
                const TKeyCellDefaults& keyDefaults) noexcept;
    };

    /**
     * Slice of some specific part
     */
    struct TSlice : public TBounds {
        TRowId FirstRowId;
        TRowId LastRowId;

        TSlice()
            : FirstRowId(0)
            , LastRowId(Max<TRowId>())
        {
        }

        TSlice(TSerializedCellVec firstKey,
                         TSerializedCellVec lastKey,
                         TRowId firstRowId,
                         TRowId lastRowId,
                         bool firstInclusive,
                         bool lastInclusive)
            : TBounds(
                std::move(firstKey),
                std::move(lastKey),
                firstInclusive,
                lastInclusive)
            , FirstRowId(firstRowId)
            , LastRowId(lastRowId)
        {
        }

        TRowId BeginRowId() const noexcept
        {
            return FirstRowId + !FirstInclusive;
        }

        TRowId EndRowId() const noexcept
        {
            return LastRowId + LastInclusive;
        }

        TScreen::THole ToScreenHole() const noexcept
        {
            return { BeginRowId(), EndRowId() };
        }

        bool Has(TRowId rowId) const noexcept
        {
            return BeginRowId() <= rowId && rowId < EndRowId();
        }

        bool Has(TRowId begin, TRowId end) const noexcept
        {
            return BeginRowId() < end && begin < EndRowId();
        }

        ui64 Rows() const noexcept
        {
            if (LastRowId == Max<TRowId>()) {
                // True number of rows is unknown
                return Max<TRowId>();
            }
            return EndRowId() - BeginRowId();
        }

        void Describe(IOutputStream& out) const noexcept;
        void Describe(IOutputStream& out, const TKeyCellDefaults& keyDefaults) const;

        /**
         * Returns true if first row of a is less than first row of b
         */
        static bool LessByFirstRowId(const TSlice& a, const TSlice& b) noexcept
        {
            if (a.FirstRowId != b.FirstRowId) {
                // There's a special case e.g. for slices (1,3] and [2,3]
                // Technically they have the same real range, however their
                // first keys are not equal a.FirstKey+epsilon might be smaller
                // than b.FirstKey, so we treat such slices as non-equal
                return a.FirstRowId < b.FirstRowId;
            }
            // Both ends are the same row, a < b only if a is inclusive,
            // while b is not inclusive, e.g. [1,3] < (1,3]
            return a.FirstInclusive && !b.FirstInclusive;
        }

        /**
         * Returns true if a is less than b without any intersections
         */
        static bool LessByRowId(const TSlice& a, const TSlice& b) noexcept
        {
            if (a.LastRowId != b.FirstRowId) {
                // There's a special case e.g. for slices [1,4) and (3,6]
                // These slices are actually [1,3] and [4,6], but this
                // comparison treats them as intersecting. This case is
                // very unlikely and nothing should break even if we
                // treat such slices as intersecting. Moreover, if we look
                // at these slices from the standpoint of their corresponding
                // keys, then a.LastKey-epsilon < b.FirstKey+epsilon is almost
                // certainly not true, so they really do "intersect".
                return a.LastRowId < b.FirstRowId;
            }
            // Both ends are the same row, a < b only if one is non-inclusive
            return !a.LastInclusive || !b.FirstInclusive;
        }
    };

    class TScreenRowsIterator {
    public:
        explicit TScreenRowsIterator(TConstArrayRef<TScreen::THole> holes)
            : Pos(holes.begin())
            , End(holes.end())
            , Current(true)
        {
            MoveNext();
        }

        explicit TScreenRowsIterator(const TScreen& screen)
            : TScreenRowsIterator(FromScreen(screen))
        { }

        explicit operator bool() const
        {
            return Valid;
        }

        const TScreen::THole& operator*() const
        {
            Y_DEBUG_ABORT_UNLESS(Valid);
            return Current;
        }

        const TScreen::THole* operator->() const
        {
            Y_DEBUG_ABORT_UNLESS(Valid);
            return &Current;
        }

        TScreenRowsIterator& operator++()
        {
            Y_DEBUG_ABORT_UNLESS(Valid);
            MoveNext();
            return *this;
        }

        bool HasNext() const {
            return Pos != End;
        }

    private:
        void MoveNext() {
            if (Pos != End) {
                Current = *Pos;
                while (++Pos != End && Current.End == Pos->Begin) {
                    Current.End = Pos->End;
                }
                Valid = true;
            } else {
                Valid = false;
            }
        }

        static TConstArrayRef<TScreen::THole> FromScreen(const TScreen& screen) {
            if (screen.Size() > 0) {
                return { &*screen.begin(), screen.Size() };
            } else {
                return { };
            }
        }

    private:
        const TScreen::THole* Pos;
        const TScreen::THole* End;
        TScreen::THole Current;
        bool Valid;
    };

    class TSlicesRowsIterator {
    public:
        explicit TSlicesRowsIterator(TConstArrayRef<TSlice> slices)
            : Pos(slices.begin())
            , End(slices.end())
            , Current(true)
        {
            MoveNext();
        }

        explicit operator bool() const
        {
            return Valid;
        }

        const TScreen::THole& operator*() const
        {
            Y_DEBUG_ABORT_UNLESS(Valid);
            return Current;
        }

        const TScreen::THole* operator->() const
        {
            Y_DEBUG_ABORT_UNLESS(Valid);
            return &Current;
        }

        TSlicesRowsIterator& operator++()
        {
            Y_DEBUG_ABORT_UNLESS(Valid);
            MoveNext();
            return *this;
        }

        bool HasNext() const {
            return Pos != End;
        }

    private:
        void MoveNext() {
            if (Pos != End) {
                Current = Pos->ToScreenHole();
                while (++Pos != End && Current.End == Pos->BeginRowId()) {
                    Current.End = Pos->EndRowId();
                }
                Valid = true;
            } else {
                Valid = false;
            }
        }

    private:
        const TSlice* Pos;
        const TSlice* End;
        TScreen::THole Current;
        bool Valid;
    };

    /**
     * A sorted run of non-intersecting slices for the same part
     */
    struct TSlices final
        : public TAtomicRefCount<TSlices>
        , public TVector<TSlice>
    {
        using TBase = TVector<TSlice>;

        TSlices() = default;

        explicit TSlices(const TVector<TSlice>& slices)
            : TBase(slices)
        {
        }

        explicit TSlices(TVector<TSlice>&& slices)
            : TBase(std::move(slices))
        {
        }

        TSlicesRowsIterator IterateRowRanges() const noexcept
        {
            return TSlicesRowsIterator(*this);
        }

        void Describe(IOutputStream& out) const noexcept;

        void Describe(IOutputStream& out, const TKeyCellDefaults& keyDefaults) const;

        /**
         * Validate slices are correct, crash otherwise
         */
        void Validate() const noexcept;

        /**
         * Converts run to a matching screen
         */
        TIntrusiveConstPtr<TScreen> ToScreen() const noexcept;

        /**
         * Returns a special run that includes all possible rows
         *
         * Currently only used in tests when bounds are unavailable
         */
        static TIntrusiveConstPtr<TSlices> All() noexcept
        {
            TIntrusivePtr<TSlices> run = new TSlices;
            run->emplace_back();
            return run;
        }

        /**
         * Returns true if both a and b have matching row id ranges
         */
        static bool EqualByRowId(const TIntrusiveConstPtr<TSlices>& a, const TIntrusiveConstPtr<TSlices>& b) noexcept;

        /**
         * Returns true if a is a superset of b
         */
        static bool SupersetByRowId(const TIntrusiveConstPtr<TSlices>& a, const TIntrusiveConstPtr<TSlices>& b) noexcept;

        /**
         * Returns the result of removing b from a
         */
        static TIntrusiveConstPtr<TSlices> Subtract(const TIntrusiveConstPtr<TSlices>& a, const TIntrusiveConstPtr<TSlices>& b) noexcept;

        /**
         * Merges two sorted runs
         * Runs may intersect in which case they are merged
         */
        static TIntrusiveConstPtr<TSlices> Merge(const TIntrusiveConstPtr<TSlices>& a, const TIntrusiveConstPtr<TSlices>& b) noexcept;

        /**
         * Cuts run using [begin,end) range of row ids, with specified keys
         * Will only leave slices that potentially intersect with the range
         * Slices that are cut may be adjusted to the specified keys
         */
        static TIntrusiveConstPtr<TSlices> Cut(
                TIntrusiveConstPtr<TSlices> run,
                TRowId beginRowId,
                TRowId endRowId,
                TConstArrayRef<TCell> beginKey,
                TConstArrayRef<TCell> endKey) noexcept;

        /**
         * Replaces row ranges with new slices in the specified run
         */
        static TIntrusiveConstPtr<TSlices> Replace(TIntrusiveConstPtr<TSlices> run, TConstArrayRef<TSlice> slices) noexcept;

        /**
         * Walks backwards until the first potential intersection with [0, rowId] range
         */
        const_iterator LookupBackward(const_iterator it, TRowId rowId) const noexcept
        {
            auto cmp = [](TRowId rowId, const TSlice& bounds) {
                return rowId < bounds.BeginRowId();
            };
            if (it != end() && !cmp(rowId, *it)) {
                return it;
            }
            for (int linear = 0; linear < 4; ++linear) {
                if (it == begin() || !cmp(rowId, *--it)) {
                    return it;
                }
            }
            it = std::upper_bound(begin(), it, rowId, cmp);
            if (it != begin()) {
                --it;
            }
            return it;
        }

        /**
         * Walks forward until the first potential intersection with [rowId, +inf) range
         */
        const_iterator LookupForward(const_iterator it, TRowId rowId) const noexcept
        {
            auto cmp = [](const TSlice& bounds, TRowId rowId) {
                return bounds.EndRowId() <= rowId;
            };
            if (it == end() || !cmp(*it, rowId)) {
                return it;
            }
            for (int linear = 0; linear < 4; ++linear) {
                if (++it == end() || !cmp(*it, rowId)) {
                    return it;
                }
            }
            return std::lower_bound(it, end(), rowId, cmp);
        }

        /**
         * Starting from it finds closest slice that may contain rowId
         */
        const_iterator Lookup(const_iterator it, TRowId rowId) const noexcept
        {
            it = LookupBackward(it, rowId);
            it = LookupForward(it, rowId);
            return it;
        }

        /**
         * Starting from it finds closest slice that may intersect with [rowBegin,rowEnd)
         */
        const_iterator Lookup(const_iterator it, TRowId rowBegin, TRowId rowEnd) const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(rowBegin < rowEnd);
            it = LookupBackward(it, rowEnd - 1);
            it = LookupForward(it, rowBegin);
            return it;
        }
    };

    /**
     * Helper class filtering rows by slice bounds
     */
    class TSlicesRowFilter {
    public:
        TSlicesRowFilter()
            : Slices(nullptr)
        {
        }

        TSlicesRowFilter(TIntrusiveConstPtr<TSlices> slices)
            : Slices(std::move(slices))
        {
            if (Slices) {
                Current = Slices->begin();
            }
        }

        /**
         * Returns true if rowId is included by slice bounds
         */
        bool Has(TRowId rowId) const noexcept
        {
            if (!Slices || Current != Slices->end() && Current->Has(rowId)) {
                return true; // fast path
            }
            Current = Slices->Lookup(Current, rowId);
            return Current != Slices->end() && Current->Has(rowId);
        }

        bool Has(TRowId rowBegin, TRowId rowEnd) const noexcept
        {
            if (!Slices || Current != Slices->end() && Current->Has(rowBegin, rowEnd)) {
                return true; // fast path
            }
            Current = Slices->Lookup(Current, rowBegin, rowEnd);
            return Current != Slices->end() && Current->Has(rowBegin, rowEnd);
        }

        TIntrusiveConstPtr<TSlices> GetSlices() const noexcept
        {
            return Slices;
        }

    private:
        TIntrusiveConstPtr<TSlices> Slices;
        mutable TSlices::const_iterator Current;
    };

    /**
     * A sorted run of non-intersecting slices from multiple parts
     */
    class TRun {
        struct TItem {
            const TIntrusiveConstPtr<TPart> Part;
            const TSlice Slice;
        };

        struct TInsertKey {
            const TPart* Part;
            const TSlice& Slice;
        };

        struct TLowerBound {
            const TArrayRef<const TCell> Cells;

            explicit TLowerBound(TArrayRef<const TCell> cells)
                : Cells(cells)
            {
            }
        };

        struct TUpperBound {
            const TArrayRef<const TCell> Cells;

            explicit TUpperBound(TArrayRef<const TCell> cells)
                : Cells(cells)
            {
            }
        };

        struct TLowerBoundReverse {
            const TArrayRef<const TCell> Cells;

            explicit TLowerBoundReverse(TArrayRef<const TCell> cells)
                : Cells(cells)
            {
            }
        };

        struct TUpperBoundReverse {
            const TArrayRef<const TCell> Cells;

            explicit TUpperBoundReverse(TArrayRef<const TCell> cells)
                : Cells(cells)
            {
            }
        };

        struct TSearchRange {
            const TArrayRef<const TCell> From;
            const TArrayRef<const TCell> To;

            explicit TSearchRange(TArrayRef<const TCell> from, TArrayRef<const TCell> to)
                : From(from)
                , To(to)
            {
            }
        };

        struct TCompare {
            typedef void is_transparent;

            const TKeyCellDefaults& KeyCellDefaults;

            explicit TCompare(const TKeyCellDefaults& keyDefaults)
                : KeyCellDefaults(keyDefaults)
            {
            }

            Y_FORCE_INLINE bool operator()(const TItem& a, const TItem& b) const
            {
                if (a.Part.Get() == b.Part.Get()) {
                    return TSlice::LessByRowId(a.Slice, b.Slice);
                } else {
                    return TSlice::LessByKey(a.Slice, b.Slice, KeyCellDefaults);
                }
            }

            Y_FORCE_INLINE bool operator()(const TInsertKey& a, const TItem& b) const
            {
                if (a.Part == b.Part.Get()) {
                    return TSlice::LessByRowId(a.Slice, b.Slice);
                } else {
                    return TSlice::LessByKey(a.Slice, b.Slice, KeyCellDefaults);
                }
            }

            Y_FORCE_INLINE bool operator()(const TItem& a, const TInsertKey& b) const
            {
                if (a.Part.Get() == b.Part) {
                    return TSlice::LessByRowId(a.Slice, b.Slice);
                } else {
                    return TSlice::LessByKey(a.Slice, b.Slice, KeyCellDefaults);
                }
            }

            Y_FORCE_INLINE bool operator()(const TItem& a, const TLowerBound& key) const
            {
                // Returns true if a.LastKey < key
                return TSlice::CompareLastKeySearchKey(a.Slice, key.Cells, KeyCellDefaults) < 0;
            }

            Y_FORCE_INLINE bool operator()(const TUpperBound& key, const TItem& b) const
            {
                // Returns true if key < b.LastKey
                return TSlice::CompareLastKeySearchKey(b.Slice, key.Cells, KeyCellDefaults) > 0;
            }

            Y_FORCE_INLINE bool operator()(const TLowerBoundReverse& key, const TItem& b) const
            {
                // Returns true if key < b.FirstKey
                return TSlice::CompareSearchKeyFirstKey(key.Cells, b.Slice, KeyCellDefaults) < 0;
            }

            Y_FORCE_INLINE bool operator()(const TItem& a, const TUpperBoundReverse& key) const
            {
                // Returns true if a.FirstKey < key
                return TSlice::CompareSearchKeyFirstKey(key.Cells, a.Slice, KeyCellDefaults) > 0;
            }

            Y_FORCE_INLINE bool operator()(const TSearchRange& range, const TItem& b) const
            {
                return TSlice::CompareSearchKeyFirstKey(range.To, b.Slice, KeyCellDefaults) < 0;
            }

            Y_FORCE_INLINE bool operator()(const TItem& a, const TSearchRange& range) const
            {
                return TSlice::CompareLastKeySearchKey(a.Slice, range.From, KeyCellDefaults) < 0;
            }
        };

        using TItems = TSet<TItem, TCompare>;

    public:
        using const_iterator = TItems::const_iterator;
        using iterator = TItems::iterator;
        using value_type = TItem;

        explicit TRun(const TKeyCellDefaults& keyDefaults)
            : KeyCellDefaults(keyDefaults)
            , Slices({ }, TCompare(keyDefaults))
        {
        }

        // TRun cannot be copied or moved
        TRun(const TRun&) = delete;
        TRun(TRun&&) = delete;

        Y_FORCE_INLINE size_t size() const { return Slices.size(); }
        Y_FORCE_INLINE bool empty() const { return Slices.empty(); }

        Y_FORCE_INLINE const_iterator begin() const { return Slices.begin(); }
        Y_FORCE_INLINE const_iterator end() const { return Slices.end(); }

        Y_FORCE_INLINE iterator begin() { return Slices.begin(); }
        Y_FORCE_INLINE iterator end() { return Slices.end(); }

        std::pair<iterator, bool> Insert(TIntrusiveConstPtr<TPart> part, const TSlice& slice)
        {
            return Slices.insert(TItem{ std::move(part), slice });
        }

        iterator Insert(const_iterator hint, TIntrusiveConstPtr<TPart> part, const TSlice& slice)
        {
            return Slices.insert(hint, TItem{ std::move(part), slice });
        }

        void Erase(const_iterator pos)
        {
            Slices.erase(pos);
        }

        std::pair<const_iterator, bool> FindInsertHint(const TPart* part, const TSlice& slice) const
        {
            TInsertKey key = { part, slice };
            auto it = Slices.lower_bound(key);
            bool possible = it == Slices.end() || TCompare(KeyCellDefaults)(key, *it);
            return { it, possible };
        }

        /**
         * Returns iterator to a slice that may contain searchKey
         */
        const_iterator Find(TArrayRef<const TCell> searchKey) const
        {
            return Slices.find(TSearchRange(searchKey, searchKey));
        }

        /**
         * Returns iterator to the first slice that may contain key >= searchKey
         */
        const_iterator LowerBound(TArrayRef<const TCell> searchKey) const
        {
            return searchKey ? Slices.lower_bound(TLowerBound(searchKey)) : Slices.begin();
        }

        /**
         * Returns iterator to the first slice that may contain key > searchKey
         */
        const_iterator UpperBound(TArrayRef<const TCell> searchKey) const
        {
            return searchKey ? Slices.upper_bound(TUpperBound(searchKey)) : Slices.end();
        }

        /**
         * Returns iterator to the first (in reverse) slice that may contain key <= searchKey
         *
         * Will return end iterator if there are no slices like that
         */
        const_iterator LowerBoundReverse(TArrayRef<const TCell> searchKey) const
        {
            const_iterator it = searchKey ? Slices.upper_bound(TLowerBoundReverse(searchKey)) : Slices.end();
            if (it == Slices.begin()) {
                it = Slices.end();
            } else {
                --it;
            }
            return it;
        }

        /**
         * Returns iterator to the first (in reverse) slice that may contain key < searchKey
         *
         * Will return end iterator if there are no slices like that
         */
        const_iterator UpperBoundReverse(TArrayRef<const TCell> searchKey) const
        {
            const_iterator it = searchKey ? Slices.lower_bound(TUpperBoundReverse(searchKey)) : Slices.begin();
            if (it == Slices.begin()) {
                it = Slices.end();
            } else {
                --it;
            }
            return it;
        }

        /**
         * Returns a pair of [begin,end) iterators that may contain keys
         * in the inclusive [from,to] range
         */
        std::pair<const_iterator, const_iterator> EqualRange(
                TArrayRef<const TCell> from,
                TArrayRef<const TCell> to) const
        {
            return Slices.equal_range(TSearchRange(from, to));
        }

    private:
        const TKeyCellDefaults& KeyCellDefaults;
        TItems Slices;
    };

    /**
     * Maintains multiple intersecting levels of non-intersecting runs
     */
    class TLevels {
        class TItem : public TRun {
        public:
            TItem(const TKeyCellDefaults& keyDefaults, size_t index)
                : TRun(keyDefaults)
                , Index(index)
            { }

        public:
            // Index of the level, where 0 is at the bottom (oldest data)
            const size_t Index;
        };

        using TItems = TList<TItem>;

    public:
        using const_iterator = TItems::const_iterator;
        using iterator = TItems::iterator;
        using value_type = TItem;

        struct TAddResult {
            iterator Level;
            TRun::iterator Position;
        };

        explicit TLevels(TIntrusiveConstPtr<TKeyCellDefaults> keyDefaults)
            : KeyCellDefaults(std::move(keyDefaults))
        {
        }

        Y_FORCE_INLINE size_t size() const { return Levels.size(); }
        Y_FORCE_INLINE bool empty() const { return Levels.empty(); }

        Y_FORCE_INLINE const_iterator begin() const { return Levels.begin(); }
        Y_FORCE_INLINE const_iterator end() const { return Levels.end(); }

        Y_FORCE_INLINE iterator begin() { return Levels.begin(); }
        Y_FORCE_INLINE iterator end() { return Levels.end(); }

        TAddResult Add(TIntrusiveConstPtr<TPart> part, const TSlice& slice);

        void Add(TIntrusiveConstPtr<TPart> part, const TIntrusiveConstPtr<TSlices>& run);

        void AddContiguous(TIntrusiveConstPtr<TPart> part, const TIntrusiveConstPtr<TSlices>& run);

        TEpoch GetMaxEpoch() const {
            return MaxEpoch;
        }

    private:
        iterator AddLevel();

    private:
        TIntrusiveConstPtr<TKeyCellDefaults> KeyCellDefaults;
        TItems Levels;
        TEpoch MaxEpoch = TEpoch::Min();
    };

}
}
