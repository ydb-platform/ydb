#include "flat_part_slice.h"
#include "util_fmt_desc.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr {
namespace NTable {

////////////////////////////////////////////////////////////////////////////////

namespace {

void PrintCells(IOutputStream& out, TArrayRef<const TCell> cells, const TCellDefaults& cellDefaults) noexcept
{
    out << '{';
    size_t pos = 0;
    for (const TCell& cell : cells) {
        if (pos != 0) {
            out << ", ";
        }
        TString value;
        DbgPrintValue(value, cell, cellDefaults.Types[pos++]);
        out << value;
    }
    out << '}';
}

bool ValidateSlices(TConstArrayRef<TSlice> slices) noexcept
{
    ui64 last = 0;
    for (const TSlice& slice : slices) {
        if (slice.EndRowId() <= slice.BeginRowId()) {
            return false;
        }
        if (slice.BeginRowId() < last) {
            return false;
        }
        last = slice.EndRowId();
    }
    return true;
}

}

////////////////////////////////////////////////////////////////////////////////

int ComparePartKeys(TCellsRef left, TCellsRef right, const TKeyCellDefaults &keyDefaults) noexcept {
    size_t end = Max(left.size(), right.size());
    Y_DEBUG_ABORT_UNLESS(end <= keyDefaults.Size(), "Key schema is smaller than compared keys");

    for (size_t pos = 0; pos < end; ++pos) {
        const auto& leftCell = pos < left.size() ? left[pos] : keyDefaults.Defs[pos];
        const auto& rightCell = pos < right.size() ? right[pos] : keyDefaults.Defs[pos];
        if (int cmp = CompareTypedCells(leftCell, rightCell, keyDefaults.Types[pos])) {
            return cmp;
        }
    }

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

void TBounds::Describe(IOutputStream& out, const TKeyCellDefaults& keyDefaults) const noexcept
{
    auto left = FirstKey.GetCells();
    auto right = LastKey.GetCells();
    out << (FirstInclusive ? '[' : '(');
    if (left) {
        PrintCells(out, left, keyDefaults);
    } else {
        out << "-inf";
    }
    out << ", ";
    if (right) {
        PrintCells(out, right, keyDefaults);
    } else {
        out << "+inf";
    }
    out << (LastInclusive ? ']' : ')');
}

bool TBounds::LessByKey(const TBounds& a, const TBounds& b, const TKeyCellDefaults& keyDefaults) noexcept
{
    auto left = a.LastKey.GetCells();
    auto right = b.FirstKey.GetCells();
    if (Y_UNLIKELY(!left)) {
        // Empty LastKey is +inf-epsilon => +inf-epsilon < any is never true
        Y_DEBUG_ABORT_UNLESS(!a.LastInclusive,
            "Unexpected inclusion of +inf: %s", NFmt::Ln(a, keyDefaults).data());
        return false;
    }
    if (Y_UNLIKELY(!right)) {
        // Empty FirstKey is -inf => any < -inf is never true
        Y_DEBUG_ABORT_UNLESS(b.FirstInclusive,
            "Unexpected exclusion of -inf: %s", NFmt::Ln(b, keyDefaults).data());
        return false;
    }
    size_t end = Max(left.size(), right.size());
    Y_DEBUG_ABORT_UNLESS(end <= keyDefaults.Size(), "Key schema is smaller than slice boundary keys");
    for (size_t pos = 0; pos < end; ++pos) {
        const auto& leftCell = pos < left.size() ? left[pos] : keyDefaults[pos];
        const auto& rightCell = pos < right.size() ? right[pos] : keyDefaults[pos];
        if (int cmp = CompareTypedCells(leftCell, rightCell, keyDefaults.Types[pos])) {
            return cmp < 0;
        }
    }
    // Both ends are the same row, a < b only if one is non-inclusive
    return !a.LastInclusive || !b.FirstInclusive;
}

int TBounds::CompareSearchKeyFirstKey(
        TArrayRef<const TCell> key,
        const TBounds& bounds,
        const TKeyCellDefaults& keyDefaults) noexcept
{
    if (!key) {
        // Search key is +inf => +inf > any
        return +1;
    }
    auto right = bounds.FirstKey.GetCells();
    if (Y_UNLIKELY(!right)) {
        Y_DEBUG_ABORT_UNLESS(bounds.FirstInclusive,
            "Unexpected exclusion of -inf: %s", NFmt::Ln(bounds, keyDefaults).data());
        // Empty FirstKey is -inf => any > -inf
        return +1;
    }
    Y_DEBUG_ABORT_UNLESS(key.size() <= keyDefaults.Size(),
        "Key schema is smaller than the search key");
    for (size_t pos = 0; pos < key.size(); ++pos) {
        const auto& leftCell = key[pos];
        const auto& rightCell = pos < right.size() ? right[pos] : keyDefaults[pos];
        if (int cmp = CompareTypedCells(leftCell, rightCell, keyDefaults.Types[pos])) {
            return cmp;
        }
    }
    if (key.size() < keyDefaults.Size()) {
        // Search key is extended with +inf => +inf > any
        return +1;
    }
    // Both keys are equal, it depends on first key inclusion
    return bounds.FirstInclusive ? 0 : -1;
}

int TBounds::CompareLastKeySearchKey(
        const TBounds& bounds,
        TArrayRef<const TCell> key,
        const TKeyCellDefaults& keyDefaults) noexcept
{
    auto left = bounds.LastKey.GetCells();
    if (Y_UNLIKELY(!left)) {
        Y_DEBUG_ABORT_UNLESS(!bounds.LastInclusive,
            "Unexpected inclusion of +inf: %s", NFmt::Ln(bounds, keyDefaults).data());
        // Empty LastKey is +inf-epsilon
        // +inf-epsilon > any,
        // +inf-epsilon < +inf
        return key ? +1 : -1;
    }
    if (!key) {
        // Search key is +inf => any < +inf
        return -1;
    }
    Y_DEBUG_ABORT_UNLESS(key.size() <= keyDefaults.Size(),
        "Key schema is smaller than the search key");
    for (size_t pos = 0; pos < key.size(); ++pos) {
        const auto& leftCell = pos < left.size() ? left[pos] : keyDefaults[pos];
        const auto& rightCell = key[pos];
        if (int cmp = CompareTypedCells(leftCell, rightCell, keyDefaults.Types[pos])) {
            return cmp;
        }
    }
    if (key.size() < keyDefaults.Size()) {
        // Search key is extended with +inf => any < +inf
        return -1;
    }
    // Both keys are equal, it depends on last key inclusion
    return bounds.LastInclusive ? 0 : -1;
}

////////////////////////////////////////////////////////////////////////////////

void TSlice::Describe(IOutputStream& out) const noexcept
{
    out << (FirstInclusive ? '[' : '(');
    out << FirstRowId;
    out << ", ";
    if (LastRowId != Max<TRowId>()) {
        out << LastRowId;
    } else {
        out << "+inf";
    }
    out << (LastInclusive ? ']' : ')');
}

void TSlice::Describe(IOutputStream& out, const TKeyCellDefaults& keyDefaults) const
{
    out << "{rows: ";
    Describe(out);
    out << " keys: ";
    TBounds::Describe(out, keyDefaults);
    out << "}";
}

void TSlices::Describe(IOutputStream& out) const noexcept
{
    bool first = true;
    out << "{ ";
    for (const auto& bounds : *this) {
        if (first)
            first = false;
        else
            out << ", ";
        bounds.Describe(out);
    }
    out << (first ? "}" : " }");
}

void TSlices::Describe(IOutputStream& out, const TKeyCellDefaults& keyDefaults) const
{
    bool first = true;
    out << "{ ";
    for (const auto& bounds : *this) {
        if (first)
            first = false;
        else
            out << ", ";
        bounds.Describe(out, keyDefaults);
    }
    out << (first ? "}" : " }");
}

void TSlices::Validate() const noexcept
{
    TRowId lastEnd = 0;
    for (const auto& bounds : *this) {
        TRowId begin = bounds.BeginRowId();
        TRowId end = bounds.EndRowId();
        Y_ABORT_UNLESS(std::exchange(lastEnd, end) <= begin,
            "Slices not sorted or have intersections, search may not work correctly");
        Y_ABORT_UNLESS(begin < end,
            "Sanity check: slice [%" PRIu64 ",%" PRIu64 ") has no rows, search may not work correctly",
            begin, end);
        if (!bounds.FirstKey.GetCells()) {
            Y_ABORT_UNLESS(bounds.FirstInclusive, "Sanity check: slice has FirstKey == -inf, but it is not included");
            Y_ABORT_UNLESS(bounds.FirstRowId == 0, "Sanity check: slice has FirstKey == -inf, but FirstRowId != 0");
        }
        if (!bounds.LastKey.GetCells()) {
            Y_ABORT_UNLESS(!bounds.LastInclusive, "Sanity check: slice has LastKey == +inf, but it is included");
            Y_ABORT_UNLESS(bounds.LastRowId == Max<TRowId>(), "Sanity check: slice has LastKey == +inf, but LastRowId != +inf");
        } else {
            Y_ABORT_UNLESS(bounds.LastRowId != Max<TRowId>(), "Sanity check: slice has LastRowId == +inf, but LastKey != +inf");
        }
    }
}

TIntrusiveConstPtr<TScreen> TSlices::ToScreen() const noexcept
{
    TVector<TScreen::THole> holes;
    auto it = IterateRowRanges();
    while (it) {
        holes.push_back(*it);
        ++it;
    }
    holes.shrink_to_fit();
    return new TScreen(std::move(holes));
}

bool TSlices::EqualByRowId(const TIntrusiveConstPtr<TSlices>& a, const TIntrusiveConstPtr<TSlices>& b) noexcept
{
    if (!a || !b) {
        return !a == !b;
    }
    auto ait = a->IterateRowRanges();
    auto bit = b->IterateRowRanges();
    while (ait && bit) {
        if (!(*ait == *bit)) {
            return false;
        }
        ++ait;
        ++bit;
    }
    return bool(ait) == bool(bit);
}

bool TSlices::SupersetByRowId(const TIntrusiveConstPtr<TSlices>& a, const TIntrusiveConstPtr<TSlices>& b) noexcept
{
    if (!a || !b) {
        return !a == !b;
    }
    auto ait = a->IterateRowRanges();
    auto bit = b->IterateRowRanges();
    if (!bit) {
        return true; // empty is a subset of anything
    }
    if (!ait) {
        return false; // empty cannot be a superset of non-empty
    }
    for (;;) {
        while (ait->End <= bit->Begin) {
            if (!++ait) {
                return false; // b has rows that are not in a
            }
        }
        // b must be contained within a
        if (!(ait->Begin <= bit->Begin && bit->End <= ait->End)) {
            return false; // b has rows that are not in a
        }
        if (!++bit) {
            return true; // b doesn't have any more rows
        }
    }
}

TIntrusiveConstPtr<TSlices> TSlices::Subtract(
        const TIntrusiveConstPtr<TSlices>& a,
        const TIntrusiveConstPtr<TSlices>& b) noexcept
{
    if (!a || a->empty() || !b || b->empty()) {
        return a; // there's nothing to remove
    }

    auto ait = a->begin();
    auto bit = b->begin();
    auto left = *ait++;
    auto right = *bit++;
    TVector<TSlice> result;

    while (true) {
        if (right.EndRowId() <= left.BeginRowId()) {
            // right is exhausted, skip to the next slice
            if (left.FirstRowId < right.LastRowId) {
                // handle an important edge case:
                // right = [k1, k3)
                // left = (k2, k4]
                // then if k2 < k3 we must use k3 inclusive
                // N.B.: when switching to k3 number of rows isn't changing
                // should only be possible when right.end == left.begin
                Y_DEBUG_ABORT_UNLESS(right.EndRowId() == left.BeginRowId());
                left.FirstKey = right.LastKey;
                left.FirstRowId = right.LastRowId;
                left.FirstInclusive = !right.LastInclusive;
            }
            if (bit == b->end()) {
                // just copy everything that's left
                result.push_back(std::move(left));
                result.insert(result.end(), ait, a->end());
                break;
            }
            right = *bit++;
            continue;
        }
        if (left.EndRowId() <= right.BeginRowId()) {
            // left should be copied completely
            if (right.FirstRowId < left.LastRowId) {
                // handle an important edge case:
                // left = [k1, k3)
                // right = (k2, k4]
                // then if k2 < k3 we must use k2 inclusive
                // N.B.: when switching to k2 number of rows isn't changing
                // should only be possible when left.end == right.begin
                Y_DEBUG_ABORT_UNLESS(left.EndRowId() == right.BeginRowId());
                left.LastKey = right.FirstKey;
                left.LastRowId = right.FirstRowId;
                left.LastInclusive = !right.FirstInclusive;
            }
            result.push_back(std::move(left));
            if (ait == a->end()) {
                break;
            }
            left = *ait++;
            continue;
        }
        if (left.BeginRowId() < right.BeginRowId()) {
            // we have something at the beginning of left
            auto& part = result.emplace_back();
            part.FirstKey = left.FirstKey;
            part.FirstRowId = left.FirstRowId;
            part.FirstInclusive = left.FirstInclusive;
            part.LastKey = right.FirstKey;
            part.LastRowId = right.FirstRowId;
            part.LastInclusive = !right.FirstInclusive;
        }
        if (left.EndRowId() <= right.EndRowId()) {
            // everything else in left is removed
            if (ait == a->end()) {
                break;
            }
            left = *ait++;
        } else {
            // trim left to the end of right and repeat
            left.FirstKey = right.LastKey;
            left.FirstRowId = right.LastRowId;
            left.FirstInclusive = !right.LastInclusive;
        }
    }

    result.shrink_to_fit();
    return new TSlices(std::move(result));
}

TIntrusiveConstPtr<TSlices> TSlices::Merge(
        const TIntrusiveConstPtr<TSlices>& a,
        const TIntrusiveConstPtr<TSlices>& b) noexcept
{
    if (!b || b->empty()) {
        return a;
    }
    if (!a || a->empty()) {
        return b;
    }

    TVector r(Reserve(a->size() + b->size()));

    auto mergeLast = [&r](const TSlice& slice) {
        if (r.empty() || TSlice::LessByRowId(r.back(), slice)) {
            // There is no intersection, just append a new slice
            r.emplace_back(slice);
            return;
        }
        auto& last = r.back();
        Y_ABORT_UNLESS(!TSlice::LessByFirstRowId(slice, last), "Invalid merge order");
        if (last.LastRowId < slice.LastRowId ||
            last.LastRowId == slice.LastRowId && !last.LastInclusive && slice.LastInclusive)
        {
            // Extend up to the end of a new slice
            last.LastKey = slice.LastKey;
            last.LastRowId = slice.LastRowId;
            last.LastInclusive = slice.LastInclusive;
        }
    };

    auto ait = a->begin();
    auto bit = b->begin();
    while (ait != a->end() && bit != b->end()) {
        if (TSlice::LessByFirstRowId(*ait, *bit)) {
            mergeLast(*ait++);
        } else {
            mergeLast(*bit++);
        }
    }
    while (ait != a->end()) {
        mergeLast(*ait++);
    }
    while (bit != b->end()) {
        mergeLast(*bit++);
    }

    return new TSlices(std::move(r));
}

TIntrusiveConstPtr<TSlices> TSlices::Cut(
        TIntrusiveConstPtr<TSlices> run,
        TRowId beginRowId,
        TRowId endRowId,
        TConstArrayRef<TCell> beginKey,
        TConstArrayRef<TCell> endKey) noexcept
{
    if (!run || run->empty()) {
        return run;
    }
    if (Y_UNLIKELY(endRowId <= beginRowId)) {
        // Empty range will always result in an empty run
        return new TSlices;
    }
    auto begin = run->begin();
    auto end = run->end();
    while (begin != end) {
        if (begin->LastRowId < beginRowId ||
            begin->LastRowId == beginRowId && !begin->LastInclusive)
        {
            ++begin;
        } else {
            break;
        }
    }
    while (begin != end) {
        auto prev = end - 1;
        if (prev->FirstRowId >= endRowId ||
            prev->FirstRowId == endRowId-1 && !prev->FirstInclusive)
        {
            end = prev;
        } else {
            break;
        }
    }
    if (begin == run->begin() &&
        end == run->end() &&
        run->front().FirstRowId >= beginRowId &&
        (run->back().LastRowId < endRowId ||
            run->back().LastRowId == endRowId && !run->back().LastInclusive))
    {
        // No modifications necessary
        return run;
    }
    TIntrusivePtr<TSlices> result = new TSlices;
    result->reserve(end - begin);
    result->insert(result->end(), begin, end);
    if (!result->empty()) {
        auto& first = result->front();
        if (first.FirstRowId < beginRowId) {
            first.FirstKey = TSerializedCellVec(beginKey);
            first.FirstRowId = beginRowId;
            first.FirstInclusive = true;
        }
        auto& last = result->back();
        if (last.LastRowId > endRowId) {
            last.LastKey = TSerializedCellVec(endKey);
            last.LastRowId = endRowId;
            last.LastInclusive = false;
        } else if (last.LastRowId == endRowId && last.LastInclusive) {
            last.LastInclusive = false;
        }
    }
    return result;
}

TIntrusiveConstPtr<TSlices> TSlices::Replace(TIntrusiveConstPtr<TSlices> run, TConstArrayRef<TSlice> slices) noexcept
{
    Y_ABORT_UNLESS(run && !run->empty());
    Y_ABORT_UNLESS(slices);

    TVector<TSlice> result(Reserve(run->size() - 1 + slices.size()));

    Y_ABORT_UNLESS(ValidateSlices(*run), "TSlices::Replace got invalid source slices");
    Y_ABORT_UNLESS(ValidateSlices(slices), "TSlices::Replace got invalid new slices");

    auto it = run->begin();
    auto next = slices.begin();
    TSlicesRowsIterator removed(slices);

    while (it != run->end() && removed) {
        if (it->BeginRowId() < removed->Begin) {
            result.emplace_back(*it++);
            continue;
        }

        // Remove slices matching the full removed range
        ui64 first = it->BeginRowId();
        Y_ABORT_UNLESS(it->BeginRowId() == removed->Begin,
            "Cannot remove range [%" PRIu64 ",%" PRIu64 ") -- found slice [%" PRIu64 ",%" PRIu64 ")",
            removed->Begin, removed->End,
            it->BeginRowId(), it->EndRowId());
        ui64 last = (it++)->EndRowId();
        while (it != run->end() && it->EndRowId() <= removed->End) {
            Y_ABORT_UNLESS(last == it->BeginRowId(),
                "Cannot remove range [%" PRIu64 ",%" PRIu64 ") -- found range [%" PRIu64 ",%" PRIu64 ") and slice [%" PRIu64 ",%" PRIu64 ")",
                removed->Begin, removed->End,
                first, last,
                it->BeginRowId(), it->EndRowId());
            last = (it++)->EndRowId();
        }
        Y_ABORT_UNLESS(last == removed->End,
            "Cannot remove range [%" PRIu64 ",%" PRIu64 ") -- found range [%" PRIu64 ",%" PRIu64 ")",
            removed->Begin, removed->End,
            first, last);

        // Add slices matching the full removed range
        while (next != slices.end() && next->EndRowId() <= removed->End) {
            result.emplace_back(*next++);
        }

        ++removed;
    }

    Y_ABORT_UNLESS(!removed,
        "Cannot remove range [%" PRIu64 ",%" PRIu64 ") -- out of source slices",
        removed->Begin, removed->End);

    Y_ABORT_UNLESS(next == slices.end(),
        "Cannot process slice [%" PRIu64 ",%" PRIu64 ") -- rows out of sync",
        next->BeginRowId(), next->EndRowId());

    while (it != run->end()) {
        result.emplace_back(*it++);
    }

    Y_ABORT_UNLESS(ValidateSlices(result), "TSlices::Replace produced invalid slices");

    result.shrink_to_fit();
    return new TSlices(std::move(result));
}

////////////////////////////////////////////////////////////////////////////////

TLevels::iterator TLevels::AddLevel()
{
    size_t index = Levels.size();
    Levels.emplace_front(*KeyCellDefaults, index);
    return Levels.begin();
}

TLevels::TAddResult TLevels::Add(TIntrusiveConstPtr<TPart> part, const TSlice& slice)
{
    Y_VERIFY_S(part->Epoch >= MaxEpoch,
            "Adding part " << part->Label.ToString() << " (epoch " << part->Epoch << ") to levels with max epoch " << MaxEpoch);
    MaxEpoch = part->Epoch;

    iterator insertLevel = Levels.end();
    TRun::iterator insertHint;
    for (iterator it = Levels.begin(); it != Levels.end(); ++it) {
        auto r = it->FindInsertHint(part.Get(), slice);
        if (!r.second) {
            // Slice cannot fall through any further
            break;
        }
        insertLevel = it;
        insertHint = r.first;
    }
    if (insertLevel == Levels.end()) {
        // No suitable level found, create a new one on top
        insertLevel = AddLevel();
        insertHint = insertLevel->end();
    }
    size_t before = insertLevel->size();
    auto pos = insertLevel->Insert(insertHint, std::move(part), slice);
    size_t after = insertLevel->size();
    Y_DEBUG_ABORT_UNLESS(after == before + 1, "Slice was not inserted at a suitable level");

    return TAddResult{ insertLevel, pos };
}

void TLevels::Add(TIntrusiveConstPtr<TPart> part, const TIntrusiveConstPtr<TSlices>& run)
{
    for (const auto& slice : *run) {
        Add(part, slice);
    }
}

void TLevels::AddContiguous(TIntrusiveConstPtr<TPart> part, const TIntrusiveConstPtr<TSlices>& run)
{
    if (run->empty()) {
        return;
    }

    Y_VERIFY_S(part->Epoch >= MaxEpoch,
            "Adding part " << part->Label.ToString() << " (epoch " << part->Epoch << ") to levels with max epoch " << MaxEpoch);
    MaxEpoch = part->Epoch;

    TSlice all;
    all.FirstKey = run->front().FirstKey;
    all.FirstRowId = run->front().FirstRowId;
    all.FirstInclusive = run->front().FirstInclusive;
    all.LastKey = run->back().LastKey;
    all.LastRowId = run->back().LastRowId;
    all.LastInclusive = run->back().LastInclusive;

    iterator insertLevel = Levels.end();
    TRun::iterator insertHint;
    for (iterator it = Levels.begin(); it != Levels.end(); ++it) {
        auto r = it->FindInsertHint(nullptr, all);
        if (!r.second) {
            // We cannot fall through any further
            break;
        }
        insertLevel = it;
        insertHint = r.first;
    }

    if (insertLevel == Levels.end()) {
        // No suitable level found, create a new one on top
        insertLevel = AddLevel();
        insertHint = insertLevel->end();
    }

    size_t before = insertLevel->size();
    for (const auto& slice : *run) {
        insertLevel->Insert(insertHint, part, slice);
    }
    size_t after = insertLevel->size();
    Y_DEBUG_ABORT_UNLESS(after == before + run->size(), "Slices were not inserted at a suitable level");
}

////////////////////////////////////////////////////////////////////////////////

}
}
