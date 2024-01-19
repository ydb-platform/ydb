#include "flat_part_charge_range.h"
#include "flat_part_charge_create.h"

namespace NKikimr::NTable {

bool ChargeRange(IPages *env, const TCells key1, const TCells key2,
            const TRun &run, const TKeyCellDefaults &keyDefaults, TTagsRef tags,
            ui64 items, ui64 bytes, bool includeHistory) noexcept
{
    if (run.size() == 1) {
        auto pos = run.begin();
        TRowId row1 = pos->Slice.BeginRowId();
        TRowId row2 = pos->Slice.EndRowId() - 1;
        return CreateCharge(env, *pos->Part, tags, includeHistory)->Do(key1, key2, row1, row2, keyDefaults, items, bytes).Ready;
    }

    bool ready = true;
    auto pos = run.LowerBound(key1);

    if (pos == run.end())
        return true;

    bool fromStart = TSlice::CompareSearchKeyFirstKey(key1, pos->Slice, keyDefaults) <= 0;

    while (pos != run.end()) {
        TRowId row1 = pos->Slice.BeginRowId();
        TRowId row2 = pos->Slice.EndRowId() - 1;

        const int cmp = TSlice::CompareLastKeySearchKey(pos->Slice, key2, keyDefaults);

        TArrayRef<const TCell> key1r;
        if (!fromStart) {
            key1r = key1;
        }
        TArrayRef<const TCell> key2r;
        if (cmp > 0 /* slice->LastKey > key2 */) {
            key2r = key2;
        }

        auto r = CreateCharge(env, *pos->Part, tags, includeHistory)->Do(key1r, key2r, row1, row2, keyDefaults, items, bytes);
        ready &= r.Ready;

        if (cmp >= 0 /* slice->LastKey >= key2 */) {
            if (r.Overshot && ++pos != run.end()) {
                // Unfortunately key > key2 might be at the start of the next slice
                TRowId firstRow = pos->Slice.BeginRowId();
                // Precharge the first row on the next slice
                CreateCharge(env, *pos->Part, tags, includeHistory)->Do(firstRow, firstRow, keyDefaults, items, bytes);
            }

            break;
        }

        // Will consume this slice before encountering key2
        fromStart = true;
        ++pos;
    }

    return ready;
}

bool ChargeRangeReverse(IPages *env, const TCells key1, const TCells key2,
            const TRun &run, const TKeyCellDefaults &keyDefaults, TTagsRef tags,
            ui64 items, ui64 bytes, bool includeHistory) noexcept
{
    if (run.size() == 1) {
        auto pos = run.begin();
        TRowId row1 = pos->Slice.EndRowId() - 1;
        TRowId row2 = pos->Slice.BeginRowId();
        return CreateCharge(env, *pos->Part, tags, includeHistory)->DoReverse(key1, key2, row1, row2, keyDefaults, items, bytes).Ready;
    }

    bool ready = true;
    auto pos = run.LowerBoundReverse(key1);

    if (pos == run.end())
        return true;

    bool fromEnd = TSlice::CompareLastKeySearchKey(pos->Slice, key1, keyDefaults) <= 0;

    for (;;) {
        TRowId row1 = pos->Slice.EndRowId() - 1;
        TRowId row2 = pos->Slice.BeginRowId();

        // N.B. empty key2 is like -inf during reverse iteration
        const int cmp = key2 ? TSlice::CompareSearchKeyFirstKey(key2, pos->Slice, keyDefaults) : -1;

        TArrayRef<const TCell> key1r;
        if (!fromEnd) {
            key1r = key1;
        }
        TArrayRef<const TCell> key2r;
        if (cmp > 0 /* key2 > slice->FirstKey */) {
            key2r = key2;
        }

        auto r = CreateCharge(env, *pos->Part, tags, includeHistory)->DoReverse(key1r, key2r, row1, row2, keyDefaults, items, bytes);
        ready &= r.Ready;

        if (pos == run.begin()) {
            break;
        }

        if (cmp >= 0 /* key2 >= slice->FirstKey */) {
            if (r.Overshot) {
                --pos;
                // Unfortunately key <= key2 might be at the end of the previous slice
                TRowId lastRow = pos->Slice.EndRowId() - 1;
                // Precharge the last row on the previous slice
                CreateCharge(env, *pos->Part, tags, includeHistory)->DoReverse(lastRow, lastRow, keyDefaults, items, bytes);
            }

            break;
        }

        // Will consume this slice before encountering key2
        fromEnd = true;
        --pos;
    }

    return ready;
}

}
