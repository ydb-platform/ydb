#include "flat_part_charge_range.h"
#include "flat_part_charge_create.h"

namespace NKikimr::NTable {

bool ChargeRange(IPages *env, const TCells key1, const TCells key2,
            const TRun &run, const TKeyCellDefaults &keyDefaults, TTagsRef tags,
            ui64 items, ui64 bytes, bool includeHistory) noexcept
{
    bool ready = true;
    auto pos = run.LowerBound(key1);

    if (pos == run.end())
        return true;

    // key1 <= FirstKey
    bool chargeFromSliceFirstRow = TSlice::CompareSearchKeyFirstKey(key1, pos->Slice, keyDefaults) <= 0;

    while (pos != run.end()) {
        TRowId row1 = pos->Slice.BeginRowId();
        TRowId row2 = pos->Slice.EndRowId() - 1;

        const int cmp = TSlice::CompareLastKeySearchKey(pos->Slice, key2, keyDefaults);

        TArrayRef<const TCell> key1r;
        if (!chargeFromSliceFirstRow) {
            key1r = key1;
        }
        TArrayRef<const TCell> key2r;
        if (cmp > 0) {
            // key2 < LastKey
            key2r = key2;
        }

        auto r = CreateCharge(env, *pos->Part, tags, includeHistory)->Do(key1r, key2r, row1, row2, keyDefaults, items, bytes);
        ready &= r.Ready;

        if (cmp >= 0) {
            // key2 <= LastKey
            if (r.Overshot && ++pos != run.end()) {
                // Unfortunately first key > key2 might be at the start of the next slice
                TRowId firstRow = pos->Slice.BeginRowId();
                // Precharge the first row main key on the next slice
                ready &= CreateCharge(env, *pos->Part, { }, false)->Do(firstRow, firstRow, keyDefaults, items, bytes);
            }

            break;
        }

        // Will consume this slice before encountering key2
        chargeFromSliceFirstRow = true;
        ++pos;
    }

    return ready;
}

bool ChargeRangeReverse(IPages *env, const TCells key1, const TCells key2,
            const TRun &run, const TKeyCellDefaults &keyDefaults, TTagsRef tags,
            ui64 items, ui64 bytes, bool includeHistory) noexcept
{
    bool ready = true;
    auto pos = run.LowerBoundReverse(key1);

    if (pos == run.end())
        return true;

    // LastKey <= key1
    bool chargeFromSliceLastRow = TSlice::CompareLastKeySearchKey(pos->Slice, key1, keyDefaults) <= 0;

    for (;;) {
        TRowId row1 = pos->Slice.EndRowId() - 1;
        TRowId row2 = pos->Slice.BeginRowId();

        // N.B. empty key2 is like -inf during reverse iteration
        const int cmp = key2 ? TSlice::CompareSearchKeyFirstKey(key2, pos->Slice, keyDefaults) : -1;

        TArrayRef<const TCell> key1r;
        if (!chargeFromSliceLastRow) {
            key1r = key1;
        }
        TArrayRef<const TCell> key2r;
        if (cmp > 0) {
            // FirstKey < key2
            key2r = key2;
        }

        auto r = CreateCharge(env, *pos->Part, tags, includeHistory)->DoReverse(key1r, key2r, row1, row2, keyDefaults, items, bytes);
        ready &= r.Ready;

        if (pos == run.begin()) {
            break;
        }

        if (cmp >= 0 /* key2 >= slice->FirstKey */) {
            // FirstKey <= key2
            if (r.Overshot) {
                --pos;
                // Unfortunately first key <= key2 might be at the end of the previous slice
                TRowId lastRow = pos->Slice.EndRowId() - 1;
                // Precharge the last row main key on the previous slice
                ready &= CreateCharge(env, *pos->Part, { }, false)->DoReverse(lastRow, lastRow, keyDefaults, items, bytes);
            }

            break;
        }

        // Will consume this slice before encountering key2
        chargeFromSliceLastRow = true;
        --pos;
    }

    return ready;
}

}
