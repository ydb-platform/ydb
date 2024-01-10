#pragma once

#include "flat_page_base.h"
#include "ydb/core/tablet_flat/flat_part_slice.h"

namespace NKikimr::NTable {

    struct ICharge {
        using TCells = NPage::TCells;

        struct TResult {
            bool Ready;     /* All required pages are already in memory */
            bool Overshot;  /* Search may start outside of bounds */
        };

        ICharge(IPages *env, const TPart &part, const TSlice& slice)
            : Env(env)
            , Part(&part)
            , Slice(slice)
            , Scheme(*Part->Scheme)
        {
        }

        /**
         * Precharges data for rows between row1 and row2 inclusive
         *
         * Important caveat: given rows range should be within Slice
         */
        bool Do(TRowId row1, TRowId row2,
                const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept
        {
            if (Y_LIKELY(Slice.BeginRowId() <= row1 && row1 <= row2 && row2 < Slice.EndRowId())) {
                return Do(TCells{}, TCells{}, row1, row2,
                    keyDefaults, itemsLimit, bytesLimit).Ready;
            }
            
            Y_DEBUG_ABORT_UNLESS(Slice.BeginRowId() <= row1);
            Y_DEBUG_ABORT_UNLESS(row1 <= row2);
            Y_DEBUG_ABORT_UNLESS(row2 < Slice.EndRowId());
            return true;
        }

        /**
         * Precharges data for rows between row1 and row2 inclusive in reverse
         *
         * Important caveat: given rows range should be within Slice
         */
        bool DoReverse(TRowId row1, TRowId row2, 
                const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept
        {
            if (Y_LIKELY(Slice.EndRowId() > row1 && row1 >= row2 && row2 >= Slice.BeginRowId())) {
                return DoReverse(TCells{}, TCells{}, row1, row2,
                    keyDefaults, itemsLimit, bytesLimit).Ready;
            }
            
            Y_DEBUG_ABORT_UNLESS(Slice.EndRowId() > row1);
            Y_DEBUG_ABORT_UNLESS(row1 >= row2);
            Y_DEBUG_ABORT_UNLESS(row2 >= Slice.BeginRowId());
            return true;
        }

        /**
         * Precharges data for rows between key1 and key2 inclusive
         *
         * Important caveat: assumes iteration won't touch any rows outside of Slice
         */
        TResult Do(const TCells key1, const TCells key2,
                const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept
        {
            return Do(key1, key2, Slice.BeginRowId(), Slice.EndRowId() - 1,
                keyDefaults, itemsLimit, bytesLimit);
        }

        /**
         * Precharges data for rows between key1 and key2 inclusive in reverse
         *
         * Important caveat: assumes iteration won't touch any rows outside of Slice
         */
        TResult DoReverse(const TCells key1, const TCells key2,
                const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept
        {
            return DoReverse(key1, key2, Slice.EndRowId() - 1, Slice.BeginRowId(),
                keyDefaults, itemsLimit, bytesLimit);
        }

        virtual ~ICharge() = default;

    protected:
        /**
         * Precharges data for rows between max(key1, row1) and min(key2, row2) inclusive
         *
         * Important caveat: assumes iteration won't touch any rows outside of Slice
         */
        virtual TResult Do(const TCells key1, const TCells key2, TRowId row1, TRowId row2, 
                const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept = 0;

        /**
         * Precharges data for rows between min(key1, row1) and max(key2, row2) inclusive in reverse
         *
         * Important caveat: assumes iteration won't touch any rows outside of Slice
         */
        virtual TResult DoReverse(const TCells key1, const TCells key2, TRowId row1, TRowId row2, 
                const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept = 0;

    protected:
        IPages * const Env = nullptr;
        const TPart * const Part = nullptr;
        const TSlice& Slice;
        const TPartScheme &Scheme;
    };

}
