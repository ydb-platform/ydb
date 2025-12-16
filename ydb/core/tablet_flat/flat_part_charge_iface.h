#pragma once

#include "flat_page_base.h"

namespace NKikimr::NTable {

    struct ICharge {
        using TCells = NPage::TCells;

        struct TResult {
            bool Ready;     /* All required pages are already in memory */
            bool Overshot;  /* Search may start outside of bounds */

            /**
             * The total number of rows precharged.
             *
             * @warning The value in this field will be set to an accurate value
             *          only if the Ready field is set to true.
             *
             * @warning The value in this field is meant to be used as an approximation
             *          of the total size of all items placed into the cache.
             *          It may not be very accurate in some cases.
             */
            ui64 ItemsPrecharged;

            /**
             * The total number of bytes precharged.
             *
             * @warning The value in this field will be set to an accurate value
             *          only if the Ready field is set to true.
             *
             * @warning The value in this field is meant to be used as an approximation
             *          of the total size of all items placed into the cache.
             *          It may not be very accurate in some cases.
             */
            ui64 BytesPrecharged;
        };

        /**
         * Precharges data for rows between row1 and row2 inclusive
         *
         * Important caveat: assumes iteration won't touch any row > row2
         */
        TResult Do(TRowId row1, TRowId row2,
                const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const
        {
            return Do(TCells{}, TCells{}, row1, row2,
                keyDefaults, itemsLimit, bytesLimit);
        }

        /**
         * Precharges data for rows between row1 and row2 inclusive in reverse
         *
         * Important caveat: assumes iteration won't touch any row > row2
         */
        TResult DoReverse(TRowId row1, TRowId row2,
                const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const
        {
            return DoReverse(TCells{}, TCells{}, row1, row2,
                keyDefaults, itemsLimit, bytesLimit);
        }

        /**
         * Precharges data for rows between max(key1, row1) and min(key2, row2) inclusive
         */
        virtual TResult Do(const TCells key1, const TCells key2, TRowId row1, TRowId row2,
                const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const = 0;

        /**
         * Precharges data for rows between min(key1, row1) and max(key2, row2) inclusive in reverse
         */
        virtual TResult DoReverse(const TCells key1, const TCells key2, TRowId row1, TRowId row2,
                const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const = 0;

        virtual ~ICharge() = default;
};

}
