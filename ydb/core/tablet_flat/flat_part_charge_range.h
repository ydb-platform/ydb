#pragma once

#include "flat_part_iface.h"
#include "flat_part_slice.h"

namespace NKikimr::NTable {

namespace {
    using TCells = NPage::TCells;
}

struct TChargeResult {
    /**
     * If set to true, all the necessary pages are already in memory.
     */
    bool Ready;

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

TChargeResult ChargeRange(IPages *env, const TCells key1, const TCells key2,
            const TRun &run, const TKeyCellDefaults &keyDefaults, TTagsRef tags,
            ui64 items, ui64 bytes, bool includeHistory);

TChargeResult ChargeRangeReverse(IPages *env, const TCells key1, const TCells key2,
            const TRun &run, const TKeyCellDefaults &keyDefaults, TTagsRef tags,
            ui64 items, ui64 bytes, bool includeHistory);

}
