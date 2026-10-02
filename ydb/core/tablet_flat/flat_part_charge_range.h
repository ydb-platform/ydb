#pragma once

#include "flat_part_iface.h"
#include "flat_part_slice.h"

namespace NKikimr::NTable {

namespace {
    using TCells = NPage::TCells;
}

TPrechargeResult ChargeRange(IPages *env, const TCells key1, const TCells key2,
            const TRun &run, const TKeyCellDefaults &keyDefaults, TTagsRef tags,
            ui64 items, ui64 bytes, bool includeHistory);

TPrechargeResult ChargeRangeReverse(IPages *env, const TCells key1, const TCells key2,
            const TRun &run, const TKeyCellDefaults &keyDefaults, TTagsRef tags,
            ui64 items, ui64 bytes, bool includeHistory);

}
