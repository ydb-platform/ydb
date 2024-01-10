#pragma once

#include "flat_part_charge_iface.h"
#include "flat_part_iface.h"
#include "flat_part_slice.h"

namespace NKikimr::NTable {

THolder<ICharge> CreateCharge(IPages *env, const TPart &part, const TSlice& slice, TTagsRef tags, bool includeHistory = false);

}
