#pragma once

#include "tablet_counters.h"

#include <ydb/core/base/tablet_types.h>

namespace NKikimr {

THolder<TTabletCountersBase> CreateAppCountersByTabletType(TTabletTypes::EType type);

}
