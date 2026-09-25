#pragma once

#include "tablet_counters.h"

#include <ydb/core/base/tablet_types.h>

namespace NKikimr {

std::unique_ptr<TTabletCountersBase> CreateAppCountersByTabletType(TTabletTypes::EType type);

}
