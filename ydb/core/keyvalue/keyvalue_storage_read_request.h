#pragma once
#include "defs.h"
#include "keyvalue_events.h"

#include <util/generic/ptr.h>
#include <util/system/types.h>

namespace NKikimr {
namespace NKeyValue {

IActor* CreateKeyValueStorageReadRequest(
    THolder<TIntermediate>&& intermediate,
    const TTabletStorageInfo *tabletInfo,
    ui32 tabletGeneration);

} // NKeyValue
} // NKikimr
