#pragma once
#include "defs.h"
#include "keyvalue_events.h"

namespace NKikimr {
namespace NKeyValue {

IActor* CreateKeyValueStorageReadRequest(THolder<TIntermediate>&& intermediate,
        const TTabletStorageInfo *tabletInfo);

} // NKeyValue
} // NKikimr
