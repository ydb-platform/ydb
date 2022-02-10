#pragma once
#include "defs.h"
#include "keyvalue_intermediate.h"

namespace NKikimr {
namespace NKeyValue {

IActor* CreateKeyValueStorageRequest(THolder<TIntermediate>&& intermediate,
        const TTabletStorageInfo *tabletInfo);

} // NKeyValue
} // NKikimr
