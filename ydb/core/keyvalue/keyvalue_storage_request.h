#pragma once
#include "defs.h"
#include "keyvalue_intermediate.h"

#include <util/generic/ptr.h>
#include <util/system/types.h>

namespace NKikimr {
namespace NKeyValue {

IActor* CreateKeyValueStorageRequest(
    THolder<TIntermediate>&& intermediate,
    const TTabletStorageInfo *tabletInfo,
    ui32 tabletGeneration);

} // NKeyValue
} // NKikimr
