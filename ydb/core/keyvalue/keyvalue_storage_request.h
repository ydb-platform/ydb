#pragma once
#include "defs.h"
#include "keyvalue_intermediate.h"
#include <util/generic/ptr.h>
#include <util/system/types.h>

#include <memory>

namespace NKikimr {
namespace NKeyValue {

class TKeyValueState;

IActor* CreateKeyValueStorageRequest(
    THolder<TIntermediate>&& intermediate,
    const TTabletStorageInfo *tabletInfo,
    ui32 tabletGeneration,
    std::weak_ptr<TKeyValueState> state);

} // NKeyValue
} // NKikimr
