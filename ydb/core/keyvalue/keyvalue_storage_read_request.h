#pragma once
#include "defs.h"
#include "keyvalue_events.h"
#include <util/generic/ptr.h>
#include <util/system/types.h>

#include <memory>

namespace NKikimr {
namespace NKeyValue {

class TKeyValueState;
struct TKeyValueStateLifetimeToken;

IActor* CreateKeyValueStorageReadRequest(
    THolder<TIntermediate>&& intermediate,
    const TTabletStorageInfo *tabletInfo,
    ui32 tabletGeneration,
    TKeyValueState *state,
    std::weak_ptr<TKeyValueStateLifetimeToken> stateLifetimeToken);

} // NKeyValue
} // NKikimr
