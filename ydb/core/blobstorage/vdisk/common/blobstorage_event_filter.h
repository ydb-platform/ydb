#pragma once

#include "defs.h"

#include <library/cpp/actors/interconnect/event_filter.h>

namespace NKikimr {

    void RegisterBlobStorageEventScopes(const std::shared_ptr<NActors::TEventFilter>& filter);

} // NKikimr
