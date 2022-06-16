#pragma once

#include "wilson_trace.h"

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/log.h>

namespace NWilson {

    // stub for NBS
    template<typename TActorSystem>
    inline bool TraceEnabled(const TActorSystem&) {
        return false;
    }

    template<typename TActorSystem, typename TEvent>
    inline void TraceEvent(const TActorSystem&, TTraceId*, TEvent&&, TInstant)
    {}

} // NWilson
