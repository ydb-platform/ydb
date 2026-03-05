#pragma once

#include "common.h"

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

namespace NMVP::NSupportLinks {

struct TResolvedLink {
    TString Title;
    TString Url;
};

struct TSupportError {
    TString Source;
    TMaybe<ui32> Status;
    TString Reason;
    TString Message;
};

} // namespace NMVP::NSupportLinks

namespace NMVP::NSupportLinks::TEvPrivate {

enum EEv {
    EvSourceResponse = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "Event space for support links is exhausted");

struct TEvSourceResponse : NActors::TEventLocal<TEvSourceResponse, EvSourceResponse> {
    size_t Place = 0;
    TVector<TResolvedLink> Links;
    TVector<TSupportError> Errors;

    TEvSourceResponse(size_t place, TVector<TResolvedLink>&& links, TVector<TSupportError>&& errors)
        : Place(place)
        , Links(std::move(links))
        , Errors(std::move(errors))
    {}
};

} // namespace NMVP::NSupportLinks::TEvPrivate
