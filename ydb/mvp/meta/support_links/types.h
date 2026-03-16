#pragma once

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NMVP::NSupportLinks {

struct TResolvedLink {
    // Optional display name shown in UI.
    TString Title;
    // Fully resolved link URL.
    TString Url;
};

struct TSupportError {
    // Source id that produced this error ("meta", "grafana/dashboard/search", etc.).
    TString Source;
    // Optional HTTP status returned by external system.
    TMaybe<ui32> Status;
    // Optional HTTP reason phrase returned by external system.
    TString Reason;
    // Human-readable error description.
    TString Message;
};

} // namespace NMVP::NSupportLinks

namespace NMVP {

// Per-source resolve state used by TSupportLinksResolver.
struct TResolveOutput {
    // Source id from config ("source" field), used in result/error reporting.
    TString Name;

    // false: source is still resolving.
    // true: source resolving is finished for this entry.
    bool Ready = false;

    // Links produced by this source.
    TVector<NSupportLinks::TResolvedLink> Links;

    // Source-specific errors accumulated during resolving.
    TVector<NSupportLinks::TSupportError> Errors;

    // Actor to wait for async source response from; empty for sync sources.
    TMaybe<NActors::TActorId> Actor;
};

} // namespace NMVP
