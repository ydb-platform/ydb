#pragma once

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

namespace NKikimr::NUdfStore {

enum EEv {
    EvStoreInitialized = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
    EvStoreInitFailed,
    EvReadBodyResponse,
    EvEnd
};

// Sent to the parent actor when the meta table and KV volume are ready.
struct TEvStoreInitialized : public NActors::TEventLocal<TEvStoreInitialized, EvStoreInitialized> {
    TEvStoreInitialized(const TString& kvVolumePath)
        : KvVolumePath(kvVolumePath)
    {}
    TString KvVolumePath;
};

// Sent to the parent actor when infrastructure initialization fails.
struct TEvStoreInitFailed : public NActors::TEventLocal<TEvStoreInitFailed, EvStoreInitFailed> {
    explicit TEvStoreInitFailed(TString errorMessage)
        : ErrorMessage(std::move(errorMessage))
    {}
    TString ErrorMessage;
};


struct TEvReadBodyResponse : public NActors::TEventLocal<TEvReadBodyResponse, EvReadBodyResponse> {
    bool Success;
    TString Name;
    TString ErrorMessage;

    TEvReadBodyResponse(bool success, const TString& name, const TString& errorMessage = {})
        : Success(success)
        , Name(name)
        , ErrorMessage(errorMessage)
    {}
};

} // namespace NKikimr::NUdfStore
