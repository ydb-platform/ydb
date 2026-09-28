#pragma once

#include "metadata_subscription/udf_module.h"

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

namespace NKikimr::NUdfStore {

enum EEv {
    EvStoreInitialized = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
    EvStoreInitFailed,
    EvArtifactTableInitialized,
    EvReadBodyResponse,
    EvWasmCompileResponse,
    EvLibraryCompileResponse,
    EvEnd
};

struct TEvStoreInitialized : public NActors::TEventLocal<TEvStoreInitialized, EvStoreInitialized> {
    TEvStoreInitialized(const TString& kvVolumePath)
        : KvVolumePath(kvVolumePath)
    {}
    TString KvVolumePath;
};

struct TEvArtifactTableInitialized : public NActors::TEventLocal<TEvArtifactTableInitialized, EvArtifactTableInitialized> {
    explicit TEvArtifactTableInitialized(TString artifactTablePath)
        : ArtifactTablePath(std::move(artifactTablePath))
    {}
    TString ArtifactTablePath;
};

struct TEvStoreInitFailed : public NActors::TEventLocal<TEvStoreInitFailed, EvStoreInitFailed> {
    explicit TEvStoreInitFailed(TString errorMessage)
        : ErrorMessage(std::move(errorMessage))
    {}
    TString ErrorMessage;
};

struct TEvReadBodyResponse : public NActors::TEventLocal<TEvReadBodyResponse, EvReadBodyResponse> {
    bool Success;
    TString Name;
    //! Which pending queue the reply belongs to. Native bodies and WASM
    //! artifacts are fetched by different actors that both answer with this
    //! event, and a name can sit at the front of both queues while a type
    //! change works its way through the snapshot, so the name alone does not
    //! say whose reply this is.
    EUdfType Type;
    TString ErrorMessage;

    TEvReadBodyResponse(bool success, const TString& name, EUdfType type, const TString& errorMessage = {})
        : Success(success)
        , Name(name)
        , Type(type)
        , ErrorMessage(errorMessage)
    {}
};

struct TEvWasmCompileResponse : public NActors::TEventLocal<TEvWasmCompileResponse, EvWasmCompileResponse> {
    bool Success;
    bool Deferred = false;
    TString Name;
    TString ErrorMessage;

    TEvWasmCompileResponse(
        bool success,
        const TString& name,
        const TString& errorMessage = {},
        bool deferred = false)
        : Success(success)
        , Deferred(deferred)
        , Name(name)
        , ErrorMessage(errorMessage)
    {}
};

struct TEvLibraryCompileResponse : public NActors::TEventLocal<TEvLibraryCompileResponse, EvLibraryCompileResponse> {
    bool Success;
    //! The library was re-uploaded while this compile ran, so its result is for
    //! an upload nobody wants any more. Separate from a real failure because
    //! only the latter should count against the compile controller's retries.
    bool Deferred = false;
    TString LibraryName;
    TString ErrorMessage;

    TEvLibraryCompileResponse(
        bool success,
        const TString& libraryName,
        const TString& errorMessage = {},
        bool deferred = false)
        : Success(success)
        , Deferred(deferred)
        , LibraryName(libraryName)
        , ErrorMessage(errorMessage)
    {}
};

} // namespace NKikimr::NUdfStore
