#pragma once

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
    TString ErrorMessage;

    TEvReadBodyResponse(bool success, const TString& name, const TString& errorMessage = {})
        : Success(success)
        , Name(name)
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
    TString LibraryName;
    TString ErrorMessage;

    TEvLibraryCompileResponse(bool success, const TString& libraryName, const TString& errorMessage = {})
        : Success(success)
        , LibraryName(libraryName)
        , ErrorMessage(errorMessage)
    {}
};

} // namespace NKikimr::NUdfStore
