#pragma once

#include "events.h"

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/services/udf_store/wasm/registry.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>

#include <memory>

namespace NKikimr::NUdfStore {

enum EEv {
    EvStoreInitialized = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
    EvStoreInitFailed,
    EvArtifactTableInitialized,
    EvReadBodyResponse,
    EvWasmCompileResponse,
    EvLibraryCompileResponse,
    EvWasmCompartmentLoad,
    EvWasmCompartmentLoaded,
    EvWasmCompartmentUnload,
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
    TString Md5;
    TString ErrorMessage;

    TEvWasmCompileResponse(
        bool success,
        const TString& md5,
        const TString& errorMessage = {},
        bool deferred = false)
        : Success(success)
        , Deferred(deferred)
        , Md5(md5)
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

struct TEvWasmCompartmentLoad : public NActors::TEventLocal<TEvWasmCompartmentLoad, EvWasmCompartmentLoad> {
    TString Md5;
    TString Manifest;
    TString ModuleWasmData;
    TString ModuleObjectCode;
    NYdb::NWasm::EBytecodeFormat ModuleFormat = NYdb::NWasm::EBytecodeFormat::Binary;
    TVector<NWasm::TNamedModuleBytecode> Libraries;

    TEvWasmCompartmentLoad(
        const TString& md5,
        const TString& manifest,
        TString moduleWasmData,
        TString moduleObjectCode,
        NYdb::NWasm::EBytecodeFormat moduleFormat,
        TVector<NWasm::TNamedModuleBytecode> libraries)
        : Md5(md5)
        , Manifest(manifest)
        , ModuleWasmData(std::move(moduleWasmData))
        , ModuleObjectCode(std::move(moduleObjectCode))
        , ModuleFormat(moduleFormat)
        , Libraries(std::move(libraries))
    {}
};

struct TEvWasmCompartmentLoaded : public NActors::TEventLocal<TEvWasmCompartmentLoaded, EvWasmCompartmentLoaded> {
    bool Success = false;
    TString Md5;
    TString ErrorMessage;
    std::shared_ptr<NWasm::TWasmCompartmentState> State;

    TEvWasmCompartmentLoaded(
        bool success,
        const TString& md5,
        std::shared_ptr<NWasm::TWasmCompartmentState> state = {},
        const TString& errorMessage = {})
        : Success(success)
        , Md5(md5)
        , ErrorMessage(errorMessage)
        , State(std::move(state))
    {}
};

struct TEvWasmCompartmentUnload : public NActors::TEventLocal<TEvWasmCompartmentUnload, EvWasmCompartmentUnload> {
    explicit TEvWasmCompartmentUnload(TString md5)
        : Md5(std::move(md5))
    {}

    TString Md5;
};

} // namespace NKikimr::NUdfStore
