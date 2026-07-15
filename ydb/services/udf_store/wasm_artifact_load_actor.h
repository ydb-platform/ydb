#pragma once

#include "events.h"
#include "table_query.h"
#include "wasm/manifest.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/services/metadata/request/common.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NKikimr::NUdfStore {

class TWasmArtifactLoadActor : public NActors::TActorBootstrapped<TWasmArtifactLoadActor> {
private:
    using TBase = NActors::TActorBootstrapped<TWasmArtifactLoadActor>;

    enum class EStep {
        ReadModuleArtifact,
        ReadLibraryArtifact,
        LoadCompartment,
    };

    NActors::TActorId ReplyTo_;
    NActors::TActorId CompartmentActorId_;
    TString Md5_;
    TString Manifest_;
    TString ArtifactTablePath_;
    NWasm::TWasmManifest ParsedManifest_;
    TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> FunctionRegistry_;

    EStep Step_ = EStep::ReadModuleArtifact;
    size_t NextLibraryIndex_ = 0;
    TString PendingLibraryName_;
    NTableQuery::TWasmArtifactRow ModuleArtifact_;
    TVector<NWasm::TNamedModuleBytecode> Libraries_;

    void ExecuteQuery(const TString& yql, bool readOnly);
    void ReplyError(const TString& message);
    void HandleQueryResult(NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogYQLRequest>::TPtr& ev);
    void HandleQueryFailed(NMetadata::NRequest::TEvRequestFailed::TPtr& ev);
    void OnQuerySuccess(const Ydb::Table::ExecuteDataQueryResponse& response);
    void StartNextLibrary();
    void StartCompartmentLoad();
    void HandleCompartmentLoaded(TEvWasmCompartmentLoaded::TPtr& ev);

public:
    TWasmArtifactLoadActor(
        const NActors::TActorId& replyTo,
        const NActors::TActorId& compartmentActorId,
        const TString& md5,
        const TString& manifest,
        const TString& artifactTablePath,
        TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> functionRegistry)
        : ReplyTo_(replyTo)
        , CompartmentActorId_(compartmentActorId)
        , Md5_(md5)
        , Manifest_(manifest)
        , ArtifactTablePath_(artifactTablePath)
        , FunctionRegistry_(std::move(functionRegistry))
    {}

    void Bootstrap();

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogYQLRequest>, HandleQueryResult);
            hFunc(NMetadata::NRequest::TEvRequestFailed, HandleQueryFailed);
            hFunc(TEvWasmCompartmentLoaded, HandleCompartmentLoaded);
            default:
                break;
        }
    }
};

} // namespace NKikimr::NUdfStore
