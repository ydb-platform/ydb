#pragma once

#include "events.h"
#include "table_query.h"
#include "wasm/manifest.h"
#include "wasm/registry_helpers.h"

#include <ydb/core/kqp/common/dynamic_function_registry.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/services/metadata/request/common.h>

namespace NKikimr::NUdfStore {

class TWasmArtifactLoadActor : public NActors::TActorBootstrapped<TWasmArtifactLoadActor> {
private:
    using TBase = NActors::TActorBootstrapped<TWasmArtifactLoadActor>;

    enum class EStep {
        ReadModuleArtifact,
        ReadModuleWasmChunks,
        ReadModuleObjectChunks,
        ReadLibraryArtifact,
        ReadLibraryWasmChunks,
        ReadLibraryObjectChunks,
        RegisterModule,
    };

    NActors::TActorId ReplyTo_;
    TString Md5_;
    TString Manifest_;
    TString ArtifactTablePath_;
    TString ArtifactChunksTablePath_;
    NWasm::TWasmManifest ParsedManifest_;
    TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> FunctionRegistry_;

    EStep Step_ = EStep::ReadModuleArtifact;
    size_t NextLibraryIndex_ = 0;
    TString PendingLibraryName_;
    NTableQuery::TWasmArtifactRow ModuleArtifact_;
    NTableQuery::TWasmArtifactRow PendingLibraryArtifact_;
    TVector<TString> PendingWasmChunks_;
    TVector<NWasm::TNamedModuleBytecode> Libraries_;

    void ExecuteQuery(const TString& yql, bool readOnly);
    void ReplyError(const TString& message);
    void HandleQueryResult(NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogYQLRequest>::TPtr& ev);
    void HandleQueryFailed(NMetadata::NRequest::TEvRequestFailed::TPtr& ev);
    void OnQuerySuccess(const Ydb::Table::ExecuteDataQueryResponse& response);
    void StartNextLibrary();
    void RegisterLoadedModule();

public:
    TWasmArtifactLoadActor(
        const NActors::TActorId& replyTo,
        const TString& md5,
        const TString& manifest,
        const TString& artifactTablePath,
        const TString& artifactChunksTablePath,
        TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> functionRegistry)
        : ReplyTo_(replyTo)
        , Md5_(md5)
        , Manifest_(manifest)
        , ArtifactTablePath_(artifactTablePath)
        , ArtifactChunksTablePath_(artifactChunksTablePath)
        , FunctionRegistry_(std::move(functionRegistry))
    {}

    void Bootstrap();

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogYQLRequest>, HandleQueryResult);
            hFunc(NMetadata::NRequest::TEvRequestFailed, HandleQueryFailed);
            default:
                break;
        }
    }
};

} // namespace NKikimr::NUdfStore
