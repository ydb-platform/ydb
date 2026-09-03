#pragma once

#include "events.h"
#include "table_query.h"
#include "wasm/manifest.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/services/metadata/request/common.h>

namespace NKikimr::NUdfStore {

class TWasmCompileActor : public NActors::TActorBootstrapped<TWasmCompileActor> {
private:
    using TBase = NActors::TActorBootstrapped<TWasmCompileActor>;

    enum class EStep {
        ReadModuleSource,
        MarkCompiling,
        ReadModuleChunks,
        ReadLibraryArtifact,
        DeleteArtifactChunks,
        WriteArtifactChunk,
        UpsertModuleArtifact,
        UpdateMetaReady,
        UpdateMetaFailed,
    };

    NActors::TActorId ReplyTo_;
    TString Name_;
    TString Manifest_;
    TString CpuSpec_;
    TString ModulesTablePath_;
    TString ModuleChunksTablePath_;
    TString ArtifactTablePath_;
    TString ArtifactChunksTablePath_;

    EStep Step_ = EStep::ReadModuleSource;
    NTableQuery::TModuleSourceRow ModuleSource_;
    NWasm::TWasmManifest ParsedManifest_;
    size_t NextLibraryIndex_ = 0;
    TString PendingLibraryName_;
    TString ModuleKind_;
    NTableQuery::TWasmArtifactRow ArtifactRow_;
    TVector<NTableQuery::TPendingChunkWrite> PendingChunkWrites_;
    size_t NextChunkWriteIndex_ = 0;
    TString ErrorMessage_;

    void ExecuteQuery(const TString& yql, bool readOnly);
    void ReplyError(const TString& message);
    void ReplyDeferred(const TString& reason);
    void ReplySuccess();
    void HandleQueryResult(NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogYQLRequest>::TPtr& ev);
    void HandleQueryFailed(NMetadata::NRequest::TEvRequestFailed::TPtr& ev);
    void OnQuerySuccess(const Ydb::Table::ExecuteDataQueryResponse& response);
    void StartNextLibrary();
    void CompileUserModule();
    void ValidateExports();
    void StartWriteChunks();
    void WriteNextChunk();
    void FailAndPersist(const TString& message);

public:
    TWasmCompileActor(
        const NActors::TActorId& replyTo,
        const TString& name,
        const TString& manifest,
        const TString& cpuSpec,
        const TString& modulesTablePath,
        const TString& moduleChunksTablePath,
        const TString& artifactTablePath,
        const TString& artifactChunksTablePath)
        : ReplyTo_(replyTo)
        , Name_(name)
        , Manifest_(manifest)
        , CpuSpec_(cpuSpec)
        , ModulesTablePath_(modulesTablePath)
        , ModuleChunksTablePath_(moduleChunksTablePath)
        , ArtifactTablePath_(artifactTablePath)
        , ArtifactChunksTablePath_(artifactChunksTablePath)
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
