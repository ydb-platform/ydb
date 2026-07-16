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
        ReadWasmSource,
        ReadWasmChunks,
        ReadLibraryArtifact,
        DeleteArtifactChunks,
        UpsertModuleArtifact,
        WriteArtifactChunk,
        UpdateMetaReady,
        UpdateMetaFailed,
    };

    NActors::TActorId ReplyTo_;
    TString Md5_;
    TString Manifest_;
    TString CpuSpec_;
    TString WasmSourceTablePath_;
    TString WasmSourceChunksTablePath_;
    TString ArtifactTablePath_;
    TString ArtifactChunksTablePath_;
    TString MetaTablePath_;

    EStep Step_ = EStep::ReadWasmSource;
    NTableQuery::TWasmSourceRow WasmSource_;
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
        const TString& md5,
        const TString& manifest,
        const TString& cpuSpec,
        const TString& wasmSourceTablePath,
        const TString& wasmSourceChunksTablePath,
        const TString& artifactTablePath,
        const TString& artifactChunksTablePath,
        const TString& metaTablePath)
        : ReplyTo_(replyTo)
        , Md5_(md5)
        , Manifest_(manifest)
        , CpuSpec_(cpuSpec)
        , WasmSourceTablePath_(wasmSourceTablePath)
        , WasmSourceChunksTablePath_(wasmSourceChunksTablePath)
        , ArtifactTablePath_(artifactTablePath)
        , ArtifactChunksTablePath_(artifactChunksTablePath)
        , MetaTablePath_(metaTablePath)
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
