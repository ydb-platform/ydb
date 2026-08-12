#pragma once

#include "events.h"
#include "table_query.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/services/metadata/request/common.h>

namespace NKikimr::NUdfStore {

class TWasmLibraryCompileActor : public NActors::TActorBootstrapped<TWasmLibraryCompileActor> {
private:
    using TBase = NActors::TActorBootstrapped<TWasmLibraryCompileActor>;

    enum class EStep {
        ReadLibrarySource,
        MarkCompiling,
        ReadLibraryChunks,
        DeleteArtifactChunks,
        WriteArtifactChunk,
        UpsertArtifact,
        UpdateMetaReady,
        UpdateMetaFailed,
    };

    NActors::TActorId ReplyTo_;
    TString LibraryName_;
    TString CpuSpec_;
    TString ModulesTablePath_;
    TString ModuleChunksTablePath_;
    TString ArtifactTablePath_;
    TString ArtifactChunksTablePath_;

    EStep Step_ = EStep::ReadLibrarySource;
    NTableQuery::TModuleSourceRow LibrarySource_;
    NTableQuery::TWasmArtifactRow ArtifactRow_;
    TString Kind_;
    TString Format_;
    TVector<NTableQuery::TPendingChunkWrite> PendingChunkWrites_;
    size_t NextChunkWriteIndex_ = 0;
    TString ErrorMessage_;

    void ExecuteQuery(const TString& yql, bool readOnly);
    void ReplyError(const TString& message);
    void ReplySuccess();
    void HandleQueryResult(NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogYQLRequest>::TPtr& ev);
    void HandleQueryFailed(NMetadata::NRequest::TEvRequestFailed::TPtr& ev);
    void OnQuerySuccess(const Ydb::Table::ExecuteDataQueryResponse& response);
    void CompileLibrary();
    void StartWriteChunks();
    void WriteNextChunk();
    void FailAndPersist(const TString& message);

public:
    TWasmLibraryCompileActor(
        const NActors::TActorId& replyTo,
        const TString& libraryName,
        const TString& cpuSpec,
        const TString& modulesTablePath,
        const TString& moduleChunksTablePath,
        const TString& artifactTablePath,
        const TString& artifactChunksTablePath)
        : ReplyTo_(replyTo)
        , LibraryName_(libraryName)
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
