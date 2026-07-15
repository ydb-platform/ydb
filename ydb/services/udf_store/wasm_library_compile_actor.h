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
        UpsertArtifact,
        UpdateMetaReady,
        UpdateMetaFailed,
    };

    NActors::TActorId ReplyTo_;
    TString LibraryName_;
    TString CpuSpec_;
    TString LibrarySourceTablePath_;
    TString ArtifactTablePath_;

    EStep Step_ = EStep::ReadLibrarySource;
    NTableQuery::TLibrarySourceRow LibrarySource_;
    TString ErrorMessage_;

    void ExecuteQuery(const TString& yql, bool readOnly);
    void ReplyError(const TString& message);
    void ReplySuccess();
    void HandleQueryResult(NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogYQLRequest>::TPtr& ev);
    void HandleQueryFailed(NMetadata::NRequest::TEvRequestFailed::TPtr& ev);
    void OnQuerySuccess(const Ydb::Table::ExecuteDataQueryResponse& response);
    void CompileLibrary();

public:
    TWasmLibraryCompileActor(
        const NActors::TActorId& replyTo,
        const TString& libraryName,
        const TString& cpuSpec,
        const TString& librarySourceTablePath,
        const TString& artifactTablePath)
        : ReplyTo_(replyTo)
        , LibraryName_(libraryName)
        , CpuSpec_(cpuSpec)
        , LibrarySourceTablePath_(librarySourceTablePath)
        , ArtifactTablePath_(artifactTablePath)
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
