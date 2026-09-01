#include "artifact_table_initializer.h"
#include "blob_chunks.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NUdfStore {

namespace {

TVector<TString> GetPathFromMetadata(const TString& fullPath) {
    const auto& path = NKikimr::SplitPath(fullPath);
    auto it = cbegin(path);
    while (it != path.end() && *it != NMetadata::NProvider::TServiceOperator::GetPath()) {
        ++it;
    }
    AFL_VERIFY(it != cend(path));
    return {it, cend(path)};
}

} // namespace

void TWasmArtifactTableInitializer::Bootstrap() {
    Become(&TWasmArtifactTableInitializer::StateFunc);
    CreateCurrentTable();
}

void TWasmArtifactTableInitializer::CreateCurrentTable() {
    if (Step_ == EStep::ArtifactTable) {
        Register(CreateTableCreator(
            GetPathFromMetadata(ArtifactTablePath_),
            TUdfWasmArtifact::GetColumnDescription(),
            TUdfWasmArtifact::GetPk(),
            NKikimrServices::METADATA_PROVIDER,
            Nothing(),
            {},
            /* isSystemUser */ true
        ));
        return;
    }

    Register(CreateTableCreator(
        GetPathFromMetadata(ArtifactChunksTablePath_),
        TArtifactChunkSchema::GetColumnDescription(),
        TArtifactChunkSchema::GetPk(),
        NKikimrServices::METADATA_PROVIDER,
        Nothing(),
        {},
        /* isSystemUser */ true
    ));
}

void TWasmArtifactTableInitializer::HandleTableCreated(TEvTableCreator::TEvCreateTableResponse::TPtr& ev) {
    if (!ev->Get()->Success) {
        const TString tablePath = Step_ == EStep::ArtifactTable
            ? ArtifactTablePath_
            : ArtifactChunksTablePath_;
        const TString errorMessage = TStringBuilder()
            << "failed to create wasm artifact table '" << tablePath
            << "': " << ev->Get()->Issues.ToString();
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TWasmArtifactTableInitializer: " << errorMessage;
        Send(ParentId_, new TEvStoreInitFailed(errorMessage));
        PassAway();
        return;
    }

    if (Step_ == EStep::ArtifactTable) {
        Step_ = EStep::ArtifactChunksTable;
        CreateCurrentTable();
        return;
    }

    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TWasmArtifactTableInitializer: artifact tables ready at "
        << ArtifactTablePath_ << " and " << ArtifactChunksTablePath_;
    Send(ParentId_, new TEvArtifactTableInitialized(ArtifactTablePath_));
    PassAway();
}

} // namespace NKikimr::NUdfStore
