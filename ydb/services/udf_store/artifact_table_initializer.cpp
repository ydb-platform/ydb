#include "artifact_table_initializer.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NUdfStore {

void TWasmArtifactTableInitializer::Bootstrap() {
    Become(&TWasmArtifactTableInitializer::StateFunc);

    const auto& path = NKikimr::SplitPath(ArtifactTablePath_);
    auto it = cbegin(path);
    while (it != path.end() && *it != NMetadata::NProvider::TServiceOperator::GetPath()) {
        ++it;
    }
    AFL_VERIFY(it != cend(path));

    Register(CreateTableCreator(
        {it, cend(path)},
        TUdfWasmArtifact::GetColumnDescription(),
        TUdfWasmArtifact::GetPk(),
        NKikimrServices::METADATA_PROVIDER,
        Nothing(),
        {},
        /* isSystemUser */ true
    ));
}

void TWasmArtifactTableInitializer::HandleTableCreated(TEvTableCreator::TEvCreateTableResponse::TPtr& ev) {
    if (!ev->Get()->Success) {
        const TString errorMessage = TStringBuilder()
            << "failed to create wasm artifact table '" << ArtifactTablePath_
            << "': " << ev->Get()->Issues.ToString();
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TWasmArtifactTableInitializer: " << errorMessage;
        Send(ParentId_, new TEvStoreInitFailed(errorMessage));
        PassAway();
        return;
    }

    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TWasmArtifactTableInitializer: artifact table ready at " << ArtifactTablePath_;
    Send(ParentId_, new TEvArtifactTableInitialized(ArtifactTablePath_));
    PassAway();
}

} // namespace NKikimr::NUdfStore
