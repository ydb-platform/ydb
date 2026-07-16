#pragma once

#include "events.h"
#include "metadata_subscription/wasm_artifact.h"

#include <ydb/library/table_creator/table_creator.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NUdfStore {

class TWasmArtifactTableInitializer : public NActors::TActorBootstrapped<TWasmArtifactTableInitializer> {
private:
    using TBase = NActors::TActorBootstrapped<TWasmArtifactTableInitializer>;

    enum class EStep {
        ArtifactTable,
        ArtifactChunksTable,
    };

    NActors::TActorId ParentId_;
    TString ArtifactTablePath_;
    TString ArtifactChunksTablePath_;
    EStep Step_ = EStep::ArtifactTable;

    void CreateCurrentTable();
    void HandleTableCreated(TEvTableCreator::TEvCreateTableResponse::TPtr& ev);

public:
    TWasmArtifactTableInitializer(
        const NActors::TActorId& parentId,
        const TString& artifactTablePath,
        const TString& artifactChunksTablePath)
        : ParentId_(parentId)
        , ArtifactTablePath_(artifactTablePath)
        , ArtifactChunksTablePath_(artifactChunksTablePath)
    {}

    void Bootstrap();

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTableCreator::TEvCreateTableResponse, HandleTableCreated);
            default:
                break;
        }
    }
};

} // namespace NKikimr::NUdfStore
