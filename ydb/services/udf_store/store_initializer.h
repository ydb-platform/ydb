#pragma once

#include "events.h"
#include <ydb/services/metadata/request/common.h>
#include <ydb/services/metadata/request/request_actor_cb.h>
#include <ydb/library/table_creator/table_creator.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NUdfStore {

// Actor that sequentially initializes UDF store infrastructure:
//   1. Creates modules + module_chunks tables.
//   2. Creates the KV volume for native UDF binaries.
// Artifact tables are created lazily per CPU spec by TUdfStoreService.
class TUdfStoreInitializer : public NActors::TActorBootstrapped<TUdfStoreInitializer> {
private:
    using TBase = NActors::TActorBootstrapped<TUdfStoreInitializer>;

    enum class EInitStep {
        Modules,
        ModuleChunks,
        KvVolume,
        Done,
    };

    NActors::TActorId ParentId;
    TString KvStorageMedia;
    TString KvVolumePath;
    EInitStep InitStep_ = EInitStep::Modules;

    void CreateNextTable();
    void AdvanceInitStep();
    void HandleTableCreated(TEvTableCreator::TEvCreateTableResponse::TPtr& ev);
    void HandleKvVolumeCreated(NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogCreateKvVolume>::TPtr& ev);
    void HandleRequestFailed(NMetadata::NRequest::TEvRequestFailed::TPtr& ev);

public:
    explicit TUdfStoreInitializer(const NActors::TActorId& parentId, const TString& kvStorageMedia = "ssd")
        : ParentId(parentId)
        , KvStorageMedia(kvStorageMedia)
    {}

    void Bootstrap();

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTableCreator::TEvCreateTableResponse, HandleTableCreated);
            hFunc(NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogCreateKvVolume>, HandleKvVolumeCreated);
            hFunc(NMetadata::NRequest::TEvRequestFailed, HandleRequestFailed);
            default:
                break;
        }
    }
};

} // namespace NKikimr::NUdfStore
