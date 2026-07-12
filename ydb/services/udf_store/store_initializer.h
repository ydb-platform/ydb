#pragma once

#include <ydb/services/metadata/request/common.h>
#include <ydb/services/metadata/request/request_actor_cb.h>
#include <ydb/library/table_creator/table_creator.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NUdfStore {

// Events produced by TUdfStoreInitializer.
struct TEvUdfStoreInit {
    enum EEv {
        EvStoreInitialized = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvStoreInitFailed,
        EvEnd
    };

    // Sent to the parent actor when the meta table and KV volume are ready.
    struct TEvStoreInitialized : public NActors::TEventLocal<TEvStoreInitialized, EvStoreInitialized> {
        TEvStoreInitialized(const TString& kvVolumePath)
            : KvVolumePath(kvVolumePath)
        {}
        TString KvVolumePath;
    };

    // Sent to the parent actor when infrastructure initialization fails.
    struct TEvStoreInitFailed : public NActors::TEventLocal<TEvStoreInitFailed, EvStoreInitFailed> {
        explicit TEvStoreInitFailed(TString errorMessage)
            : ErrorMessage(std::move(errorMessage))
        {}
        TString ErrorMessage;
    };
};

// Actor that sequentially initializes UDF store infrastructure:
//   1. Creates the metadata table .metadata/udf_store/meta via CreateTableCreator.
//   2. On success, creates the KV volume .metadata/udf_store/binaries with 3 channels.
// On success, sends TEvStoreInitialized to ParentId; on failure, TEvStoreInitFailed.
// Dies after both steps complete (success or failure).
class TUdfStoreInitializer : public NActors::TActorBootstrapped<TUdfStoreInitializer> {
private:
    using TBase = NActors::TActorBootstrapped<TUdfStoreInitializer>;

    NActors::TActorId ParentId;
    TString KvStorageMedia;
    TString KvVolumePath;

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
