#include "s3.h"
#include "config.h"
#include "s3_router_events.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/protos/s3_settings.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {

    using TS3Manager = TBlobDepot::TS3Manager;

    TS3Manager::TS3Manager(TBlobDepot *self)
        : Self(self)
    {}

    TS3Manager::~TS3Manager() = default;

    void TS3Manager::Init(const NKikimrBlobDepot::TS3BackendSettings *settings) {
        YDB_LOG_DEBUG("Init",
            {"marker", "BDTS05"},
            {"settings", settings});
        DeleteRoute = settings ? std::make_shared<const TString>(TS3DeleteContext::MakeRouteKey(*settings)) : nullptr;
        if (settings) {
            RouterGeneration = Self->Executor()->Generation();
            Self->Send(MakeBlobStorageNodeWardenID(Self->SelfId().NodeId()),
                new NStorage::TEvNodeWardenAcquireBlobDepotS3Router(Self->TabletID(), RouterGeneration, *settings));
            BasePath = MakeS3BasePath(settings->GetSettings(), Self->Config.GetName());
            Bucket = settings->GetSettings().GetBucket();
            SyncMode = settings->HasSyncMode();
            AsyncMode = settings->HasAsyncMode();
            const auto prefixError = ValidateS3BasePath(BasePath);
            // Legacy depots could share both the prefix and the tablet-local locator numbers.
            GarbageCollectionEnabled = Self->Config.GetS3NamespaceValidated() && !prefixError;

            CurrentMaxWritesInFlight = MaxWritesInFlight();
            CurrentMaxDeletesInFlight = MaxDeletesInFlight();
            Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_DELETE_MAX_IN_FLIGHT] = CurrentMaxDeletesInFlight;
            Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_PUT_MAX_WRITES_IN_FLIGHT] = CurrentMaxWritesInFlight;
            if (!DeleteQueue.empty()) {
                RunDeletersIfNeeded();
            }
        } else {
            SyncMode = false;
            AsyncMode = false;
            GarbageCollectionEnabled = false;
        }
    }

    void TS3Manager::TerminateAllActors() {
        for (TActorId actorId : ActiveUploaders) {
            Self->Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, Self->SelfId(), nullptr, 0));
        }
        for (TActorId actorId : ActiveDeleters) {
            Self->Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, Self->SelfId(), nullptr, 0));
        }
        if (ScannerActorId) {
            Self->Send(new IEventHandle(TEvents::TSystem::Poison, 0, ScannerActorId, Self->SelfId(), nullptr, 0));
        }
        if (RouterGeneration) {
            Self->Send(MakeBlobStorageNodeWardenID(Self->SelfId().NodeId()),
                new NStorage::TEvNodeWardenReleaseBlobDepotS3Router(Self->TabletID(), RouterGeneration,
                    Self->RestartingForConfigChange));
            WrapperId = {};
        }
    }

    void TS3Manager::Handle(TAutoPtr<IEventHandle> ev) {
        STRICT_STFUNC_BODY(
            hFunc(NStorage::TEvNodeWardenBlobDepotS3RouterReady, [&](auto ev) {
                const auto& msg = *ev->Get();
                if (msg.TabletId != Self->TabletID() || msg.Generation != RouterGeneration || WrapperId) {
                    return;
                }
                if (!msg.Router) {
                    Self->Send(Self->Tablet(), new TEvents::TEvPoison);
                    return;
                }
                WrapperId = msg.Router;
                RunDeletersIfNeeded();
                if (Self->Data->IsLoaded()) {
                    OnDataLoaded();
                }
                Self->ProcessRegisterAgentQ();
            })
            fFunc(TEvPrivate::EvDeleteResult, HandleDeleter)
            fFunc(TEvPrivate::EvScanFound, HandleScanner)
            cFunc(TEvPrivate::EvDeleteThrottleWakeup, HandleDeleteThrottleWakeup)
            cFunc(TEvPrivate::EvPutThrottleWakeup, HandlePutThrottleWakeup)
        )
    }

    void TS3Manager::OnDataLoaded() {
        if (WrapperId && GarbageCollectionEnabled) {
            RunScannerActor();
        }
    }

    void TBlobDepot::InitS3Manager() {
        S3Manager->Init(Config.HasS3BackendSettings() ? &Config.GetS3BackendSettings() : nullptr);
    }

} // NKikimr::NBlobDepot
