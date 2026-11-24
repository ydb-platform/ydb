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
        if (settings) {
            // All S3 traffic goes through the per-node router managed by NodeWarden.
            // Acquire is fire-and-forget: NodeWarden registers the router under its
            // well-known service id before any later event we send can be processed.
            RouterGeneration = Self->Executor()->Generation();
            Self->Send(MakeBlobStorageNodeWardenID(Self->SelfId().NodeId()),
                new NStorage::TEvNodeWardenAcquireBlobDepotS3Router(Self->TabletID(), RouterGeneration, *settings));
            WrapperId = MakeBlobDepotS3RouterID(Self->TabletID());
            BasePath = MakeS3BasePath(settings->GetSettings(), Self->Config.GetName());
            Bucket = settings->GetSettings().GetBucket();
            SyncMode = settings->HasSyncMode();
            AsyncMode = settings->HasAsyncMode();
            const TString legacyPath = TStringBuilder() << settings->GetSettings().GetObjectKeyPattern()
                << '/' << Self->Config.GetName();
            // Canonical LIST can reach objects a legacy scanner never saw.
            OrphanScanEnabled = !ValidateS3BasePath(BasePath) && (Self->Config.GetS3NamespaceValidated() ||
                (!Self->Config.GetS3NamespacePinned() && !settings->HasAsyncMode() && BasePath == legacyPath));

            CurrentMaxWritesInFlight = MaxWritesInFlight();
            CurrentMaxDeletesInFlight = MaxDeletesInFlight();
            Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_DELETE_MAX_IN_FLIGHT] = CurrentMaxDeletesInFlight;
            Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_PUT_MAX_WRITES_IN_FLIGHT] = CurrentMaxWritesInFlight;
        } else {
            SyncMode = false;
            AsyncMode = false;
            OrphanScanEnabled = false;
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
        if (WrapperId) {
            Self->Send(MakeBlobStorageNodeWardenID(Self->SelfId().NodeId()),
                new NStorage::TEvNodeWardenReleaseBlobDepotS3Router(Self->TabletID(), RouterGeneration));
            WrapperId = {};
        }
    }

    void TS3Manager::Handle(TAutoPtr<IEventHandle> ev) {
        STRICT_STFUNC_BODY(
            fFunc(TEvPrivate::EvDeleteResult, HandleDeleter)
            fFunc(TEvPrivate::EvScanFound, HandleScanner)
            cFunc(TEvPrivate::EvDeleteThrottleWakeup, HandleDeleteThrottleWakeup)
            cFunc(TEvPrivate::EvPutThrottleWakeup, HandlePutThrottleWakeup)
        )
    }

    void TS3Manager::OnDataLoaded() {
        if (OrphanScanEnabled) {
            RunScannerActor();
        }
    }

    void TBlobDepot::InitS3Manager() {
        S3Manager->Init(Config.HasS3BackendSettings() ? &Config.GetS3BackendSettings() : nullptr);
    }

} // NKikimr::NBlobDepot
