#include "s3.h"
#include "config.h"
#include "s3_router_events.h"
#include "s3_request_timer.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/protos/config.pb.h>
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
            RouterGeneration = Self->Executor()->Generation();
            Self->Send(MakeBlobStorageNodeWardenID(Self->SelfId().NodeId()),
                new NStorage::TEvNodeWardenAcquireBlobDepotS3Router(Self->TabletID(), RouterGeneration, *settings));
            BasePath = MakeS3BasePath(settings->GetSettings(), Self->Config.GetName());
            DeleteRoute = std::make_shared<const TString>(TS3DeleteContext::MakeRouteKey(*settings));
            Bucket = settings->GetSettings().GetBucket();
            const auto& route = settings->GetSettings();
            const auto& defaults = AppData()->AwsClientConfig;
            const auto requestMs = route.HasRequestTimeoutMs() ? route.GetRequestTimeoutMs() : defaults.GetRequestTimeoutMs();
            const auto httpMs = route.HasHttpRequestTimeoutMs() ? route.GetHttpRequestTimeoutMs() : defaults.GetHttpRequestTimeoutMs();
            const auto connectMs = route.HasConnectionTimeoutMs() ? route.GetConnectionTimeoutMs() : defaults.GetConnectionTimeoutMs();
            const auto ioTimeout = Max(TS3RequestTimer::Timeout, TDuration::MilliSeconds(Max(requestMs, httpMs)));
            // Allow twice the SDK's default eleven attempts (including connect time).
            // An incremental namespace scan covers arrivals beyond this window.
            PutWatchWindow = ClampVal((ioTimeout + TDuration::MilliSeconds(connectMs)) * 22,
                TDuration::Hours(1), TDuration::Days(1));
            if (Self->IsDataCoolingEnabled()) {
                UploadPutsInFlight = settings->GetAsyncMode().GetUploadPutsInFlight();
            }

            CurrentMaxWritesInFlight = MaxWritesInFlight();
            CurrentMaxDeletesInFlight = MaxDeletesInFlight();
            Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_DELETE_MAX_IN_FLIGHT] = CurrentMaxDeletesInFlight;
            Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_PUT_MAX_WRITES_IN_FLIGHT] = CurrentMaxWritesInFlight;
            if (!DeleteQueue.empty()) {
                RunDeletersIfNeeded();
            }
        } else {
            DeleteRoute.reset();
        }
    }

    void TS3Manager::TerminateAllActors() {
        for (const auto& entry : ActiveUploaders) {
            Self->Send(new IEventHandle(TEvents::TSystem::Poison, 0, entry.first, Self->SelfId(), nullptr, 0));
        }
        for (TActorId actorId : ActiveDeleters) {
            Self->Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, Self->SelfId(), nullptr, 0));
        }
        if (PutProbe) {
            Self->Send(PutProbe, new TEvents::TEvPoison);
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
            fFunc(TEvPrivate::EvUploadResult, HandleUploadResult)
            fFunc(TEvPrivate::EvRunCooling, HandleCoolingWakeup)
            fFunc(TEvPrivate::EvCoolingTimer, HandleCoolingWakeup)
            fFunc(TEvPrivate::EvS3ProbeWakeup, HandleProbeWakeup)
            fFunc(TEvPrivate::EvS3ProbeFinished, HandleProbeFinished)
            fFunc(TEvPrivate::EvDeleteResult, HandleDeleter)
            fFunc(TEvPrivate::EvScanFound, HandleScanner)
            cFunc(TEvPrivate::EvResumeS3Scan, HandleResumeScanner)
            cFunc(TEvPrivate::EvDeleteThrottleWakeup, HandleDeleteThrottleWakeup)
            cFunc(TEvPrivate::EvPutThrottleWakeup, HandlePutThrottleWakeup)
        )
    }

    void TS3Manager::OnDataLoaded() {
        if (WrapperId) {
            if (NextOrphanScan > TActivationContext::Now()) {
                ScheduleScan(NextOrphanScan - TActivationContext::Now());
            } else {
                RunScannerActor();
            }
        }
        if (WrapperId && NumPutWatches) {
            RunPutProbe();
        }
        MaybeScheduleCooling();
    }

    void TBlobDepot::InitS3Manager() {
        S3Manager->Init(Config.HasS3BackendSettings() ? &Config.GetS3BackendSettings() : nullptr);
    }

} // NKikimr::NBlobDepot
