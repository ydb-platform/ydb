#include "s3_router.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/random_provider/random_provider.h>

#include <util/string/strip.h>

namespace NKikimr::NBlobDepot {

    namespace {

    // Adapter installed on the inner storage wrapper. It does NOT redirect the response
    // (recipient stays at the original sender of the request), but it inspects every
    // outgoing response and notifies the router actor when an HTTP 5xx is detected, so
    // the router can refresh the endpoint promptly.
    //
    // The adapter runs in AWS SDK callback threads, so ActorSystem is captured up front.
    class TRouterReplyAdapter : public NWrappers::NExternalStorage::IReplyAdapter {
        using IReplyAdapter = NWrappers::NExternalStorage::IReplyAdapter;
        TActorSystem* const ActorSystem;
        const TActorId RouterId;
        const ui32 NotifyEventType;

    private:
        template <typename T>
        std::unique_ptr<IEventBase> Inspect(std::unique_ptr<T>&& ev) const {
            if (!ev->IsSuccess()) {
                const auto& error = ev->GetError();
                const int code = static_cast<int>(error.GetResponseCode());
                if (code >= 500 && code < 600) {
                    ActorSystem->Send(new IEventHandle(NotifyEventType, 0, RouterId, TActorId{}, nullptr, 0));
                }
            }
            return std::move(ev);
        }

    public:
        TRouterReplyAdapter(TActorSystem* actorSystem, TActorId routerId, ui32 notifyEventType)
            : ActorSystem(actorSystem)
            , RouterId(routerId)
            , NotifyEventType(notifyEventType)
        {}

#define IMPL_REBUILD(NAME) \
        std::unique_ptr<IEventBase> RebuildReplyEvent(std::unique_ptr<NWrappers::NExternalStorage::NAME>&& ev) const override { \
            return Inspect(std::move(ev)); \
        }

        IMPL_REBUILD(TEvListObjectsResponse)
        IMPL_REBUILD(TEvGetObjectResponse)
        IMPL_REBUILD(TEvHeadObjectResponse)
        IMPL_REBUILD(TEvPutObjectResponse)
        IMPL_REBUILD(TEvDeleteObjectResponse)
        IMPL_REBUILD(TEvDeleteObjectsResponse)
        IMPL_REBUILD(TEvCreateMultipartUploadResponse)
        IMPL_REBUILD(TEvUploadPartResponse)
        IMPL_REBUILD(TEvCompleteMultipartUploadResponse)
        IMPL_REBUILD(TEvAbortMultipartUploadResponse)
        IMPL_REBUILD(TEvCheckObjectExistsResponse)
        IMPL_REBUILD(TEvUploadPartCopyResponse)
#undef IMPL_REBUILD
    };

    class TBlobDepotS3Router : public TActorBootstrapped<TBlobDepotS3Router> {
        struct TEvPrivate {
            enum {
                EvBalancerTick = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvRefreshNow,
            };
        };

        NKikimrBlobDepot::TS3BackendSettings Settings;
        TString CurrentEndpoint;
        TActorId InnerWrapperId;
        TActorId HttpProxyId;
        bool RefreshInFlight = false;
        bool RefreshScheduled = false;

        ui32 RefreshSecMin() const {
            return Settings.HasBalancerRefreshSecMin() ? Settings.GetBalancerRefreshSecMin() : 10;
        }

        ui32 RefreshSecMax() const {
            const ui32 lo = RefreshSecMin();
            const ui32 hi = Settings.HasBalancerRefreshSecMax() ? Settings.GetBalancerRefreshSecMax() : 15;
            return hi >= lo ? hi : lo;
        }

        TDuration NextRefreshDelay() const {
            const ui32 lo = RefreshSecMin();
            const ui32 hi = RefreshSecMax();
            const ui32 sec = lo == hi ? lo
                : lo + TAppData::RandomProvider->GenRand() % (hi - lo + 1);
            return TDuration::Seconds(sec);
        }

        void BuildInnerWrapper(const TString& endpoint) {
            if (InnerWrapperId) {
                Send(InnerWrapperId, new TEvents::TEvPoison());
                InnerWrapperId = {};
            }
            auto* mutableSettings = Settings.MutableSettings();
            mutableSettings->SetEndpoint(endpoint);
            auto externalStorageConfig = NWrappers::IExternalStorageConfig::Construct(
                AppData()->AwsClientConfig, *mutableSettings);
            auto storageOperator = externalStorageConfig->ConstructStorageOperator();
            storageOperator->InitReplyAdapter(std::make_shared<TRouterReplyAdapter>(
                TActivationContext::ActorSystem(), SelfId(), TEvPrivate::EvRefreshNow));
            InnerWrapperId = Register(NWrappers::CreateStorageWrapper(std::move(storageOperator)));
            CurrentEndpoint = endpoint;
        }

        bool BalancerEnabled() const {
            return Settings.HasBalancerHost() && Settings.GetBalancerHost();
        }

        void IssueBalancerRequest() {
            if (RefreshInFlight || !BalancerEnabled()) {
                return;
            }
            if (!HttpProxyId) {
                HttpProxyId = Register(NHttp::CreateHttpProxy());
            }
            const TString url = TStringBuilder() << "http://" << Settings.GetBalancerHost() << "/";
            Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(
                NHttp::THttpOutgoingRequest::CreateRequestGet(url),
                TDuration::Seconds(10)));
            RefreshInFlight = true;
        }

        void ScheduleNextRefresh() {
            if (!RefreshScheduled && BalancerEnabled()) {
                TActivationContext::Schedule(NextRefreshDelay(),
                    new IEventHandle(TEvPrivate::EvBalancerTick, 0, SelfId(), SelfId(), nullptr, 0));
                RefreshScheduled = true;
            }
        }

        void HandleBalancerTick() {
            RefreshScheduled = false;
            IssueBalancerRequest();
        }

        void HandleRefreshNow() {
            if (!RefreshInFlight) {
                IssueBalancerRequest();
            }
        }

        void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr ev) {
            RefreshInFlight = false;
            const auto& msg = *ev->Get();
            if (msg.Response && msg.Response->Status.StartsWith("2")) {
                TString endpoint = TString(StripString(msg.Response->Body));
                if (!endpoint.empty() && endpoint != CurrentEndpoint) {
                    BuildInnerWrapper(endpoint);
                }
            }
            ScheduleNextRefresh();
        }

        void Forward(STATEFN_SIG) {
            if (!InnerWrapperId) {
                return;
            }
            TActivationContext::Send(ev->Forward(InnerWrapperId));
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BLOB_DEPOT_S3_ROUTER;
        }

        explicit TBlobDepotS3Router(NKikimrBlobDepot::TS3BackendSettings settings)
            : Settings(std::move(settings))
        {}

        void Bootstrap() {
            const TString& endpoint = Settings.GetSettings().GetEndpoint();
            BuildInnerWrapper(endpoint);
            if (BalancerEnabled()) {
                IssueBalancerRequest();
            }
            Become(&TThis::StateWork);
        }

        void PassAway() override {
            if (InnerWrapperId) {
                Send(InnerWrapperId, new TEvents::TEvPoison());
                InnerWrapperId = {};
            }
            if (HttpProxyId) {
                Send(HttpProxyId, new TEvents::TEvPoison());
                HttpProxyId = {};
            }
            TActor::PassAway();
        }

        STATEFN(StateWork) {
            const ui32 type = ev->GetTypeRewrite();
            if (type >= NWrappers::NExternalStorage::EvBegin && type < NWrappers::NExternalStorage::EvEnd) {
                Forward(ev);
                return;
            }
            switch (type) {
                hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
                cFunc(TEvPrivate::EvBalancerTick, HandleBalancerTick);
                cFunc(TEvPrivate::EvRefreshNow, HandleRefreshNow);
                cFunc(TEvents::TSystem::Poison, PassAway);
            }
        }
    };

    } // anonymous

    IActor* CreateBlobDepotS3Router(NKikimrBlobDepot::TS3BackendSettings settings) {
        return new TBlobDepotS3Router(std::move(settings));
    }

} // NKikimr::NBlobDepot
