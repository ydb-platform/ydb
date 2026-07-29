#include "s3_router.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/random_provider/random_provider.h>

#include <util/string/cast.h>
#include <util/string/strip.h>

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

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
        ui64 TabletId = 0;
        TString LogId;
        TString OriginalEndpoint;
        TString CurrentEndpoint;
        TActorId InnerWrapperId;
        TActorId HttpProxyId;
        bool RefreshInFlight = false;
        bool RefreshScheduled = false;

        NMonitoring::TDynamicCounters::TCounterPtr BalancerRequests;
        NMonitoring::TDynamicCounters::TCounterPtr BalancerSuccesses;
        NMonitoring::TDynamicCounters::TCounterPtr BalancerFailures;
        NMonitoring::TDynamicCounters::TCounterPtr EndpointSwitches;
        NMonitoring::TDynamicCounters::TCounterPtr FiveXxRefreshTriggers;
        NMonitoring::TDynamicCounters::TCounterPtr IsUsingProxy;

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

        void SetupCounters() {
            if (auto counters = AppData()->Counters) {
                auto group = GetServiceCounters(std::move(counters), "blob_depot")
                    ->GetSubgroup("tablet", ::ToString(TabletId))
                    ->GetSubgroup("subsystem", "s3_router");
                BalancerRequests      = group->GetCounter("BalancerRequests", true);
                BalancerSuccesses     = group->GetCounter("BalancerSuccesses", true);
                BalancerFailures      = group->GetCounter("BalancerFailures", true);
                EndpointSwitches      = group->GetCounter("EndpointSwitches", true);
                FiveXxRefreshTriggers = group->GetCounter("FiveXxRefreshTriggers", true);
                IsUsingProxy          = group->GetCounter("IsUsingProxy", false);
            }
        }

        ui16 BalancerProxyPort() const {
            return Settings.GetBalancerProxyPort();
        }

        void RegisterInnerWrapper(NWrappers::IExternalStorageConfig::TPtr externalStorageConfig) {
            if (InnerWrapperId) {
                Send(InnerWrapperId, new TEvents::TEvPoison());
                InnerWrapperId = {};
            }

            auto storageOperator = externalStorageConfig->ConstructStorageOperator();
            storageOperator->InitReplyAdapter(std::make_shared<TRouterReplyAdapter>(
                TActivationContext::ActorSystem(), SelfId(), TEvPrivate::EvRefreshNow));
            InnerWrapperId = Register(NWrappers::CreateStorageWrapper(std::move(storageOperator)));
        }

        void BuildInnerWrapper(const TString& endpoint) {
            auto* mutableSettings = Settings.MutableSettings();
            mutableSettings->SetEndpoint(endpoint);
            RegisterInnerWrapper(NWrappers::IExternalStorageConfig::Construct(
                AppData()->AwsClientConfig, *mutableSettings));
            CurrentEndpoint = endpoint;

            YDB_LOG_INFO("S3Router endpoint set (direct)",
                {"marker", "BDTS25"},
                {"id", LogId},
                {"endpoint", endpoint});

            if (IsUsingProxy) {
                *IsUsingProxy = 0;
            }
        }

        void BuildInnerWrapperViaProxy(const TString& host, ui16 port) {
            const TString prevEndpoint = CurrentEndpoint;
            auto* mutableSettings = Settings.MutableSettings();
            mutableSettings->SetEndpoint(OriginalEndpoint);
            mutableSettings->SetProxyHost(host);
            mutableSettings->SetProxyPort(port);
            mutableSettings->SetProxyScheme(Settings.GetBalancerProxyScheme());
            RegisterInnerWrapper(NWrappers::IExternalStorageConfig::Construct(
                AppData()->AwsClientConfig, *mutableSettings));
            CurrentEndpoint = TStringBuilder() << host << ':' << port;

            YDB_LOG_INFO("S3Router endpoint switch (via proxy)",
                {"marker", "BDTS26"},
                {"id", LogId},
                {"from", prevEndpoint},
                {"to", CurrentEndpoint},
                {"proxyHost", host},
                {"proxyPort", port});

            if (EndpointSwitches) {
                ++*EndpointSwitches;
            }
            if (IsUsingProxy) {
                *IsUsingProxy = 1;
            }
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
            const TString url = TStringBuilder() << "http://" << Settings.GetBalancerHost();

            YDB_LOG_DEBUG("S3Router issuing balancer request",
                {"marker", "BDTS27"},
                {"id", LogId},
                {"url", url});

            Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(
                NHttp::THttpOutgoingRequest::CreateRequestGet(url),
                TDuration::Seconds(10)));
            RefreshInFlight = true;
            if (BalancerRequests) {
                ++*BalancerRequests;
            }
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
            YDB_LOG_WARN("S3Router 5xx detected, triggering endpoint refresh",
                {"marker", "BDTS30"},
                {"id", LogId},
                {"currentEndpoint", CurrentEndpoint});

            if (FiveXxRefreshTriggers) {
                ++*FiveXxRefreshTriggers;
            }
            if (!RefreshInFlight) {
                IssueBalancerRequest();
            }
        }

        void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr ev) {
            RefreshInFlight = false;
            const auto& msg = *ev->Get();
            if (msg.Response && msg.Response->Status.StartsWith("2")) {
                TString host = TString(StripString(msg.Response->Body));

                YDB_LOG_DEBUG("S3Router balancer response OK",
                    {"marker", "BDTS28"},
                    {"id", LogId},
                    {"status", msg.Response->Status},
                    {"body", host});

                if (BalancerSuccesses) {
                    ++*BalancerSuccesses;
                }

                if (!host.empty()) {
                    ui16 port = BalancerProxyPort();
                    if (TStringBuf h, p; TStringBuf(host).TrySplit(':', h, p)) {
                        host = TString(h);
                        TryFromString(p, port);
                    }

                    const TString endpoint = TStringBuilder() << host << ':' << port;
                    if (endpoint != CurrentEndpoint) {
                        BuildInnerWrapperViaProxy(host, port);
                    }
                }
            } else {
                YDB_LOG_WARN("S3Router balancer response failure",
                    {"marker", "BDTS29"},
                    {"id", LogId},
                    {"hasResponse", msg.Response != nullptr},
                    {"status", msg.Response ? TString(msg.Response->Status) : TString("(no response)")},
                    {"error", msg.Error});

                if (BalancerFailures) {
                    ++*BalancerFailures;
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

        explicit TBlobDepotS3Router(NKikimrBlobDepot::TS3BackendSettings settings, ui64 tabletId)
            : Settings(std::move(settings))
            , TabletId(tabletId)
            , LogId(TStringBuilder() << '{' << tabletId << ":s3r}")
        {}

        void Bootstrap() {
            const TString& endpoint = Settings.GetSettings().GetEndpoint();
            OriginalEndpoint = endpoint;
            SetupCounters();
            BuildInnerWrapper(endpoint);

            YDB_LOG_INFO("S3Router bootstrap",
                {"marker", "BDTS24"},
                {"id", LogId},
                {"endpoint", OriginalEndpoint},
                {"balancerEnabled", BalancerEnabled()},
                {"balancerHost", BalancerEnabled() ? Settings.GetBalancerHost() : TString()});

            if (BalancerEnabled()) {
                IssueBalancerRequest();
            }
            Become(&TThis::StateWork);
        }

        void PassAway() override {
            YDB_LOG_INFO("S3Router shutting down",
                {"marker", "BDTS31"},
                {"id", LogId},
                {"currentEndpoint", CurrentEndpoint});

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

    IActor* CreateBlobDepotS3Router(NKikimrBlobDepot::TS3BackendSettings settings, ui64 tabletId) {
        return new TBlobDepotS3Router(std::move(settings), tabletId);
    }

} // NKikimr::NBlobDepot
