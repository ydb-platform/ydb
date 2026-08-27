#include "s3_router.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/events/abstract.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/wrappers/unavailable_storage.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/random_provider/random_provider.h>

#include <util/string/cast.h>
#include <util/string/strip.h>

<<<<<<< HEAD
=======
#include <algorithm>
#include <atomic>
#include <deque>

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

>>>>>>> 106e608ed10 (do not bypass S3 non-balancer before resolve (#50972))
namespace NKikimr::NBlobDepot {

    namespace {

<<<<<<< HEAD
=======
    struct TLatencyHistogram {
        static constexpr ui64 Bounds[] = {
            1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000
        };

        static constexpr size_t BucketCount = std::size(Bounds) + 1; // last bucket is +inf

        std::atomic<ui64> Buckets[BucketCount] = {};

        void Record(ui64 valueMs) {
            const auto* end = Bounds + std::size(Bounds);
            const size_t i = std::lower_bound(Bounds, end, valueMs) - Bounds;
            Buckets[i].fetch_add(1, std::memory_order_relaxed);
        }

        template <typename TRepeated>
        void Take(TRepeated* out) {
            ui64 snapshot[BucketCount];
            bool any = false;
            for (size_t i = 0; i < BucketCount; ++i) {
                snapshot[i] = Buckets[i].exchange(0, std::memory_order_relaxed);
                any = any || snapshot[i];
            }

            if (!any) {
                return;
            }

            out->Clear();
            out->Reserve(BucketCount);
            for (size_t i = 0; i < BucketCount; ++i) {
                out->Add(snapshot[i]);
            }
        }
    };

    struct TRouteStats {
        std::atomic<ui64> Requests{0};
        std::atomic<ui64> Errors{0};
        std::atomic<ui64> BytesRead{0};
        std::atomic<ui64> BytesWritten{0};
        TLatencyHistogram Latency;
    };

    struct TRouterStats {
        TRouteStats BalancerRoute;
        TRouteStats NonBalancerRoute;

        std::atomic<ui64> BalancerResolveRequests{0};
        std::atomic<ui64> BalancerResolveSuccesses{0};
        std::atomic<ui64> BalancerResolveFailures{0};
        std::atomic<ui64> EndpointSwitches{0};
        std::atomic<ui64> FiveXxRefreshTriggers{0};
        std::atomic<ui64> PendingRejects{0};
        std::atomic<bool> IsUsingProxy{false};

        TLatencyHistogram BalancerResolveLatency;
        TLatencyHistogram PendingLatency;
    };

    class TRouteCounters : public TThrRefBase {
        TRouterStats& Stats;
        const bool NonBalancer;

    public:
        TRouteCounters(TRouterStats& stats, bool nonBalancer)
            : Stats(stats)
            , NonBalancer(nonBalancer)
        {}

        void Collect(const NWrappers::NExternalStorage::IReplyAdapter::TRequestStats& requestStats) const {
            TRouteStats& route = NonBalancer ? Stats.NonBalancerRoute : Stats.BalancerRoute;
            ++route.Requests;
            if (requestStats.Success) {
                route.BytesRead += requestStats.BytesRead;
                route.BytesWritten += requestStats.BytesWritten;
            } else {
                ++route.Errors;
            }

            route.Latency.Record(requestStats.Latency.MilliSeconds());
        }
    };

>>>>>>> 106e608ed10 (do not bypass S3 non-balancer before resolve (#50972))
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
        std::unique_ptr<IEventBase> RebuildReplyEvent(std::unique_ptr<NWrappers::NExternalStorage::TEv##NAME##Response>&& ev) const override { \
            return Inspect(std::move(ev)); \
        }

        Y_FOR_EACH_S3_WRAPPER_OP(IMPL_REBUILD)
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
        TString OriginalEndpoint;
        TString CurrentEndpoint;
        TActorId InnerWrapperId;
        TActorId HttpProxyId;
        bool RefreshInFlight = false;
        bool RefreshScheduled = false;

<<<<<<< HEAD
=======
        struct TPendingRequest {
            TMonotonic EnqueuedAt;
            std::unique_ptr<IEventHandle> Ev;
        };

        static constexpr size_t MaxPendingRequests = 256;
        std::deque<TPendingRequest> PendingRequests;

        TRouterStats Stats;

        TMonotonic BalancerRequestStartedAt;

>>>>>>> 106e608ed10 (do not bypass S3 non-balancer before resolve (#50972))
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
            FlushPendingRequests();
        }

        void RejectRequest(std::unique_ptr<IEventHandle> ev) {
            YDB_LOG_DEBUG("S3Router has no endpoint yet, rejecting request",
                {"marker", "BDTS32"},
                {"id", LogId},
                {"type", ev->GetTypeRewrite()},
                {"pending", PendingRequests.size()});

            ++Stats.PendingRejects;

            auto response = NWrappers::NExternalStorage::MakeErrorResponse(
                *ev,
                NWrappers::NExternalStorage::MakeServiceUnavailableError(
                    "ServiceUnavailable",
                    TStringBuilder() << "S3 endpoint is not resolved yet, id# " << LogId));
            Y_ABORT_UNLESS(response);
            Send(ev->Sender, response.release(), 0, ev->Cookie);
        }

        void RejectPendingRequest(TPendingRequest&& pending) {
            RecordPendingLatency(pending, TActivationContext::Monotonic());
            RejectRequest(std::move(pending.Ev));
        }

        void RecordPendingLatency(const TPendingRequest& pending, TMonotonic now) {
            Stats.PendingLatency.Record((now - pending.EnqueuedAt).MilliSeconds());
        }

        void FlushPendingRequests() {
            Y_ABORT_UNLESS(InnerWrapperId);
            if (PendingRequests.empty()) {
                return;
            }

            YDB_LOG_DEBUG("S3Router flushing pending requests",
                {"marker", "BDTS33"},
                {"id", LogId},
                {"count", PendingRequests.size()},
                {"endpoint", CurrentEndpoint});

            const TMonotonic now = TActivationContext::Monotonic();
            while (!PendingRequests.empty()) {
                auto pending = std::move(PendingRequests.front());
                PendingRequests.pop_front();
                RecordPendingLatency(pending, now);
                TActivationContext::Send(IEventHandle::Forward(std::move(pending.Ev), InnerWrapperId));
            }
        }

        void BuildInnerWrapper(const TString& endpoint) {
            auto* mutableSettings = Settings.MutableSettings();
            mutableSettings->SetEndpoint(endpoint);
            RegisterInnerWrapper(NWrappers::IExternalStorageConfig::Construct(
                AppData()->AwsClientConfig, *mutableSettings));
            CurrentEndpoint = endpoint;
        }

        void BuildInnerWrapperViaProxy(const TString& host, ui16 port) {
            auto* mutableSettings = Settings.MutableSettings();
            mutableSettings->SetEndpoint(OriginalEndpoint);
            mutableSettings->SetProxyHost(host);
            mutableSettings->SetProxyPort(port);
            mutableSettings->SetProxyScheme(Settings.GetBalancerProxyScheme());
            RegisterInnerWrapper(NWrappers::IExternalStorageConfig::Construct(
                AppData()->AwsClientConfig, *mutableSettings));
            CurrentEndpoint = TStringBuilder() << host << ':' << port;
        }

        bool BalancerEnabled() const {
            return Settings.HasBalancerHost() && Settings.GetBalancerHost();
        }

<<<<<<< HEAD
=======
        void CreatePipe() {
            Y_ABORT_UNLESS(!PipeId);
            PipeId = Register(NTabletPipe::CreateClient(SelfId(), TabletId,
                NTabletPipe::TClientRetryPolicy::WithRetries()));
        }

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
            auto& msg = *ev->Get();
            if (msg.ClientId != PipeId) {
                return;
            }
            if (msg.Status == NKikimrProto::OK) {
                PipeConnected = true;
            } else {
                PipeConnected = false;
                PipeId = {};
                CreatePipe();
            }
        }

        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev) {
            if (ev->Get()->ClientId != PipeId) {
                return;
            }

            PipeConnected = false;
            PipeId = {};
            CreatePipe();
        }

        void SchedulePushMetrics() {
            TActivationContext::Schedule(MetricsPushInterval(), new IEventHandle(TEvPrivate::EvPushMetrics, 0,
                SelfId(), {}, nullptr, 0));
        }

        void HandlePushMetrics() {
            if (PipeConnected) {
                auto event = std::make_unique<TEvBlobDepot::TEvPushS3RouterMetrics>();
                auto& record = event->Record;
                record.SetNodeId(SelfId().NodeId());

                record.SetBalancerRequests(ExchangeAtomic(Stats.BalancerRoute.Requests));
                record.SetBalancerErrors(ExchangeAtomic(Stats.BalancerRoute.Errors));

                record.SetNonBalancerRequests(ExchangeAtomic(Stats.NonBalancerRoute.Requests));
                record.SetNonBalancerErrors(ExchangeAtomic(Stats.NonBalancerRoute.Errors));
                record.SetNonBalancerBytesRead(ExchangeAtomic(Stats.NonBalancerRoute.BytesRead));
                record.SetNonBalancerBytesWritten(ExchangeAtomic(Stats.NonBalancerRoute.BytesWritten));

                record.SetBalancerResolveRequests(ExchangeAtomic(Stats.BalancerResolveRequests));
                record.SetBalancerResolveSuccesses(ExchangeAtomic(Stats.BalancerResolveSuccesses));
                record.SetBalancerResolveFailures(ExchangeAtomic(Stats.BalancerResolveFailures));
                record.SetEndpointSwitches(ExchangeAtomic(Stats.EndpointSwitches));
                record.SetFiveXxRefreshTriggers(ExchangeAtomic(Stats.FiveXxRefreshTriggers));
                record.SetPendingRejects(ExchangeAtomic(Stats.PendingRejects));
                record.SetIsUsingProxy(Stats.IsUsingProxy.load());

                Stats.BalancerRoute.Latency.Take(record.MutableBalancerLatencyHistogram());
                Stats.NonBalancerRoute.Latency.Take(record.MutableNonBalancerLatencyHistogram());
                Stats.BalancerResolveLatency.Take(record.MutableBalancerResolveLatencyHistogram());
                Stats.PendingLatency.Take(record.MutablePendingLatencyHistogram());

                NTabletPipe::SendData(SelfId(), PipeId, event.release());
            }

            SchedulePushMetrics();
        }

>>>>>>> 106e608ed10 (do not bypass S3 non-balancer before resolve (#50972))
        void IssueBalancerRequest() {
            if (RefreshInFlight || !BalancerEnabled()) {
                return;
            }
            if (!HttpProxyId) {
                HttpProxyId = Register(NHttp::CreateHttpProxy());
            }
            const TString url = TStringBuilder() << "http://" << Settings.GetBalancerHost();
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
            RefreshInFlight = false;
            IssueBalancerRequest();
            ScheduleNextRefresh();
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
                TString host = TString(StripString(msg.Response->Body));
                if (!host.empty()) {
                    ui16 port = BalancerProxyPort();
                    if (TStringBuf h, p; TStringBuf(host).TrySplit(':', h, p)) {
                        TryFromString(p, port);
                        host = TString(h);
                    }

                    const TString endpoint = TStringBuilder() << host << ':' << port;
                    if (endpoint != CurrentEndpoint) {
                        BuildInnerWrapperViaProxy(host, port);
                    }
                }
            }
            ScheduleNextRefresh();
        }

        void Forward(STATEFN_SIG) {
            if (InnerWrapperId) {
                TActivationContext::Send(ev->Forward(InnerWrapperId));
                return;
            }

            if (PendingRequests.size() < MaxPendingRequests) {
                YDB_LOG_DEBUG("S3Router queueing request until balancer resolves",
                    {"marker", "BDTS34"},
                    {"id", LogId},
                    {"type", ev->GetTypeRewrite()},
                    {"pending", PendingRequests.size() + 1});
                PendingRequests.push_back(TPendingRequest{
                    .EnqueuedAt = TActivationContext::Monotonic(),
                    .Ev = std::unique_ptr<IEventHandle>(ev.Release()),
                });
                return;
            }

            RejectRequest(std::unique_ptr<IEventHandle>(ev.Release()));
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
            OriginalEndpoint = endpoint;
<<<<<<< HEAD
            BuildInnerWrapper(endpoint);
=======
            CreatePipe();
            SchedulePushMetrics();

            YDB_LOG_INFO("S3Router bootstrap",
                {"marker", "BDTS24"},
                {"id", LogId},
                {"endpoint", OriginalEndpoint},
                {"balancerEnabled", BalancerEnabled()},
                {"balancerHost", BalancerEnabled() ? Settings.GetBalancerHost() : TString()});

>>>>>>> 106e608ed10 (do not bypass S3 non-balancer before resolve (#50972))
            if (BalancerEnabled()) {
                IssueBalancerRequest();
                ScheduleNextRefresh();
            } else {
                BuildInnerWrapper(endpoint);
            }

            Become(&TThis::StateWork);
        }

        void PassAway() override {
<<<<<<< HEAD
=======
            YDB_LOG_INFO("S3Router shutting down",
                {"marker", "BDTS31"},
                {"id", LogId},
                {"currentEndpoint", CurrentEndpoint},
                {"pending", PendingRequests.size()});

            while (!PendingRequests.empty()) {
                RejectPendingRequest(std::move(PendingRequests.front()));
                PendingRequests.pop_front();
            }

>>>>>>> 106e608ed10 (do not bypass S3 non-balancer before resolve (#50972))
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
