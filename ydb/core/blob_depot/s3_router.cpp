#include "s3_router.h"

#include "events.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/wrappers/unavailable_storage.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/random_provider/random_provider.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/string/cast.h>
#include <util/string/strip.h>

#include <algorithm>
#include <atomic>
#include <deque>

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {

    namespace {

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

    // Adapter installed on the inner storage wrapper. It does NOT redirect the response
    // (recipient stays at the original sender of the request), but it observes every
    // finished request: it reports its latency/traffic to the route sensors and notifies
    // the router actor when an HTTP 5xx is detected, so the router can refresh the
    // endpoint promptly.
    //
    // The adapter runs in AWS SDK callback threads, so ActorSystem is captured up front.
    class TRouterReplyAdapter : public NWrappers::NExternalStorage::IReplyAdapter {
        using IReplyAdapter = NWrappers::NExternalStorage::IReplyAdapter;
        TActorSystem* const ActorSystem;
        const TActorId RouterId;
        const ui32 NotifyEventType;
        const TIntrusivePtr<TRouteCounters> Counters;

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
        TRouterReplyAdapter(TActorSystem* actorSystem, TActorId routerId, ui32 notifyEventType,
                TIntrusivePtr<TRouteCounters> counters)
            : ActorSystem(actorSystem)
            , RouterId(routerId)
            , NotifyEventType(notifyEventType)
            , Counters(std::move(counters))
        {}

        void CollectStats(const TRequestStats& stats) const override {
            if (Counters) {
                Counters->Collect(stats);
            }
        }

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

    static ui64 ExchangeAtomic(std::atomic<ui64>& value) {
        return value.exchange(0);
    }

    // Answers requests the router cannot serve yet with a regular S3 error response. The base class
    // delays such replies through its backoff policy, which makes no sense here -- the caller has to
    // learn about the missing endpoint right away.
    class TNoEndpointStorageOperator : public NWrappers::NExternalStorage::TUnavailableExternalStorageOperator {
    public:
        TNoEndpointStorageOperator(const TString& exceptionName, const TString& reason)
            : TUnavailableExternalStorageOperator(exceptionName, reason)
        {
            BackoffPolicy = std::make_shared<NWrappers::NExternalStorage::TThreadSafeBackoff>(
                0, TDuration::Zero(), TDuration::Zero());
        }
    };

    class TBlobDepotS3Router : public TActorBootstrapped<TBlobDepotS3Router> {
        struct TEvPrivate {
            enum {
                EvBalancerTick = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvRefreshNow,
                EvPushMetrics,
            };
        };

        NKikimrBlobDepot::TS3BackendSettings Settings;
        ui64 TabletId = 0;
        TString LogId;
        TString OriginalEndpoint;
        TString CurrentEndpoint;
        TActorId InnerWrapperId;
        TActorId UnavailableWrapperId;
        TActorId HttpProxyId;
        TActorId PipeId;
        bool PipeConnected = false;
        bool RefreshInFlight = false;
        bool RefreshScheduled = false;

        struct TPendingRequest {
            TMonotonic EnqueuedAt;
            std::unique_ptr<IEventHandle> Ev;
        };

        static constexpr size_t MaxPendingRequests = 256;
        std::deque<TPendingRequest> PendingRequests;

        TRouterStats Stats;

        TMonotonic BalancerRequestStartedAt;

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

        TDuration MetricsPushInterval() const {
            const ui32 ms = Settings.GetMetricsPushIntervalMs();
            return TDuration::MilliSeconds(ms ? ms : 2500);
        }

        TIntrusivePtr<TRouteCounters> MakeRouteCounters(bool nonBalancer) {
            return MakeIntrusive<TRouteCounters>(Stats, nonBalancer);
        }

        ui16 BalancerProxyPort() const {
            return Settings.GetBalancerProxyPort();
        }

        void RegisterInnerWrapper(NWrappers::IExternalStorageConfig::TPtr externalStorageConfig,
                TIntrusivePtr<TRouteCounters> routeCounters) {
            if (InnerWrapperId) {
                Send(InnerWrapperId, new TEvents::TEvPoison());
                InnerWrapperId = {};
            }

            auto storageOperator = externalStorageConfig->ConstructStorageOperator();
            storageOperator->InitReplyAdapter(std::make_shared<TRouterReplyAdapter>(
                TActivationContext::ActorSystem(), SelfId(), TEvPrivate::EvRefreshNow,
                std::move(routeCounters)));
            InnerWrapperId = Register(NWrappers::CreateStorageWrapper(std::move(storageOperator)));
            FlushPendingRequests();
        }

        TActorId GetUnavailableWrapperId() {
            if (!UnavailableWrapperId) {
                auto storageOperator = std::make_shared<TNoEndpointStorageOperator>(
                    "ServiceUnavailable", TStringBuilder() << "S3 endpoint is not resolved yet, id# " << LogId);
                storageOperator->InitReplyAdapter(std::make_shared<TRouterReplyAdapter>(
                    TActivationContext::ActorSystem(), SelfId(), TEvPrivate::EvRefreshNow, nullptr));
                UnavailableWrapperId = Register(NWrappers::CreateStorageWrapper(std::move(storageOperator)));
            }

            return UnavailableWrapperId;
        }

        void RejectRequest(std::unique_ptr<IEventHandle> ev) {
            YDB_LOG_DEBUG("S3Router has no endpoint yet, rejecting request",
                {"marker", "BDTS32"},
                {"id", LogId},
                {"type", ev->GetTypeRewrite()},
                {"pending", PendingRequests.size()});

            ++Stats.PendingRejects;
            TActivationContext::Send(IEventHandle::Forward(std::move(ev), GetUnavailableWrapperId()));
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
                AppData()->AwsClientConfig, *mutableSettings),
                MakeRouteCounters(false));
            CurrentEndpoint = endpoint;

            YDB_LOG_INFO("S3Router endpoint set (direct)",
                {"marker", "BDTS25"},
                {"id", LogId},
                {"endpoint", endpoint});

            Stats.IsUsingProxy.store(false);
        }

        void BuildInnerWrapperViaProxy(const TString& host, ui16 port) {
            const TString prevEndpoint = CurrentEndpoint;
            auto* mutableSettings = Settings.MutableSettings();
            mutableSettings->SetEndpoint(OriginalEndpoint);
            mutableSettings->SetProxyHost(host);
            mutableSettings->SetProxyPort(port);
            mutableSettings->SetProxyScheme(Settings.GetBalancerProxyScheme());
            const TString proxyEndpoint = TStringBuilder() << host << ':' << port;
            RegisterInnerWrapper(NWrappers::IExternalStorageConfig::Construct(
                AppData()->AwsClientConfig, *mutableSettings),
                MakeRouteCounters(true));
            CurrentEndpoint = proxyEndpoint;

            YDB_LOG_INFO("S3Router endpoint switch (via proxy)",
                {"marker", "BDTS26"},
                {"id", LogId},
                {"from", prevEndpoint},
                {"to", CurrentEndpoint},
                {"proxyHost", host},
                {"proxyPort", port});

            ++Stats.EndpointSwitches;
            Stats.IsUsingProxy.store(true);
        }

        bool BalancerEnabled() const {
            return Settings.HasBalancerHost() && Settings.GetBalancerHost();
        }

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
            BalancerRequestStartedAt = TActivationContext::Monotonic();
            ++Stats.BalancerResolveRequests;
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
            YDB_LOG_WARN("S3Router 5xx detected, triggering endpoint refresh",
                {"marker", "BDTS30"},
                {"id", LogId},
                {"currentEndpoint", CurrentEndpoint});

            ++Stats.FiveXxRefreshTriggers;

            if (!RefreshInFlight) {
                IssueBalancerRequest();
            }
        }

        void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr ev) {
            RefreshInFlight = false;
            const TDuration latency = TActivationContext::Monotonic() - BalancerRequestStartedAt;
            Stats.BalancerResolveLatency.Record(latency.MilliSeconds());

            const auto& msg = *ev->Get();
            if (msg.Response && msg.Response->Status.StartsWith("2")) {
                TString host = TString(StripString(msg.Response->Body));

                YDB_LOG_DEBUG("S3Router balancer response OK",
                    {"marker", "BDTS28"},
                    {"id", LogId},
                    {"status", msg.Response->Status},
                    {"body", host},
                    {"latencyMs", latency.MilliSeconds()});

                ++Stats.BalancerResolveSuccesses;

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
            } else {
                YDB_LOG_WARN("S3Router balancer response failure",
                    {"marker", "BDTS29"},
                    {"id", LogId},
                    {"hasResponse", msg.Response != nullptr},
                    {"status", msg.Response ? TString(msg.Response->Status) : TString("(no response)")},
                    {"error", msg.Error},
                    {"latencyMs", latency.MilliSeconds()});

                ++Stats.BalancerResolveFailures;
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

        explicit TBlobDepotS3Router(NKikimrBlobDepot::TS3BackendSettings settings, ui64 tabletId)
            : Settings(std::move(settings))
            , TabletId(tabletId)
            , LogId(TStringBuilder() << '{' << tabletId << ":s3r}")
        {}

        void Bootstrap() {
            const TString& endpoint = Settings.GetSettings().GetEndpoint();
            OriginalEndpoint = endpoint;
            CreatePipe();
            SchedulePushMetrics();

            YDB_LOG_INFO("S3Router bootstrap",
                {"marker", "BDTS24"},
                {"id", LogId},
                {"endpoint", OriginalEndpoint},
                {"balancerEnabled", BalancerEnabled()},
                {"balancerHost", BalancerEnabled() ? Settings.GetBalancerHost() : TString()});

            if (BalancerEnabled()) {
                IssueBalancerRequest();
                ScheduleNextRefresh();
            } else {
                BuildInnerWrapper(endpoint);
            }

            Become(&TThis::StateWork);
        }

        void PassAway() override {
            YDB_LOG_INFO("S3Router shutting down",
                {"marker", "BDTS31"},
                {"id", LogId},
                {"currentEndpoint", CurrentEndpoint},
                {"pending", PendingRequests.size()});

            while (!PendingRequests.empty()) {
                RejectPendingRequest(std::move(PendingRequests.front()));
                PendingRequests.pop_front();
            }

            if (InnerWrapperId) {
                Send(InnerWrapperId, new TEvents::TEvPoison());
                InnerWrapperId = {};
            }
            if (UnavailableWrapperId) {
                Send(UnavailableWrapperId, new TEvents::TEvPoison());
                UnavailableWrapperId = {};
            }
            if (HttpProxyId) {
                Send(HttpProxyId, new TEvents::TEvPoison());
                HttpProxyId = {};
            }
            if (PipeId) {
                NTabletPipe::CloseAndForgetClient(SelfId(), PipeId);
                PipeId = {};
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
                hFunc(TEvTabletPipe::TEvClientConnected, Handle);
                hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
                cFunc(TEvPrivate::EvBalancerTick, HandleBalancerTick);
                cFunc(TEvPrivate::EvRefreshNow, HandleRefreshNow);
                cFunc(TEvPrivate::EvPushMetrics, HandlePushMetrics);
                cFunc(TEvents::TSystem::Poison, PassAway);
            }
        }
    };

    } // anonymous

    IActor* CreateBlobDepotS3Router(NKikimrBlobDepot::TS3BackendSettings settings, ui64 tabletId) {
        return new TBlobDepotS3Router(std::move(settings), tabletId);
    }

} // NKikimr::NBlobDepot
