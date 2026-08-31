#include "s3_router.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/events/abstract.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/wrappers/unavailable_storage.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/random_provider/random_provider.h>

#include <util/generic/ptr.h>
#include <util/string/cast.h>
#include <util/string/strip.h>

#include <atomic>
#include <deque>

namespace NKikimr::NBlobDepot {

    namespace {

    struct TRouteMonCounters {
        NMonitoring::TDynamicCounters::TCounterPtr Requests;
        NMonitoring::TDynamicCounters::TCounterPtr Errors;
        NMonitoring::TDynamicCounters::TCounterPtr BytesRead;
        NMonitoring::TDynamicCounters::TCounterPtr BytesWritten;
        NMonitoring::THistogramPtr Latency;
    };

    struct TRouterMonCounters {
        TRouteMonCounters Balancer;
        TRouteMonCounters NonBalancer;
        NMonitoring::TDynamicCounters::TCounterPtr BalancerResolveRequests;
        NMonitoring::TDynamicCounters::TCounterPtr BalancerResolveSuccesses;
        NMonitoring::TDynamicCounters::TCounterPtr BalancerResolveFailures;
        NMonitoring::TDynamicCounters::TCounterPtr EndpointSwitches;
        NMonitoring::TDynamicCounters::TCounterPtr FiveXxRefreshTriggers;
        NMonitoring::TDynamicCounters::TCounterPtr PendingRejects;
        NMonitoring::TDynamicCounters::TCounterPtr RetiringWrappersAborted;
        NMonitoring::TDynamicCounters::TCounterPtr IsUsingProxy;
        NMonitoring::THistogramPtr BalancerResolveLatency;
        NMonitoring::THistogramPtr PendingLatency;
    };

    static NMonitoring::IHistogramCollectorPtr MakeLatencyHistogram() {
        return NMonitoring::ExplicitHistogram({
            1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000
        });
    }

    static void IncCounter(const NMonitoring::TDynamicCounters::TCounterPtr& counter, ui64 value = 1) {
        if (counter && value) {
            *counter += value;
        }
    }

    static void SetCounter(const NMonitoring::TDynamicCounters::TCounterPtr& counter, i64 value) {
        if (counter) {
            *counter = value;
        }
    }

    static void CollectHistogram(const NMonitoring::THistogramPtr& histogram, ui64 valueMs) {
        if (histogram) {
            histogram->Collect(static_cast<i64>(valueMs));
        }
    }

    class TRouteCounters : public TThrRefBase {
        TRouteMonCounters Route;

    public:
        explicit TRouteCounters(TRouteMonCounters route)
            : Route(std::move(route))
        {}

        void Collect(const NWrappers::NExternalStorage::IReplyAdapter::TRequestStats& requestStats) const {
            IncCounter(Route.Requests);
            if (requestStats.Success) {
                IncCounter(Route.BytesRead, requestStats.BytesRead);
                IncCounter(Route.BytesWritten, requestStats.BytesWritten);
            } else {
                IncCounter(Route.Errors);
            }
            CollectHistogram(Route.Latency, requestStats.Latency.MilliSeconds());
        }
    };

    class TWrapperInFlight : public TThrRefBase {
        std::atomic<i64> Count{0};

    public:
        void Inc() {
            Count.fetch_add(1, std::memory_order_relaxed);
        }

        void Dec() {
            Count.fetch_sub(1, std::memory_order_release);
        }

        i64 Get() const {
            return Count.load(std::memory_order_acquire);
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
        const TIntrusivePtr<TWrapperInFlight> InFlight;

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
                TIntrusivePtr<TRouteCounters> counters, TIntrusivePtr<TWrapperInFlight> inFlight)
            : ActorSystem(actorSystem)
            , RouterId(routerId)
            , NotifyEventType(notifyEventType)
            , Counters(std::move(counters))
            , InFlight(std::move(inFlight))
        {}

        void CollectStats(const TRequestStats& stats) const override {
            if (Counters) {
                Counters->Collect(stats);
            }

            if (InFlight) {
                InFlight->Dec();
            }
        }

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
                EvSweepRetiring,
            };
        };

        NKikimrBlobDepot::TS3BackendSettings Settings;
        ui64 TabletId = 0;
        TString LogId;
        TString OriginalEndpoint;
        TString CurrentEndpoint;
        TActorId InnerWrapperId;
        TIntrusivePtr<TWrapperInFlight> InnerWrapperInFlight;

        struct TRetiringWrapper {
            TActorId ActorId;
            TIntrusivePtr<TWrapperInFlight> InFlight;
            TMonotonic Deadline;
            TString Endpoint;
        };

        std::deque<TRetiringWrapper> RetiringWrappers;

        TActorId HttpProxyId;
        bool RefreshInFlight = false;
        bool RefreshScheduled = false;

        struct TPendingRequest {
            TMonotonic EnqueuedAt;
            std::unique_ptr<IEventHandle> Ev;
        };

        static constexpr size_t MaxPendingRequests = 256;
        std::deque<TPendingRequest> PendingRequests;

        TRouterMonCounters Mon;

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

        size_t MaxRetiringWrappers() const {
            return Settings.GetMaxRetiringWrappers();
        }

        TDuration MinRetireGracePeriod() const {
            return TDuration::Seconds(Settings.GetMinRetireGracePeriodSec());
        }

        TDuration SweepRetiringInterval() const {
            const ui32 ms = Settings.GetMetricsPushIntervalMs();
            return TDuration::MilliSeconds(ms ? ms : 2500);
        }

        static TRouteMonCounters MakeRouteMonCounters(::NMonitoring::TDynamicCounterPtr group) {
            TRouteMonCounters route;
            route.Requests = group->GetCounter("Requests", true);
            route.Errors = group->GetCounter("Errors", true);
            route.BytesRead = group->GetCounter("BytesRead", true);
            route.BytesWritten = group->GetCounter("BytesWritten", true);
            route.Latency = group->GetHistogram("LatencyMs", MakeLatencyHistogram());
            return route;
        }

        void SetupCounters() {
            auto group = GetServiceCounters(AppData()->Counters, "tablets")
                ->GetSubgroup("subsystem", "blob_depot")
                ->GetSubgroup("module_id", "s3_router")
                ->GetSubgroup("tablet", ::ToString(TabletId));

            Mon.Balancer = MakeRouteMonCounters(group->GetSubgroup("route", "Balancer"));
            Mon.NonBalancer = MakeRouteMonCounters(group->GetSubgroup("route", "NonBalancer"));

            auto resolve = group->GetSubgroup("component", "BalancerResolve");
            Mon.BalancerResolveRequests = resolve->GetCounter("Requests", true);
            Mon.BalancerResolveSuccesses = resolve->GetCounter("Successes", true);
            Mon.BalancerResolveFailures = resolve->GetCounter("Failures", true);
            Mon.BalancerResolveLatency = resolve->GetHistogram("LatencyMs", MakeLatencyHistogram());

            Mon.EndpointSwitches = group->GetCounter("EndpointSwitches", true);
            Mon.FiveXxRefreshTriggers = group->GetCounter("FiveXxRefreshTriggers", true);
            Mon.PendingRejects = group->GetSubgroup("component", "Pending")->GetCounter("Rejects", true);
            Mon.PendingLatency = group->GetSubgroup("component", "Pending")->GetHistogram("LatencyMs", MakeLatencyHistogram());
            Mon.RetiringWrappersAborted = group->GetCounter("RetiringWrappersAborted", true);
            Mon.IsUsingProxy = group->GetCounter("IsUsingProxy", false);
        }

        TIntrusivePtr<TRouteCounters> MakeRouteCounters(bool nonBalancer) {
            return MakeIntrusive<TRouteCounters>(nonBalancer ? Mon.NonBalancer : Mon.Balancer);
        }

        ui16 BalancerProxyPort() const {
            return Settings.GetBalancerProxyPort();
        }

        void RetireInnerWrapper() {
            if (!InnerWrapperId) {
                return;
            }

            const i64 inFlight = InnerWrapperInFlight ? InnerWrapperInFlight->Get() : 0;
            Y_ABORT_UNLESS(inFlight >= 0);
            if (inFlight == 0) {
                Send(InnerWrapperId, new TEvents::TEvPoison());
            } else {
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS35, "S3Router retiring inner wrapper",
                    (Id, LogId), (Endpoint, CurrentEndpoint), (InFlight, inFlight),
                    (RetiringCount, RetiringWrappers.size() + 1));

                RetiringWrappers.push_back(TRetiringWrapper{
                    .ActorId = InnerWrapperId,
                    .InFlight = InnerWrapperInFlight,
                    .Deadline = TActivationContext::Monotonic() + RetireGracePeriod(),
                    .Endpoint = CurrentEndpoint,
                });
            }

            InnerWrapperId = {};
            InnerWrapperInFlight.Reset();

            while (RetiringWrappers.size() > MaxRetiringWrappers()) {
                PoisonRetiringWrapper(RetiringWrappers.front(), "too many retiring wrappers");
                RetiringWrappers.pop_front();
            }
        }

        TDuration RetireGracePeriod() const {
            const auto& config = AppData()->AwsClientConfig;
            const ui32 timeoutMs = Max(
                config.HasRequestTimeoutMs() ? config.GetRequestTimeoutMs() : 0u,
                config.HasHttpRequestTimeoutMs() ? config.GetHttpRequestTimeoutMs() : 0u);
            return Max(TDuration::MilliSeconds(timeoutMs) * 2, MinRetireGracePeriod());
        }

        void PoisonRetiringWrapper(const TRetiringWrapper& wrapper, const char *reason) {
            const i64 inFlight = wrapper.InFlight->Get();
            Y_ABORT_UNLESS(inFlight >= 0);
            if (inFlight > 0) {
                STLOG(PRI_WARN, BLOB_DEPOT, BDTS36, "S3Router aborting requests of retiring inner wrapper",
                    (Id, LogId), (Endpoint, wrapper.Endpoint), (InFlight, inFlight), (Reason, reason));
                IncCounter(Mon.RetiringWrappersAborted);
            }

            Send(wrapper.ActorId, new TEvents::TEvPoison());
        }

        void SweepRetiringWrappers() {
            const TMonotonic now = TActivationContext::Monotonic();
            for (auto it = RetiringWrappers.begin(); it != RetiringWrappers.end(); ) {
                const i64 inFlight = it->InFlight->Get();
                Y_ABORT_UNLESS(inFlight >= 0);
                if (inFlight == 0) {
                    Send(it->ActorId, new TEvents::TEvPoison());
                    it = RetiringWrappers.erase(it);
                } else if (now >= it->Deadline) {
                    PoisonRetiringWrapper(*it, "grace period expired");
                    it = RetiringWrappers.erase(it);
                } else {
                    ++it;
                }
            }
        }

        void RegisterInnerWrapper(NWrappers::IExternalStorageConfig::TPtr externalStorageConfig,
                TIntrusivePtr<TRouteCounters> routeCounters) {
            RetireInnerWrapper();

            auto inFlight = MakeIntrusive<TWrapperInFlight>();
            auto storageOperator = externalStorageConfig->ConstructStorageOperator();
            storageOperator->InitReplyAdapter(std::make_shared<TRouterReplyAdapter>(
                TActivationContext::ActorSystem(), SelfId(), TEvPrivate::EvRefreshNow,
                std::move(routeCounters), inFlight));
            InnerWrapperId = Register(NWrappers::CreateStorageWrapper(std::move(storageOperator)));
            InnerWrapperInFlight = std::move(inFlight);
            FlushPendingRequests();
        }

        void RejectRequest(std::unique_ptr<IEventHandle> ev) {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS32, "S3Router has no endpoint yet, rejecting request",
                (Id, LogId), (Type, ev->GetTypeRewrite()), (Pending, PendingRequests.size()));

            IncCounter(Mon.PendingRejects);

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
            CollectHistogram(Mon.PendingLatency, (now - pending.EnqueuedAt).MilliSeconds());
        }

        void FlushPendingRequests() {
            Y_ABORT_UNLESS(InnerWrapperId);
            if (PendingRequests.empty()) {
                return;
            }

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS33, "S3Router flushing pending requests",
                (Id, LogId), (Count, PendingRequests.size()), (Endpoint, CurrentEndpoint));

            const TMonotonic now = TActivationContext::Monotonic();
            while (!PendingRequests.empty()) {
                auto pending = std::move(PendingRequests.front());
                PendingRequests.pop_front();
                RecordPendingLatency(pending, now);
                TrackAndForward(std::move(pending.Ev));
            }
        }

        void BuildInnerWrapper(const TString& endpoint) {
            auto* mutableSettings = Settings.MutableSettings();
            mutableSettings->SetEndpoint(endpoint);
            RegisterInnerWrapper(NWrappers::IExternalStorageConfig::Construct(
                AppData()->AwsClientConfig, *mutableSettings),
                MakeRouteCounters(false));
            CurrentEndpoint = endpoint;

            STLOG(PRI_INFO, BLOB_DEPOT, BDTS25, "S3Router endpoint set (direct)",
                (Id, LogId), (Endpoint, endpoint));

            SetCounter(Mon.IsUsingProxy, 0);
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

            STLOG(PRI_INFO, BLOB_DEPOT, BDTS26, "S3Router endpoint switch (via proxy)",
                (Id, LogId), (From, prevEndpoint), (To, CurrentEndpoint),
                (ProxyHost, host), (ProxyPort, port));

            IncCounter(Mon.EndpointSwitches);
            SetCounter(Mon.IsUsingProxy, 1);
        }

        bool BalancerEnabled() const {
            return Settings.HasBalancerHost() && Settings.GetBalancerHost();
        }

        void ScheduleSweepRetiring() {
            TActivationContext::Schedule(SweepRetiringInterval(), new IEventHandle(TEvPrivate::EvSweepRetiring, 0,
                SelfId(), {}, nullptr, 0));
        }

        void HandleSweepRetiring() {
            SweepRetiringWrappers();
            ScheduleSweepRetiring();
        }

        void IssueBalancerRequest() {
            if (RefreshInFlight || !BalancerEnabled()) {
                return;
            }
            if (!HttpProxyId) {
                HttpProxyId = Register(NHttp::CreateHttpProxy());
            }
            const TString url = TStringBuilder() << "http://" << Settings.GetBalancerHost();

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS27, "S3Router issuing balancer request",
                (Id, LogId), (Url, url));

            Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(
                NHttp::THttpOutgoingRequest::CreateRequestGet(url),
                TDuration::Seconds(10)));
            RefreshInFlight = true;
            BalancerRequestStartedAt = TActivationContext::Monotonic();
            IncCounter(Mon.BalancerResolveRequests);
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
            STLOG(PRI_WARN, BLOB_DEPOT, BDTS30, "S3Router 5xx detected, triggering endpoint refresh",
                (Id, LogId), (CurrentEndpoint, CurrentEndpoint));

            IncCounter(Mon.FiveXxRefreshTriggers);

            if (!RefreshInFlight) {
                IssueBalancerRequest();
            }
        }

        void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr ev) {
            RefreshInFlight = false;
            const TDuration latency = TActivationContext::Monotonic() - BalancerRequestStartedAt;
            CollectHistogram(Mon.BalancerResolveLatency, latency.MilliSeconds());

            const auto& msg = *ev->Get();
            if (msg.Response && msg.Response->Status.StartsWith("2")) {
                TString host = TString(StripString(msg.Response->Body));

                STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS28, "S3Router balancer response OK",
                    (Id, LogId), (Status, msg.Response->Status), (Body, host),
                    (LatencyMs, latency.MilliSeconds()));

                IncCounter(Mon.BalancerResolveSuccesses);

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
                STLOG(PRI_WARN, BLOB_DEPOT, BDTS29, "S3Router balancer response failure",
                    (Id, LogId), (HasResponse, msg.Response != nullptr),
                    (Status, msg.Response ? TString(msg.Response->Status) : TString("(no response)")),
                    (Error, msg.Error), (LatencyMs, latency.MilliSeconds()));

                IncCounter(Mon.BalancerResolveFailures);
            }
            ScheduleNextRefresh();
        }

        static bool IsRequestEvent(ui32 type) {
            using namespace NWrappers::NExternalStorage;
            static_assert(EvGetObjectRequest == EvBegin + 1);
            static_assert(EvGetObjectResponse == EvBegin + 2);
            return (type - EvBegin) % 2 == 1;
        }

        void TrackAndForward(std::unique_ptr<IEventHandle> ev) {
            if (InnerWrapperInFlight && IsRequestEvent(ev->GetTypeRewrite())) {
                InnerWrapperInFlight->Inc();
            }
            TActivationContext::Send(IEventHandle::Forward(std::move(ev), InnerWrapperId));
        }

        void Forward(STATEFN_SIG) {
            if (InnerWrapperId) {
                TrackAndForward(std::unique_ptr<IEventHandle>(ev.Release()));
                return;
            }

            if (PendingRequests.size() < MaxPendingRequests) {
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS34, "S3Router queueing request until balancer resolves",
                    (Id, LogId), (Type, ev->GetTypeRewrite()), (Pending, PendingRequests.size() + 1));
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
            SetupCounters();
            ScheduleSweepRetiring();

            STLOG(PRI_INFO, BLOB_DEPOT, BDTS24, "S3Router bootstrap",
                (Id, LogId), (Endpoint, OriginalEndpoint),
                (BalancerEnabled, BalancerEnabled()),
                (BalancerHost, BalancerEnabled() ? Settings.GetBalancerHost() : TString()));

            if (BalancerEnabled()) {
                IssueBalancerRequest();
                ScheduleNextRefresh();
            } else {
                BuildInnerWrapper(endpoint);
            }

            Become(&TThis::StateWork);
        }

        void PassAway() override {
            STLOG(PRI_INFO, BLOB_DEPOT, BDTS31, "S3Router shutting down",
                (Id, LogId), (CurrentEndpoint, CurrentEndpoint),
                (Pending, PendingRequests.size()), (RetiringCount, RetiringWrappers.size()));

            while (!PendingRequests.empty()) {
                RejectPendingRequest(std::move(PendingRequests.front()));
                PendingRequests.pop_front();
            }

            if (InnerWrapperId) {
                Send(InnerWrapperId, new TEvents::TEvPoison());
                InnerWrapperId = {};
                InnerWrapperInFlight.Reset();
            }

            for (auto&& wrapper : RetiringWrappers) {
                Send(wrapper.ActorId, new TEvents::TEvPoison());
            }

            RetiringWrappers.clear();
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
                cFunc(TEvPrivate::EvSweepRetiring, HandleSweepRetiring);
                cFunc(TEvents::TSystem::Poison, PassAway);
            }
        }
    };

    } // anonymous

    IActor* CreateBlobDepotS3Router(NKikimrBlobDepot::TS3BackendSettings settings, ui64 tabletId) {
        return new TBlobDepotS3Router(std::move(settings), tabletId);
    }

} // NKikimr::NBlobDepot
