#include "s3_router.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/events/abstract.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/wrappers/unavailable_storage.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/services/scheme_secret/client/events.h>
#include <library/cpp/random_provider/random_provider.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/string/cast.h>
#include <util/string/strip.h>

#include <array>
#include <atomic>
#include <deque>

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {

    namespace {

    static constexpr size_t BalancerHostBucketCount = 16;

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

        struct THostBucket {
            NMonitoring::TDynamicCounters::TCounterPtr Selections;
            NMonitoring::THistogramPtr Latency;
        };

        std::array<THostBucket, BalancerHostBucketCount> BalancerHostBuckets;
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
        ui64 CredentialRequestId = 0;
        bool ResolvingCredentials = false;
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
            auto root = GetServiceCounters(AppData()->Counters, "tablets")
                ->GetSubgroup("subsystem", "blob_depot")
                ->GetSubgroup("module_id", "s3_router");
            auto group = root->GetSubgroup("tablet", ToString(TabletId));

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

            auto hostBuckets = root->GetSubgroup("component", "BalancerResolveByHostBucket");
            for (size_t i = 0; i < Mon.BalancerHostBuckets.size(); ++i) {
                auto bucket = hostBuckets->GetSubgroup("host_bucket", ToString(i));
                Mon.BalancerHostBuckets[i].Selections = bucket->GetCounter("Selections", true);
                Mon.BalancerHostBuckets[i].Latency = bucket->GetHistogram("LatencyMs", MakeLatencyHistogram());
            }
        }

        void RecordBalancerHostSelection(TStringBuf host, TDuration latency) {
            const size_t bucket = THash<TStringBuf>{}(host) % Mon.BalancerHostBuckets.size();
            IncCounter(Mon.BalancerHostBuckets[bucket].Selections);
            CollectHistogram(Mon.BalancerHostBuckets[bucket].Latency, latency.MilliSeconds());
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
                YDB_LOG_DEBUG("S3Router retiring inner wrapper",
                    {"marker", "BDTS35"},
                    {"id", LogId},
                    {"endpoint", CurrentEndpoint},
                    {"inFlight", inFlight},
                    {"retiringCount", RetiringWrappers.size() + 1});

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
                YDB_LOG_WARN("S3Router aborting requests of retiring inner wrapper",
                    {"marker", "BDTS36"},
                    {"id", LogId},
                    {"endpoint", wrapper.Endpoint},
                    {"inFlight", inFlight},
                    {"reason", reason});
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

        void ClearCredentials() {
            RetireInnerWrapper();
            Settings.MutableSettings()->ClearAccessKey();
            Settings.MutableSettings()->ClearSecretKey();
            ResolvingCredentials = false;
            while (!PendingRequests.empty()) {
                RejectPendingRequest(std::move(PendingRequests.front()));
                PendingRequests.pop_front();
            }
        }

        void ResolveCredentials() {
            if (!Settings.HasExternalDataSourceCredentials()) {
                return;
            }

            if (ResolvingCredentials) {
                // A resolver can disappear during a node reconfiguration. Bound the lifetime
                // of the old credentials and fence out a delayed result from that request.
                ClearCredentials();
            }

            const ui64 cookie = ++CredentialRequestId;
            Schedule(TDuration::Seconds(30), new TEvents::TEvWakeup());
            const auto& credentials = Settings.GetExternalDataSourceCredentials();
            const TString& database = credentials.GetDatabase();
            const auto isLocalSecret = [&database](const TString& path) {
                return path == CanonizePath(path) && path.StartsWith(database + "/");
            };

            if (!AppData()->FeatureFlags.GetEnableSchemaSecrets() || !database || database != CanonizePath(database)
                    || !credentials.GetOwnerSid() || !isLocalSecret(credentials.GetAccessKeySecretPath())
                    || !isLocalSecret(credentials.GetSecretKeySecretPath())) {
                ClearCredentials();
                return;
            }

            ResolvingCredentials = true;
            using TResult = NKqp::TEvDescribeSecretsResponse;
            auto promise = NThreading::NewPromise<TResult::TDescription>();
            promise.GetFuture().Subscribe([system = TActivationContext::ActorSystem(), self = SelfId(), cookie](
                    const NThreading::TFuture<TResult::TDescription>& future) {
                if (future.HasValue()) {
                    system->Send(new IEventHandle(self, self, new TResult(future.GetValue()), 0, cookie));
                }
            });
            auto token = MakeIntrusive<NACLib::TUserToken>(credentials.GetOwnerSid(), TVector<NACLib::TSID>{});
            Send(NSecret::MakeDescribeSchemaSecretServiceId(SelfId().NodeId()),
                new NSecret::TEvResolveSecret(token, database,
                    {credentials.GetAccessKeySecretPath(), credentials.GetSecretKeySecretPath()}, std::move(promise),
                    {.ForStoragePool = true}),
                IEventHandle::FlagTrackDelivery, cookie);
        }

        void Handle(NKqp::TEvDescribeSecretsResponse::TPtr ev) {
            if (ev->Sender != SelfId() || !ResolvingCredentials || ev->Cookie != CredentialRequestId) {
                return;
            }

            ResolvingCredentials = false;
            const auto& result = ev->Get()->Description;
            if (result.Status != Ydb::StatusIds::SUCCESS || result.SecretValues.size() != 2
                    || !result.SecretValues[0] || !result.SecretValues[1]) {
                ClearCredentials();
                return;
            }

            auto* s3 = Settings.MutableSettings();
            if (s3->GetAccessKey() == result.SecretValues[0] && s3->GetSecretKey() == result.SecretValues[1]) {
                return;
            }

            s3->SetAccessKey(result.SecretValues[0]);
            s3->SetSecretKey(result.SecretValues[1]);
            if (!BalancerEnabled()) {
                BuildInnerWrapper(OriginalEndpoint);
            } else if (s3->HasProxyHost()) {
                RegisterInnerWrapper(true);
            }
        }

        void Handle(TEvents::TEvUndelivered::TPtr ev) {
            if (ev->Get()->SourceType == NSecret::TEvResolveSecret::EventType && ev->Cookie == CredentialRequestId) {
                ResolvingCredentials = false;
                ClearCredentials();
            }
        }

        void RegisterInnerWrapper(bool proxy) {
            if (Settings.HasExternalDataSourceCredentials() && !Settings.GetSettings().HasSecretKey()) {
                return;
            }

            auto externalStorageConfig = NWrappers::IExternalStorageConfig::Construct(AppData()->AwsClientConfig, Settings.GetSettings());
            auto routeCounters = MakeRouteCounters(proxy);
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
            YDB_LOG_DEBUG("S3Router has no endpoint yet, rejecting request",
                {"marker", "BDTS32"},
                {"id", LogId},
                {"type", ev->GetTypeRewrite()},
                {"pending", PendingRequests.size()});

            IncCounter(Mon.PendingRejects);

            auto response = NWrappers::NExternalStorage::MakeErrorResponse(
                *ev,
                NWrappers::NExternalStorage::MakeServiceUnavailableError(
                    "ServiceUnavailable",
                    TStringBuilder() << (Settings.HasExternalDataSourceCredentials() && !Settings.GetSettings().HasSecretKey()
                        ? "S3 credentials are unavailable, id# " : "S3 endpoint is not resolved yet, id# ") << LogId));
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
                TrackAndForward(std::move(pending.Ev));
            }
        }

        void BuildInnerWrapper(const TString& endpoint) {
            auto* mutableSettings = Settings.MutableSettings();
            mutableSettings->SetEndpoint(endpoint);
            RegisterInnerWrapper(false);
            CurrentEndpoint = endpoint;

            YDB_LOG_INFO("S3Router endpoint set (direct)",
                {"marker", "BDTS25"},
                {"id", LogId},
                {"endpoint", endpoint});

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
            RegisterInnerWrapper(true);
            CurrentEndpoint = proxyEndpoint;

            YDB_LOG_INFO("S3Router endpoint switch (via proxy)",
                {"marker", "BDTS26"},
                {"id", LogId},
                {"from", prevEndpoint},
                {"to", CurrentEndpoint},
                {"proxyHost", host},
                {"proxyPort", port});

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

            YDB_LOG_DEBUG("S3Router issuing balancer request",
                {"marker", "BDTS27"},
                {"id", LogId},
                {"url", url});

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
            YDB_LOG_WARN("S3Router 5xx detected, triggering endpoint refresh",
                {"marker", "BDTS30"},
                {"id", LogId},
                {"currentEndpoint", CurrentEndpoint});

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

                YDB_LOG_DEBUG("S3Router balancer response OK",
                    {"marker", "BDTS28"},
                    {"id", LogId},
                    {"status", msg.Response->Status},
                    {"body", host},
                    {"latencyMs", latency.MilliSeconds()});

                IncCounter(Mon.BalancerResolveSuccesses);

                if (!host.empty()) {
                    ui16 port = BalancerProxyPort();
                    if (TStringBuf h, p; TStringBuf(host).TrySplit(':', h, p)) {
                        TryFromString(p, port);
                        host = TString(h);
                    }

                    RecordBalancerHostSelection(host, latency);

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

            if (Settings.HasExternalDataSourceCredentials() && !ResolvingCredentials && !Settings.GetSettings().HasSecretKey()) {
                RejectRequest(std::unique_ptr<IEventHandle>(ev.Release()));
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
            SetupCounters();
            ScheduleSweepRetiring();

            YDB_LOG_INFO("S3Router bootstrap",
                {"marker", "BDTS24"},
                {"id", LogId},
                {"endpoint", OriginalEndpoint},
                {"balancerEnabled", BalancerEnabled()},
                {"balancerHost", BalancerEnabled() ? Settings.GetBalancerHost() : TString()});

            if (Settings.HasExternalDataSourceCredentials()) {
                ClearCredentials();
                ResolveCredentials();
            }

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
                {"pending", PendingRequests.size()},
                {"retiringCount", RetiringWrappers.size()});

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
                hFunc(NKqp::TEvDescribeSecretsResponse, Handle);
                hFunc(TEvents::TEvUndelivered, Handle);
                cFunc(TEvents::TSystem::Wakeup, ResolveCredentials);
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
