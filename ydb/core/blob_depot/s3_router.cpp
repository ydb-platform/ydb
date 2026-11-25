#include "s3_router.h"
#include "s3_put_timeout.h"
#include "s3_request_timer.h"
#include "s3_router_events.h"
#include "types.h"
#include "wakeup_timer.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/events/abstract.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/wrappers/unavailable_storage.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/aws_init/cancellable_request.h>
#include <library/cpp/random_provider/random_provider.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/string/cast.h>
#include <util/string/strip.h>
#include <util/system/mutex.h>

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
        std::atomic_bool Cancelled{false};

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

        void Cancel() { Cancelled.store(true, std::memory_order_relaxed); }
        bool IsCancelled() const { return Cancelled.load(std::memory_order_relaxed); }
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
                EvExpirePending,
            };
        };

        const NKikimrBlobDepot::TS3BackendSettings Settings;
        const TString DeleteRoute;
        ui64 TabletId = 0;
        TString LogId;
        const std::shared_ptr<TS3BalancerEndpoint> LastEndpoint;
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
            TMonotonic Deadline = TMonotonic::Max();
            std::unique_ptr<IEventHandle> Ev;
        };

        static constexpr size_t MaxPendingRequests = 256;
        std::deque<TPendingRequest> PendingRequests;
        TWakeupTimer PendingTimer;
        TDuration TransferTimeout;
        TControlWrapper MaxAgentGets = 32;
        TControlWrapper MaxAgentPuts = 32;
        TControlWrapper MaxDeletes = 3;
        // The router and physical SDK operations share these counts across wrapper and router replacement.
        struct TSdkRequests : std::enable_shared_from_this<TSdkRequests> {
            using TKey = std::pair<TActorSystem*, ui64>;
            const TKey Key;
            std::atomic<size_t> AgentGets{0};
            std::atomic<size_t> AgentPuts{0};
            std::atomic<size_t> CoolingPuts{0};
            std::atomic<size_t> Deletes{0};

            struct TRegistry {
                TMutex Mutex;
                THashMap<TKey, std::weak_ptr<TSdkRequests>> Tablets;
            };

            static std::shared_ptr<TSdkRequests> Get(TActorSystem* system, ui64 tabletId) {
                auto& registry = *Singleton<TRegistry>();
                TGuard guard(registry.Mutex);
                const TKey key(system, tabletId);
                auto& entry = registry.Tablets[key];
                if (auto current = entry.lock()) {
                    return current;
                }
                auto tracker = std::make_shared<TSdkRequests>(key);
                entry = tracker;
                return tracker;
            }

            explicit TSdkRequests(TKey key) : Key(key) {}
            ~TSdkRequests() {
                auto& registry = *Singleton<TRegistry>();
                TGuard guard(registry.Mutex);
                const auto it = registry.Tablets.find(Key);
                if (it != registry.Tablets.end() && it->second.expired()) {
                    registry.Tablets.erase(it);
                }
            }

            struct TSlot {
                std::shared_ptr<TSdkRequests> Owner;
                std::atomic<size_t>& Count;

                TSlot(std::shared_ptr<TSdkRequests> owner, std::atomic<size_t>& count)
                    : Owner(std::move(owner))
                    , Count(count)
                {}
                ~TSlot() { Count.fetch_sub(1, std::memory_order_relaxed); }
            };

            std::shared_ptr<TSlot> Acquire(std::atomic<size_t>& count, size_t limit) {
                if (count.fetch_add(1, std::memory_order_relaxed) >= limit) {
                    count.fetch_sub(1, std::memory_order_relaxed);
                    return {};
                }
                return std::make_shared<TSlot>(shared_from_this(), count);
            }
        };
        std::shared_ptr<TSdkRequests> SdkRequests;

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

            wrapper.InFlight->Cancel();
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

        void RejectRequest(std::unique_ptr<IEventHandle> ev, TStringBuf reason = "S3 endpoint is not resolved yet") {
            YDB_LOG_DEBUG("S3Router rejecting request",
                {"marker", "BDTS32"},
                {"id", LogId},
                {"type", ev->GetTypeRewrite()},
                {"pending", PendingRequests.size()},
                {"reason", reason});

            auto response = NWrappers::NExternalStorage::MakeErrorResponse(
                *ev,
                NWrappers::NExternalStorage::MakeServiceUnavailableError(
                    "ServiceUnavailable",
                    TStringBuilder() << reason << ", id# " << LogId));
            Y_ABORT_UNLESS(response);
            Send(ev->Sender, response.release(), 0, ev->Cookie);
        }

        void RejectPendingRequest(TPendingRequest&& pending, TStringBuf reason = "S3 endpoint is not resolved yet") {
            RecordPendingLatency(pending, TActivationContext::Monotonic());
            IncCounter(Mon.PendingRejects);
            RejectRequest(std::move(pending.Ev), reason);
        }

        void RecordPendingLatency(const TPendingRequest& pending, TMonotonic now) {
            CollectHistogram(Mon.PendingLatency, (now - pending.EnqueuedAt).MilliSeconds());
        }

        void ExpirePending(TAutoPtr<IEventHandle> ev) {
            if (!PendingTimer.Consume(ev->Cookie)) {
                return;
            }
            const auto now = TActivationContext::Monotonic();
            auto next = TMonotonic::Max();
            std::erase_if(PendingRequests, [&](TPendingRequest& pending) {
                if (pending.Deadline > now) {
                    next = Min(next, pending.Deadline);
                    return false;
                }
                RejectPendingRequest(std::move(pending), "S3 request expired while waiting for an endpoint");
                return true;
            });
            PendingTimer.Schedule(next, SelfId(), TEvPrivate::EvExpirePending);
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
                if (pending.Deadline <= now) {
                    RejectPendingRequest(std::move(pending), "S3 request expired while waiting for an endpoint");
                    continue;
                }
                RecordPendingLatency(pending, now);
                TrackAndForward(std::move(pending), now);
            }
        }

        void BuildInnerWrapper(const TString& endpoint) {
            auto settings = Settings.GetSettings();
            settings.SetEndpoint(endpoint);
            RegisterInnerWrapper(NWrappers::IExternalStorageConfig::Construct(
                AppData()->AwsClientConfig, settings),
                MakeRouteCounters(false));
            CurrentEndpoint = endpoint;

            YDB_LOG_INFO("S3Router endpoint set (direct)",
                {"marker", "BDTS25"},
                {"id", LogId},
                {"endpoint", endpoint});

            SetCounter(Mon.IsUsingProxy, 0);
        }

        void BuildInnerWrapperViaProxy(const TString& host, ui16 port) {
            const TString prevEndpoint = CurrentEndpoint;
            auto settings = Settings.GetSettings();
            settings.SetEndpoint(OriginalEndpoint);
            settings.SetProxyHost(host);
            settings.SetProxyPort(port);
            settings.SetProxyScheme(Settings.GetBalancerProxyScheme());
            const TString proxyEndpoint = TStringBuilder() << host << ':' << port;
            RegisterInnerWrapper(NWrappers::IExternalStorageConfig::Construct(
                AppData()->AwsClientConfig, settings),
                MakeRouteCounters(true));
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

                    if (LastEndpoint) {
                        LastEndpoint->Set(host, port);
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

        template<typename TCallback>
        static void VisitRequest(IEventHandle& ev, TCallback&& visit) {
            using namespace NWrappers::NExternalStorage;
            switch (ev.GetTypeRewrite()) {
#define VISIT_REQUEST(name) \
                case TEv##name##Request::EventType: visit(*ev.Get<TEv##name##Request>()); break;
                Y_FOR_EACH_S3_WRAPPER_OP(VISIT_REQUEST)
#undef VISIT_REQUEST
            }
        }

        TDuration RequestTimeout(ui32 type) const {
            using namespace NWrappers::NExternalStorage;
            switch (type) {
                case TEvGetObjectRequest::EventType:
                case TEvPutObjectRequest::EventType:
                case TEvUploadPartRequest::EventType:
                case TEvUploadPartCopyRequest::EventType:
                    return TransferTimeout;
                default:
                    return TS3RequestTimer::Timeout;
            }
        }

        template<class T>
        void SetSdkDeadline(T& request, TMonotonic deadline, const std::shared_ptr<TSdkRequests::TSlot>& slot) {
            request.SetContinueRequestHandler(TCancellableAwsRequest{[deadline, slot, wrapper = InnerWrapperInFlight,
                    previous = request.GetContinueRequestHandler()](const auto* http) {
                Y_UNUSED(slot); // retain the physical slot through the SDK callback
                return !wrapper->IsCancelled() && TMonotonic::Now() < deadline && (!previous || previous(http));
            }});
        }

        void TrackAndForward(TPendingRequest pending, TMonotonic now) {
            using namespace NWrappers::NExternalStorage;
            auto& ev = pending.Ev;
            if (pending.Deadline != TMonotonic::Max()) {
                std::shared_ptr<TSdkRequests::TSlot> slot;
                switch (ev->GetTypeRewrite()) {
                    case TEvGetObjectRequest::EventType: {
                        auto& get = *ev->Get<TEvGetObjectRequest>();
                        // Tablet-side source reads have their own concurrency budget.
                        if (std::dynamic_pointer_cast<TS3GetContext>(get.GetRequestContext())) {
                            slot = SdkRequests->Acquire(SdkRequests->AgentGets, S3ControlLimit(MaxAgentGets));
                            if (!slot) {
                                return RejectRequest(std::move(ev), "too many outstanding S3 agent reads");
                            }
                        }
                        break;
                    }
                    case TEvPutObjectRequest::EventType: {
                        const auto context = std::dynamic_pointer_cast<TS3PutContext>(ev->Get<TEvPutObjectRequest>()->GetRequestContext());
                        const bool cooling = context && context->Source == TS3PutContext::ESource::Cooling;
                        slot = cooling ? SdkRequests->Acquire(SdkRequests->CoolingPuts,
                            Settings.GetAsyncMode().GetUploadPutsInFlight())
                            : SdkRequests->Acquire(SdkRequests->AgentPuts, S3ControlLimit(MaxAgentPuts));
                        if (!slot) {
                            if (cooling) {
                                // This attempt never reached storage. No additional late-PUT watch is needed.
                                Send(ev->Sender, new TEvents::TEvUndelivered(TEvPutObjectRequest::EventType,
                                    TEvents::TEvUndelivered::ReasonUnknown), 0, ev->Cookie);
                                return;
                            }
                            return RejectRequest(std::move(ev), "too many outstanding S3 agent writes");
                        }
                        break;
                    }
                    case TEvDeleteObjectRequest::EventType:
                    case TEvDeleteObjectsRequest::EventType: {
                        slot = SdkRequests->Acquire(SdkRequests->Deletes, S3ControlLimit(MaxDeletes));
                        if (!slot) {
                            return RejectRequest(std::move(ev), "too many outstanding S3 deletes");
                        }
                        break;
                    }
                }
                // SDK callbacks run outside the actor runtime. Convert the remaining budget, including queue time.
                const auto deadline = TMonotonic::Now() + (pending.Deadline - now);
                VisitRequest(*ev, [&](auto& request) { SetSdkDeadline(request.MutableRequest(), deadline, slot); });
            }
            if (InnerWrapperInFlight && IsRequestEvent(ev->GetTypeRewrite())) {
                InnerWrapperInFlight->Inc();
            }
            TActivationContext::Send(IEventHandle::Forward(std::move(ev), InnerWrapperId));
        }

        void Forward(STATEFN_SIG) {
            const auto now = TActivationContext::Monotonic();
            auto deadline = TMonotonic::Max();
            bool matchesBucket = true;
            bool matchesDeleteRoute = true;
            VisitRequest(*ev, [&](const auto& event) {
                const auto& request = event.GetRequest();
                matchesBucket = !request.BucketHasBeenSet() || request.GetBucket() == Settings.GetSettings().GetBucket();
                deadline = now + RequestTimeout(event.EventType);
                if (const auto context = std::dynamic_pointer_cast<TS3RequestDeadline>(event.GetRequestContext())) {
                    deadline = Min(deadline, context->Deadline);
                    if (const auto* deletion = dynamic_cast<const TS3DeleteContext*>(context.get())) {
                        matchesDeleteRoute = *deletion->Route == DeleteRoute;
                    }
                }
            });
            // A delayed request can reach the replacement router during a namespace migration.
            // The storage wrapper otherwise replaces the request's bucket with its configured bucket.
            if (!matchesBucket) {
                return RejectRequest(std::unique_ptr<IEventHandle>(ev.Release()), "S3 request targets a different bucket");
            }
            if (!matchesDeleteRoute) {
                return RejectRequest(std::unique_ptr<IEventHandle>(ev.Release()), "S3 delete targets a different BlobDepot configuration");
            }
            TPendingRequest pending{now, deadline, std::unique_ptr<IEventHandle>(ev.Release())};
            if (pending.Deadline <= now) {
                return RejectRequest(std::move(pending.Ev), "S3 request deadline exceeded before dispatch");
            }
            if (InnerWrapperId) {
                return TrackAndForward(std::move(pending), now);
            }

            if (PendingRequests.size() < MaxPendingRequests) {
                YDB_LOG_DEBUG("S3Router queueing request until balancer resolves",
                    {"marker", "BDTS34"},
                    {"id", LogId},
                    {"type", pending.Ev->GetTypeRewrite()},
                    {"pending", PendingRequests.size() + 1});
                PendingTimer.Schedule(pending.Deadline, SelfId(), TEvPrivate::EvExpirePending);
                PendingRequests.push_back(std::move(pending));
                return;
            }

            IncCounter(Mon.PendingRejects);
            RejectRequest(std::move(pending.Ev));
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BLOB_DEPOT_S3_ROUTER;
        }

        TBlobDepotS3Router(NKikimrBlobDepot::TS3BackendSettings settings, ui64 tabletId,
                std::shared_ptr<TS3BalancerEndpoint> lastEndpoint)
            : Settings(std::move(settings))
            , DeleteRoute(TS3DeleteContext::MakeRouteKey(Settings))
            , TabletId(tabletId)
            , LogId(TStringBuilder() << '{' << tabletId << ":s3r}")
            , LastEndpoint(std::move(lastEndpoint))
        {}

        void Bootstrap() {
            const TString& endpoint = Settings.GetSettings().GetEndpoint();
            OriginalEndpoint = endpoint;
            SdkRequests = TSdkRequests::Get(TActivationContext::ActorSystem(), TabletId);
            TransferTimeout = GetS3PutTimeout(Settings.GetSettings(), AppData()->AwsClientConfig);
            auto& controls = AppData()->Icb->BlobDepotControls;
            TControlBoard::RegisterSharedControl(MaxAgentGets, controls.S3MaxGetsInFlight);
            TControlBoard::RegisterSharedControl(MaxAgentPuts, controls.S3MaxWritesInFlight);
            TControlBoard::RegisterSharedControl(MaxDeletes, controls.S3MaxDeletesInFlight);
            SetupCounters();
            ScheduleSweepRetiring();

            YDB_LOG_INFO("S3Router bootstrap",
                {"marker", "BDTS24"},
                {"id", LogId},
                {"endpoint", OriginalEndpoint},
                {"balancerEnabled", BalancerEnabled()},
                {"balancerHost", BalancerEnabled() ? Settings.GetBalancerHost() : TString()});

            if (BalancerEnabled()) {
                if (LastEndpoint) {
                    const auto [host, port] = LastEndpoint->Get();
                    if (!host.empty()) {
                        BuildInnerWrapperViaProxy(host, port);
                    }
                }
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
                InnerWrapperInFlight->Cancel();
                Send(InnerWrapperId, new TEvents::TEvPoison());
                InnerWrapperId = {};
                InnerWrapperInFlight.Reset();
            }

            for (auto&& wrapper : RetiringWrappers) {
                wrapper.InFlight->Cancel();
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
                fFunc(TEvPrivate::EvExpirePending, ExpirePending);
                cFunc(TEvents::TSystem::Poison, PassAway);
            }
        }
    };

    } // anonymous

    IActor* CreateBlobDepotS3Router(NKikimrBlobDepot::TS3BackendSettings settings, ui64 tabletId,
            std::shared_ptr<TS3BalancerEndpoint> lastEndpoint) {
        return new TBlobDepotS3Router(std::move(settings), tabletId, std::move(lastEndpoint));
    }

} // NKikimr::NBlobDepot
