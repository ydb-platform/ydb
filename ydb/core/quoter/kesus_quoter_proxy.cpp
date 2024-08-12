#include "kesus_quoter_proxy.h"
#include "quoter_service_impl.h"
#include "debug_info.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>
#include <ydb/core/kesus/tablet/events.h>
#include <ydb/core/kesus/tablet/quoter_constants.h>

#include <ydb/library/time_series_vec/time_series_vec.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/map.h>
#include <util/generic/hash.h>
#include <util/system/types.h>

#include <limits>
#include <cmath>

#if defined PLOG_TRACE || defined PLOG_DEBUG || defined PLOG_INFO || defined PLOG_WARN || defined PLOG_ERROR \
    || defined KESUS_PROXY_LOG_TRACE || defined KESUS_PROXY_LOG_DEBUG || defined KESUS_PROXY_LOG_INFO || defined KESUS_PROXY_LOG_WARN || defined KESUS_PROXY_LOG_ERROR
#error log macro definition clash
#endif

#define PLOG_TRACE(stream) LOG_TRACE_S((TlsActivationContext->AsActorContext()), NKikimrServices::QUOTER_PROXY, stream)
#define PLOG_DEBUG(stream) LOG_DEBUG_S((TlsActivationContext->AsActorContext()), NKikimrServices::QUOTER_PROXY, stream)
#define PLOG_INFO(stream) LOG_INFO_S((TlsActivationContext->AsActorContext()), NKikimrServices::QUOTER_PROXY, stream)
#define PLOG_WARN(stream) LOG_WARN_S((TlsActivationContext->AsActorContext()), NKikimrServices::QUOTER_PROXY, stream)
#define PLOG_ERROR(stream) LOG_ERROR_S((TlsActivationContext->AsActorContext()), NKikimrServices::QUOTER_PROXY, stream)

#define KESUS_PROXY_LOG_TRACE(stream) PLOG_TRACE(LogPrefix << stream)
#define KESUS_PROXY_LOG_DEBUG(stream) PLOG_DEBUG(LogPrefix << stream)
#define KESUS_PROXY_LOG_INFO(stream) PLOG_INFO(LogPrefix << stream)
#define KESUS_PROXY_LOG_WARN(stream) PLOG_WARN(LogPrefix << stream)
#define KESUS_PROXY_LOG_ERROR(stream) PLOG_ERROR(LogPrefix << stream)

namespace NKikimr {
namespace NQuoter {

using NKesus::TEvKesus;

class TKesusQuoterProxy : public TActorBootstrapped<TKesusQuoterProxy> {
    struct TResourceState {
        const TString Resource;
        ui64 ResId = Max<ui64>();

        double Available = 0;
        double QueueWeight = 0;
        double ResourceBucketMaxSize = 0;
        double ResourceBucketMinSize = 0;
        bool SessionIsActive = false;
        bool ProxySessionWasSent = false;
        TInstant LastAllocated = TInstant::Zero();
        std::pair<TDuration, double> AverageAllocationParams = {TDuration::Zero(), 0.0};

        NKikimrKesus::TStreamingQuoterResource Props;

        bool InitedProps = false;

        TKesusResourceAllocationStatistics AllocStats;

        THolder<TTimeSeriesVec<double>> History;
        bool PendingAccountingReport = false; // History contains data to send
        TInstant HistoryAccepted; // Do not report history before this instant
        TInstant LastAccountingReportEnd; // Do not write history before this instant
        TInstant LastAccountingReport; // Aligned to `ReportPeriod` grid timestamp of last sent report
        TDuration AccountingReportPeriod = TDuration::Max();

        struct TReportHistoryItem {
            ui32 ReportId;
            double TotalConsumed;
            double TotalAllocated;
        };
        bool ReplicationEnabled = false;
        std::deque<TReportHistoryItem> ReportHistory;
        size_t MaxReportHistory = 120;
        ui32 ReportId = 0;
        double TotalConsumed = 0; // Since last replication enabled
        double TotalAllocated = 0;
        TInstant LastReplicationReport; // Aligned to `ReportPeriod` grid timestamp of last sent report
        TDuration ReplicationReportPeriod = TDuration::Max();

        struct TCounters {
            class TDoubleCounter {
            public:
                TDoubleCounter() = default;

                TDoubleCounter(::NMonitoring::TDynamicCounters::TCounterPtr counter)
                    : Counter(std::move(counter))
                {
                }

                TDoubleCounter& operator=(::NMonitoring::TDynamicCounters::TCounterPtr counter) {
                    Counter = std::move(counter);
                    return *this;
                }

                TDoubleCounter& operator+=(double value) {
                    value += Remainder;
                    const double counterIncrease = std::floor(value);
                    Remainder = value - counterIncrease;
                    if (Counter) {
                        *Counter += static_cast<i64>(counterIncrease);
                    }
                    return *this;
                }

            private:
                ::NMonitoring::TDynamicCounters::TCounterPtr Counter;
                double Remainder = 0.0;
            };

            std::vector<::NMonitoring::TDynamicCounters::TCounterPtr> ParentConsumed; // Aggregated consumed counters for parent resources.
            ::NMonitoring::TDynamicCounters::TCounterPtr QueueSize;
            ::NMonitoring::TDynamicCounters::TCounterPtr QueueWeight;
            ::NMonitoring::TDynamicCounters::TCounterPtr Dropped;
            ::NMonitoring::TDynamicCounters::TCounterPtr Accumulated;
            TDoubleCounter AllocatedOffline;
            TDoubleCounter ReceivedFromKesus;

            TCounters(const TString& resource, const ::NMonitoring::TDynamicCounterPtr& quoterCounters) {
                if (!quoterCounters) {
                    return;
                }

                auto splittedPath = SplitPath(resource);
                Y_ABORT_UNLESS(!splittedPath.empty());
                ParentConsumed.reserve(splittedPath.size() - 1);
                for (auto pathIter = splittedPath.begin() + 1; pathIter != splittedPath.end(); ++pathIter) {
                    const TString parentResourceName = NKesus::CanonizeQuoterResourcePath(TVector<TString>(splittedPath.begin(), pathIter));
                    const auto resourceCounters = quoterCounters->GetSubgroup(RESOURCE_COUNTER_SENSOR_NAME, parentResourceName);
                    ParentConsumed.emplace_back(resourceCounters->GetCounter(CONSUMED_COUNTER_NAME, true));
                }

                const auto resourceCounters = quoterCounters->GetSubgroup(RESOURCE_COUNTER_SENSOR_NAME, NKesus::CanonizeQuoterResourcePath(splittedPath));
                QueueSize = resourceCounters->GetExpiringCounter(RESOURCE_QUEUE_SIZE_COUNTER_SENSOR_NAME, false);
                QueueWeight = resourceCounters->GetExpiringCounter(RESOURCE_QUEUE_WEIGHT_COUNTER_SENSOR_NAME, false);
                AllocatedOffline = resourceCounters->GetCounter(RESOURCE_ALLOCATED_OFFLINE_COUNTER_SENSOR_NAME, true);
                Dropped = resourceCounters->GetCounter(RESOURCE_DROPPED_COUNTER_SENSOR_NAME, true);
                Accumulated = resourceCounters->GetExpiringCounter(RESOURCE_ACCUMULATED_COUNTER_SENSOR_NAME, false);
                ReceivedFromKesus = resourceCounters->GetCounter(RESOURCE_RECEIVED_FROM_KESUS_COUNTER_SENSOR_NAME, true);
            }

            void AddConsumed(ui64 consumed) {
                for (::NMonitoring::TDynamicCounters::TCounterPtr& counter : ParentConsumed) {
                    *counter += consumed;
                }
            }
        };

        TCounters Counters;

        explicit TResourceState(const TString& resource, const ::NMonitoring::TDynamicCounterPtr& quoterCounters)
            : Resource(resource)
            , Counters(resource, quoterCounters)
        {}

        void AddUpdate(TEvQuota::TEvProxyUpdate& ev) const {
            TVector<TEvQuota::TUpdateTick> update;
            double sustainedRate = 0.0;
            if (Available > 0.0) {
                constexpr double rateBurst = 2.0;
                constexpr ui32 ticks = 2;
                constexpr double ticksD = static_cast<double>(ticks);
                update.emplace_back(0, ticks, Available * (rateBurst / ticksD), TEvQuota::ETickPolicy::Front);
                sustainedRate = Available * rateBurst;
            } else {
                update.emplace_back();
            }
            ev.Resources.emplace_back(ResId, sustainedRate, std::move(update), TEvQuota::EUpdateState::Normal);
        }

        void AddConsumed(double consumed) {
            if (ReplicationEnabled) {
                TotalConsumed += consumed;
            }
        }

        void SetProps(const NKikimrKesus::TStreamingQuoterResource& props, ui32 serverVersion) {
            Props = props;
            const auto& cfg = Props.GetHierarchicalDRRResourceConfig();
            const double speed = cfg.GetMaxUnitsPerSecond();
            const double prefetch = cfg.GetPrefetchCoefficient() ? cfg.GetPrefetchCoefficient() : NKesus::NQuoter::PREFETCH_COEFFICIENT_DEFAULT;
            double watermark = std::clamp(cfg.GetPrefetchWatermark() ? cfg.GetPrefetchWatermark() : NKesus::NQuoter::PREFETCH_WATERMARK_DEFAULT, 0.0, 1.0);

            if (Props.GetHierarchicalDRRResourceConfig().HasReplicatedBucket()) {
                watermark = 0.999;
            }

            const double prevBucketMaxSize = ResourceBucketMaxSize;
            ResourceBucketMaxSize = Max(0.0, speed * prefetch);
            ResourceBucketMinSize = ResourceBucketMaxSize * watermark;
            Y_ABORT_UNLESS(ResourceBucketMinSize <= ResourceBucketMaxSize);

            // Decrease available resource if speed or prefetch settings have been changed.
            if (prefetch > 0.0) { // RTMR-3774
                if (InitedProps && ResourceBucketMaxSize < prevBucketMaxSize) {
                    if (const double maxAvailable = ResourceBucketMaxSize + QueueWeight; Available > maxAvailable) {
                        if (Counters.Dropped) {
                            const double dropped = Available - maxAvailable;
                            *Counters.Dropped += dropped;
                        }
                        SetAvailable(maxAvailable); // Update resource props with smaller quota.
                    }
                }
            }

            if (Props.GetAccountingConfig().GetEnabled()) {
                AccountingReportPeriod = TDuration::MilliSeconds(Props.GetAccountingConfig().GetReportPeriodMs());
                const ui64 intervalsInSec = 100; // as far as default resolution in TTimeSerisVec is 10'000
                THolder<TTimeSeriesVec<double>> history(new TTimeSeriesVec<double>(Props.GetAccountingConfig().GetCollectPeriodSec() * intervalsInSec));
                if (History) {
                    history->Add(*History.Get());
                }
                History.Reset(history.Release());
            } else {
                AccountingReportPeriod = TDuration::Max();
                History.Destroy();
            }

            if (serverVersion >= 1 && Props.GetHierarchicalDRRResourceConfig().HasReplicatedBucket()) {
                ReplicationEnabled = true;
                ReplicationReportPeriod = TDuration::MilliSeconds(Props.GetHierarchicalDRRResourceConfig().GetReplicatedBucket().GetReportIntervalMs());
                TotalConsumed = 0;
                TotalAllocated = 0;
            } else {
                ReplicationEnabled = false;
                ReplicationReportPeriod = TDuration::Max();
                TotalConsumed = 0;
                TotalAllocated = 0;
            }

            if (!InitedProps) {
                InitedProps = true;
                SetAvailable(ResourceBucketMaxSize);
            }
            AllocStats.SetProps(Props);
        }

        void SetAvailable(double available) {
            Available = available;
            if (Counters.Accumulated) {
                *Counters.Accumulated = static_cast<i64>(available);
            }
        }
    };

    struct TEvPrivate {
        enum EEv {
            EvOfflineResourceAllocation = EventSpaceBegin(TEvents::ES_PRIVATE),

            EvEnd
        };

        static_assert(EvEnd <= EventSpaceEnd(TKikimrEvents::ES_PRIVATE),
            "expected EvEnd <= EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvOfflineResourceAllocation : public TEventLocal<TEvOfflineResourceAllocation, EvOfflineResourceAllocation> {
            struct TResourceInfo {
                ui64 ResourceId;
                double Amount;

                TResourceInfo(ui64 resId, double amount)
                    : ResourceId(resId)
                    , Amount(amount)
                {
                }
            };

            std::vector<TResourceInfo> Resources;

            TEvOfflineResourceAllocation() = default;
        };
    };

    const TActorId QuoterServiceId;
    const ui64 QuoterId;
    const TVector<TString> Path;
    const TString LogPrefix;
    TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TKesusInfo> KesusInfo;
    THolder<ITabletPipeFactory> TabletPipeFactory;
    TActorId KesusPipeClient;
    ui32 ServerVersion = 0;

    bool Connected = false;
    TInstant DisconnectTime;
    ui64 OfflineAllocationCookie = 0;

    TMap<TString, THolder<TResourceState>> Resources; // Map because iterators are needed to remain valid during insertions.
    THashMap<ui64, decltype(Resources)::iterator> ResIndex;

    THashMap<ui64, std::vector<TString>> CookieToResourcePath;
    ui64 NextCookie = 1;

    THolder<NKesus::TEvKesus::TEvUpdateConsumptionState> UpdateEv;
    THolder<NKesus::TEvKesus::TEvAccountResources> AccountEv;
    THolder<NKesus::TEvKesus::TEvReportResources> ReplicationEv;
    THolder<TEvQuota::TEvProxyUpdate> ProxyUpdateEv;
    THashMap<TDuration, THolder<TEvPrivate::TEvOfflineResourceAllocation>> OfflineAllocationEvSchedule;

    struct TCounters {
        ::NMonitoring::TDynamicCounterPtr QuoterCounters;

        ::NMonitoring::TDynamicCounters::TCounterPtr Disconnects;

        void Init(const TString& quoterPath) {
            TIntrusivePtr<::NMonitoring::TDynamicCounters> serviceCounters = GetServiceCounters(AppData()->Counters, QUOTER_SERVICE_COUNTER_SENSOR_NAME);
            if (serviceCounters) {
                QuoterCounters = serviceCounters->GetSubgroup(QUOTER_COUNTER_SENSOR_NAME, quoterPath);
                Disconnects = QuoterCounters->GetCounter(DISCONNECTS_COUNTER_SENSOR_NAME, true);
            }
        }
    };

    TCounters Counters;

private:
    ui64 NewCookieForRequest(TString resourcePath) {
        Y_ABORT_UNLESS(resourcePath);
        std::vector<TString> paths = {std::move(resourcePath)};
        return NewCookieForRequest(std::move(paths));
    }

    ui64 NewCookieForRequest(std::vector<TString> resourcePaths) {
        Y_ABORT_UNLESS(!resourcePaths.empty());
        const ui64 cookie = NextCookie++;
        Y_ABORT_UNLESS(CookieToResourcePath.emplace(cookie, std::move(resourcePaths)).second);
        return cookie;
    }

    std::vector<TString> PopResourcePathsForRequest(ui64 cookie) {
        auto resPathIt = CookieToResourcePath.find(cookie);
        if (resPathIt != CookieToResourcePath.end()) {
            std::vector<TString> ret = std::move(resPathIt->second);
            CookieToResourcePath.erase(resPathIt);
            return ret;
        } else {
            return {};
        }
    }

    static TString KesusErrorToString(const NKikimrKesus::TKesusError& err) {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(err.GetIssues(), issues);
        return issues.ToString();
    }

    void SendProxySessionError(TEvQuota::TEvProxySession::EResult code, const TString& resourcePath) {
        KESUS_PROXY_LOG_TRACE("ProxySession(\"" << resourcePath << "\", Error: " << code << ")");
        Send(QuoterServiceId,
             new TEvQuota::TEvProxySession(
                 code,
                 QuoterId,
                 0,
                 resourcePath,
                 TDuration::Zero(),
                 TEvQuota::EStatUpdatePolicy::Never
             ));
    }

    void ProcessSubscribeResourceError(Ydb::StatusIds::StatusCode code, TResourceState* resState) {
        if (!resState->ProxySessionWasSent) {
            resState->ProxySessionWasSent = true;
            const TEvQuota::TEvProxySession::EResult sessionCode = code == Ydb::StatusIds::NOT_FOUND ? TEvQuota::TEvProxySession::UnknownResource : TEvQuota::TEvProxySession::GenericError;
            SendProxySessionError(sessionCode, resState->Resource);
            DeleteResourceInfo(resState->Resource, resState->ResId);
        } else {
            BreakResource(*resState, GetProxyUpdateEv());
        }
    }

    void SendProxySessionIfNotSent(TResourceState* resState) {
        if (!resState->ProxySessionWasSent) {
            resState->ProxySessionWasSent = true;
            KESUS_PROXY_LOG_TRACE("ProxySession(\"" << resState->Resource << "\", " << resState->ResId << ")");
            Send(QuoterServiceId,
                new TEvQuota::TEvProxySession(
                    TEvQuota::TEvProxySession::Success,
                    QuoterId,
                    resState->ResId,
                    resState->Resource,
                    TDuration::MilliSeconds(100),
                    TEvQuota::EStatUpdatePolicy::EveryActiveTick
                ));
        }
    }

    TResourceState* FindResource(ui64 id) {
        const auto indexIt = ResIndex.find(id);
        return indexIt != ResIndex.end() ? indexIt->second->second.Get() : nullptr;
    }

    const TResourceState* FindResource(ui64 id) const {
        const auto indexIt = ResIndex.find(id);
        return indexIt != ResIndex.end() ? indexIt->second->second.Get() : nullptr;
    }

    void Handle(NMon::TEvHttpInfo::TPtr &ev) {
        TStringStream str;
        str << NMonitoring::HTTPOKHTML;
        HTML(str) {
            HEAD() {
                str << "<link rel='stylesheet' href='../cms/ext/bootstrap.min.css'>" << Endl
                    << "<script language='javascript' type='text/javascript' src='../cms/ext/jquery.min.js'></script>" << Endl
                    << "<script language='javascript' type='text/javascript' src='../cms/ext/bootstrap.bundle.min.js'></script>" << Endl;
            }

            DIV() {
                OL_CLASS("breadcrumb") {
                    LI_CLASS("breadcrumb-item") {
                        str << "<a href='..' id='host-ref'>YDB Developer UI</a>" << Endl;
                    }
                    LI_CLASS("breadcrumb-item") {
                        str << "<a href='.'>Actors</a>" << Endl;
                    }
                    LI_CLASS("breadcrumb-item") {
                        str << "<a href='./quoter_proxy'>QuoterService</a>" << Endl;
                    }
                    LI_CLASS("breadcrumb-item active") {
                        str << LogPrefix << Endl;
                    }
                }
            }
            DIV_CLASS("container") {
                str << "<a class='collapse-ref' data-toggle='collapse' data-target='#proxy-state'>"
                    << "ProxyState</a><div id='proxy-state' class='collapse show'>";
                PRE() {
                    str << "Path: " << LogPrefix << "\n"
                        << "KesusTabletId: " << KesusInfo->Description.GetKesusTabletId() << "\n"
                        << "ServerVersion: " << ServerVersion << "\n"
                        << "Connected: " << (Connected ? "true" : "false") << "\n"
                        << "DisconnectTime: " << DisconnectTime << "\n"
                        << "Resources:\n";
                    for (auto& [name, res] : Resources) {
                        str << "  Resource: " << name << "\n";

                        if (res) {
                            str << "  Id: " << res->ResId << "\n"
                                << "  Available: " << res->Available << "\n"
                                << "  QueueWeight: " << res->QueueWeight << "\n"
                                << "  SessionIsActive: " << (res->SessionIsActive ? "true" : "false") << "\n"
                                << "  ProxySessionWasSent: " << (res->ProxySessionWasSent ? "true" : "false") << "\n"
                                << "  LastAllocated: " << res->LastAllocated << "\n"
                                << "  InitedProps: " << (res->InitedProps ? "true" : "false") << "\n"
                                << "  PendingAccountingReport: " << (res->PendingAccountingReport ? "true" : "false") << "\n"
                                << "  HistoryAccepted: " << res->HistoryAccepted << "\n"
                                << "  LastAccountingReport: " << res->LastAccountingReport << "\n"
                                << "  AccountingReportPeriod: " << res->AccountingReportPeriod << "\n"
                                << "  ReplicationEnabled: " << (res->ReplicationEnabled ? "true" : "false") << "\n"
                                << "  TotalConsumed: " << res->TotalConsumed << "\n"
                                << "  TotalAllocated: " << res->TotalAllocated << "\n"
                                << "  LastReplicationReport: " << res->LastReplicationReport << "\n"
                                << "  ReplicationReportPeriod: " << res->ReplicationReportPeriod << "\n"
                                << "  Props: " << res->Props.ShortDebugString() << "\n";
                        }
                    }
                    str << "UpdateEv: " << (UpdateEv ? "" : "null") << "\n";
                    if (UpdateEv) {

                    }
                    str << "AccountEv: " << (AccountEv ? "" : "null") << "\n";
                    if (AccountEv) {

                    }
                    str << "ReplicationEv: " << (ReplicationEv ? "" : "null") << "\n";
                    if (ReplicationEv) {

                    }
                }
                str << "</div>";
            }
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
    }

    void Handle(TEvQuota::TEvProxyRequest::TPtr& ev) {
        TEvQuota::TEvProxyRequest* msg = ev->Get();
        KESUS_PROXY_LOG_INFO("ProxyRequest \"" << msg->Resource << "\"");
        Y_ABORT_UNLESS(ev->Sender == QuoterServiceId);

        auto resourceIt = Resources.find(msg->Resource);
        if (resourceIt == Resources.end()) {
            const TString canonPath = NKesus::CanonizeQuoterResourcePath(msg->Resource);
            if (canonPath != msg->Resource) {
                KESUS_PROXY_LOG_WARN("Resource \"" << msg->Resource << "\" has incorrect name. Maybe this was some error on client side.");
                SendProxySessionError(TEvQuota::TEvProxySession::GenericError, msg->Resource);
                return;
            }

            auto [iter, inserted] = Resources.emplace(msg->Resource, MakeHolder<TResourceState>(msg->Resource, Counters.QuoterCounters));
            Y_ASSERT(inserted);
            resourceIt = iter;
        }
        Y_ASSERT(resourceIt != Resources.end());

        TResourceState* const resState = resourceIt->second.Get();
        if (resState->ResId == Max<ui64>()) {
            InitiateNewSessionToResource(resState->Resource);
        } else {
            // Already. Resend result.
            resState->ProxySessionWasSent = false;
            SendProxySessionIfNotSent(resState);
            resState->AddUpdate(GetProxyUpdateEv());
        }
    }

    void InitiateNewSessionToResource(const TString& resourcePath) {
        if (Connected) {
            KESUS_PROXY_LOG_DEBUG("Subscribe on resource \"" << resourcePath << "\"");
            auto ev = std::make_unique<TEvKesus::TEvSubscribeOnResources>();
            ev->Record.SetProtocolVersion(NKesus::NQuoter::QUOTER_PROTOCOL_VERSION);
            ActorIdToProto(SelfId(), ev->Record.MutableActorID());
            auto* res = ev->Record.AddResources();
            res->SetResourcePath(resourcePath);
            NTabletPipe::SendData(SelfId(), KesusPipeClient, ev.release(), NewCookieForRequest(resourcePath));
        }
    }

    void SubscribeToAllResources() {
        Y_ABORT_UNLESS(Connected);
        if (Resources.empty()) {
            return;
        }
        std::vector<TString> resourcePaths;
        resourcePaths.reserve(Resources.size());
        auto ev = std::make_unique<TEvKesus::TEvSubscribeOnResources>();
        ev->Record.SetProtocolVersion(NKesus::NQuoter::QUOTER_PROTOCOL_VERSION);
        ActorIdToProto(SelfId(), ev->Record.MutableActorID());
        for (auto&& [resourcePath, resInfo] : Resources) {
            auto* res = ev->Record.AddResources();
            res->SetResourcePath(resourcePath);
            if (resInfo->SessionIsActive) {
                res->SetStartConsuming(true);
                res->SetInitialAmount(std::numeric_limits<double>::infinity());
            }
            resourcePaths.push_back(resourcePath);
        }
        NTabletPipe::SendData(SelfId(), KesusPipeClient, ev.release(), NewCookieForRequest(std::move(resourcePaths)));
    }

    TEvQuota::TEvProxyUpdate& GetProxyUpdateEv() {
        if (!ProxyUpdateEv) {
            ProxyUpdateEv = CreateUpdateEvent();
        }
        return *ProxyUpdateEv;
    }

    void InitUpdateEv() {
        if (!UpdateEv) {
            UpdateEv = MakeHolder<NKesus::TEvKesus::TEvUpdateConsumptionState>();
            ActorIdToProto(SelfId(), UpdateEv->Record.MutableActorID());
        }
    }

    void InitAccountEv() {
        if (!AccountEv) {
            AccountEv = MakeHolder<NKesus::TEvKesus::TEvAccountResources>();
            ActorIdToProto(SelfId(), AccountEv->Record.MutableActorID());
        }
    }

    void InitReplicationEv() {
        if (!ReplicationEv) {
            ReplicationEv = MakeHolder<NKesus::TEvKesus::TEvReportResources>();
            ActorIdToProto(SelfId(), ReplicationEv->Record.MutableActorID());
        }
    }

    void SendDeferredEvents() {
        for (auto& [_, res] : Resources) {
            if (res->ReplicationEnabled) {
                CheckReplicationReport(*res, TActivationContext::Now());
            }
        }

        if (Connected && UpdateEv) {
            KESUS_PROXY_LOG_TRACE("UpdateConsumptionState(" << UpdateEv->Record << ")");
            NTabletPipe::SendData(SelfId(), KesusPipeClient, UpdateEv.Release());
        }
        UpdateEv.Reset();

        if (Connected && AccountEv && AccountEv->Record.GetResourcesInfo().size() > 0) {
            KESUS_PROXY_LOG_TRACE("AccountResources(" << AccountEv->Record << ")");
            NTabletPipe::SendData(SelfId(), KesusPipeClient, AccountEv.Release());
        }
        AccountEv.Reset();

        if (Connected && ReplicationEv && ReplicationEv->Record.GetResourcesInfo().size() > 0) {
            KESUS_PROXY_LOG_TRACE("ReportResources(" << ReplicationEv->Record << ")");
            NTabletPipe::SendData(SelfId(), KesusPipeClient, ReplicationEv.Release());
        }
        ReplicationEv.Reset();

        if (ProxyUpdateEv && ProxyUpdateEv->Resources) {
            SendToService(std::move(ProxyUpdateEv));
        }
    }

    void ScheduleOfflineAllocation() {
        if (OfflineAllocationEvSchedule.empty()) {
            return;
        }

        if (!Connected) {
            for (auto&& alloc : OfflineAllocationEvSchedule) {
                KESUS_PROXY_LOG_TRACE("Schedule offline allocation in " << alloc.first << ": " << PrintResources(*alloc.second));
                TAutoPtr<IEventHandle> h = new IEventHandle(SelfId(), SelfId(), alloc.second.Release(), 0, OfflineAllocationCookie);
                TActivationContext::Schedule(alloc.first, std::move(h));
            }
        }
        OfflineAllocationEvSchedule.clear();
    }

    void MarkAllActiveResourcesForOfflineAllocation() {
        const TInstant now = TActivationContext::Now();
        for (auto&& [path, resState] : Resources) {
            Y_UNUSED(path);
            if (resState->ResId != Max<ui64>() && resState->SessionIsActive) {
                resState->AverageAllocationParams = resState->AllocStats.GetAverageAllocationParams();
                MarkResourceForOfflineAllocation(*resState, now);
            }
        }
    }

    void MarkResourceForOfflineAllocation(TResourceState& res, TInstant now) {
        TDuration averageDuration;
        double averageAmount;
        std::tie(averageDuration, averageAmount) = res.AverageAllocationParams;
        KESUS_PROXY_LOG_TRACE("Mark \"" << res.Resource << "\" for offline allocation. Connected: " << Connected
                              << ", SessionIsActive: " << res.SessionIsActive
                              << ", AverageDuration: " << averageDuration
                              << ", AverageAmount: " << averageAmount);
        if (!Connected && res.SessionIsActive && averageDuration && averageAmount) {
            const TDuration when =
                res.LastAllocated + averageDuration <= now ?
                TDuration::Zero() :
                res.LastAllocated + averageDuration - now;
            auto& event = OfflineAllocationEvSchedule[when];
            if (!event) {
                event = MakeHolder<TEvPrivate::TEvOfflineResourceAllocation>();
            }
            double amount = averageAmount;
            if (when) {
                const TDuration disconnected = now - DisconnectTime;
                const double microseconds = static_cast<double>((when + disconnected).MicroSeconds());
                amount *= std::pow(NKesus::NQuoter::FADING_ALLOCATION_COEFFICIENT, microseconds / 1000000.0);
            }
            event->Resources.emplace_back(res.ResId, amount);
        }
    }

    void ActivateSession(TResourceState& res, bool activate = true) {
        Y_ASSERT(res.SessionIsActive != activate);
        KESUS_PROXY_LOG_INFO((activate ? "Activate" : "Deactivate") << " session to \"" << res.Resource << "\". Connected: " << Connected);

        res.SessionIsActive = activate;
        if (Connected) {
            InitUpdateEv();
            auto* resInfo = UpdateEv->Record.AddResourcesInfo();
            resInfo->SetResourceId(res.ResId);
            resInfo->SetConsumeResource(activate);
            if (activate) {
                resInfo->SetAmount(std::numeric_limits<double>::infinity());
            }
        } else {
            if (activate) {
                res.AverageAllocationParams = res.AllocStats.GetAverageAllocationParams();
                MarkResourceForOfflineAllocation(res, TActivationContext::Now());
            }
        }
    }

    void ReportAccountingSession(TResourceState& res) {
        if (Connected && res.History) {
            InitAccountEv();
            auto* resInfo = AccountEv->Record.AddResourcesInfo();
            bool hasNonZeroAmount = false;
            TInstant start;
            double totalAmount = 0;
            for (TInstant i = res.History->Begin(), e = res.History->End(); i != e; i = res.History->Next(i)) {
                if (i >= res.HistoryAccepted) {
                    if (!start) {
                        start = i;
                    }
                    double amount = res.History->Get(i);
                    if (amount > 0) {
                        hasNonZeroAmount = true;
                        totalAmount += amount;
                    }
                    resInfo->AddAmount(amount);
                }
            }
            KESUS_PROXY_LOG_INFO("Report session to \"" << res.Resource << "\". Total amount: " << totalAmount);
            if (hasNonZeroAmount) {
                resInfo->SetResourceId(res.ResId);
                resInfo->SetStartUs(start.MicroSeconds());
                resInfo->SetIntervalUs(res.History->Interval().MicroSeconds());
                res.LastAccountingReportEnd = start + resInfo->GetAmount().size() * res.History->Interval();
            } else {
                // We have no useful data to report
                res.PendingAccountingReport = false;
                AccountEv->Record.MutableResourcesInfo()->RemoveLast(); // undo AddResourcesInfo()
            }
        }
    }

    void ReportReplicationSession(TResourceState& res) {
        if (Connected && res.ReplicationEnabled) {
            InitReplicationEv();
            auto* resInfo = ReplicationEv->Record.AddResourcesInfo();
            resInfo->SetResourceId(res.ResId);
            resInfo->SetTotalConsumed(res.TotalConsumed);
            resInfo->SetReportId(res.ReportId);
            res.ReportHistory.push_back({res.ReportId, res.TotalConsumed, res.TotalAllocated});
            ++res.ReportId;
            if (res.ReportHistory.size() > res.MaxReportHistory) {
                res.ReportHistory.pop_front();
            }
        }
    }

    void Handle(TEvQuota::TEvProxyStats::TPtr& ev) {
        TEvQuota::TEvProxyStats* msg = ev->Get();
        KESUS_PROXY_LOG_TRACE("ProxyStats(" << PrintResources(*ev->Get()) << ")");
        for (const TEvQuota::TProxyStat& stat : msg->Stats) {
            const auto indexIt = ResIndex.find(stat.ResourceId);
            if (indexIt != ResIndex.end()) {
                TResourceState& res = *indexIt->second->second;
                res.AddConsumed(stat.Consumed);
                res.SetAvailable(res.Available - stat.Consumed);
                res.QueueWeight = stat.QueueWeight;
                res.Counters.AddConsumed(stat.Consumed);
                if (res.History) {
                    res.History->AddShifted(stat.History, res.LastAccountingReportEnd);
                    res.PendingAccountingReport = true;
                    CheckAccountingReport(res, TActivationContext::Now());
                }
                if (res.ReplicationEnabled) {
                    CheckReplicationReport(res, TActivationContext::Now());
                }
                if (res.Counters.QueueSize) {
                    *res.Counters.QueueSize = static_cast<i64>(stat.QueueSize);
                    *res.Counters.QueueWeight = static_cast<i64>(stat.QueueWeight);
                }
                KESUS_PROXY_LOG_TRACE("Set info for resource \"" << res.Resource << "\": { Available: " << res.Available << ", QueueWeight: " << res.QueueWeight << " }");
                CheckState(res);
                res.AddUpdate(GetProxyUpdateEv());
            }
        }
    }

    void DeleteResourceInfo(const TString& resource, const ui64 resourceId) {
        auto indexIt = ResIndex.find(resourceId);
        if (indexIt != ResIndex.end()) {
            auto resIt = indexIt->second;
            if (resIt != Resources.end()) { // else it is already new resource with same path.
                TResourceState& res = *resIt->second;
                if (res.SessionIsActive) {
                    ActivateSession(res, false);
                }
                Resources.erase(resIt);
            }
            ResIndex.erase(indexIt);
            return;
        }

        auto resIt = Resources.find(resource);
        if (resIt != Resources.end()) {
            TResourceState& res = *resIt->second;
            if (res.SessionIsActive) {
                ActivateSession(res, false);
            }
            if (res.ResId != Max<ui64>()) {
                ResIndex.erase(res.ResId);
            }
            Resources.erase(resIt);
        }
    }

    void Handle(TEvQuota::TEvProxyCloseSession::TPtr& ev) {
        TEvQuota::TEvProxyCloseSession* msg = ev->Get();
        KESUS_PROXY_LOG_TRACE("ProxyCloseSession(\"" << msg->Resource << "\", " << msg->ResourceId << ")");
        DeleteResourceInfo(msg->Resource, msg->ResourceId);
    }

    void BreakResource(TResourceState& res, TEvQuota::TEvProxyUpdate& ev) {
        ev.Resources.emplace_back(res.ResId, 0.0, TVector<TEvQuota::TUpdateTick>(), TEvQuota::EUpdateState::Broken);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status == NKikimrProto::OK) {
            KESUS_PROXY_LOG_DEBUG("Successfully connected to tablet");
            Connected = true;
            SubscribeToAllResources();
        } else {
            if (ev->Get()->Dead) {
                KESUS_PROXY_LOG_WARN("Tablet doesn't exist");
                SendToService(CreateUpdateEvent(TEvQuota::EUpdateState::Broken));
            } else {
                KESUS_PROXY_LOG_WARN("Failed to connect to tablet. Status: " << ev->Get()->Status);
                ConnectToKesus(true);
            }
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        Y_ABORT_UNLESS(ev->Get()->TabletId == GetKesusTabletId(),
                 "Got EvClientDestroyed with tablet %" PRIu64 ", but kesus tablet is %" PRIu64, ev->Get()->TabletId, GetKesusTabletId());
        KESUS_PROXY_LOG_WARN("Disconnected from tablet");
        ConnectToKesus(true);
        DisconnectTime = TActivationContext::Now();
        OfflineAllocationCookie = NextCookie++;
        MarkAllActiveResourcesForOfflineAllocation();
        if (Counters.Disconnects) {
            ++*Counters.Disconnects;
        }
    }

    void Handle(NKesus::TEvKesus::TEvSubscribeOnResourcesResult::TPtr& ev) {
        const std::vector<TString> resourcePaths = PopResourcePathsForRequest(ev->Cookie);
        if (!resourcePaths.empty()) {
            const auto& result = ev->Get()->Record;
            ServerVersion = result.GetProtocolVersion();
            KESUS_PROXY_LOG_TRACE("SubscribeOnResourceResult(" << result << ")");
            Y_ABORT_UNLESS(result.ResultsSize() == resourcePaths.size(), "Expected %" PRISZT " resources, but got %" PRISZT, resourcePaths.size(), result.ResultsSize());
            for (size_t i = 0; i < resourcePaths.size(); ++i) {
                const auto& resResult = result.GetResults(i);
                auto resourceIt = Resources.find(resourcePaths[i]);
                if (resourceIt != Resources.end()) {
                    auto* resState = resourceIt->second.Get();
                    Y_ABORT_UNLESS(resState != nullptr);
                    if (resResult.GetError().GetStatus() == Ydb::StatusIds::SUCCESS) {
                        KESUS_PROXY_LOG_INFO("Initialized new session with resource \"" << resourcePaths[i] << "\"");
                        if (resState->ResId != Max<ui64>() && resState->ResId != resResult.GetResourceId()) { // Kesus was disconnected and then resource was recreated.
                            BreakResource(*resState, GetProxyUpdateEv());
                            ResIndex[resState->ResId] = Resources.end();
                        }
                        resState->ResId = resResult.GetResourceId();
                        ResIndex[resState->ResId] = resourceIt;
                        resourceIt->second->SetProps(resResult.GetEffectiveProps(), ServerVersion);
                        if (resourceIt->second->ReplicationEnabled) { // use initial availiable only in replicated mode
                            resourceIt->second->SetAvailable(resResult.GetInitialAvailable());
                        }
                        resState->AllocStats.OnConnected();
                        resourceIt->second->AddUpdate(GetProxyUpdateEv());
                        SendProxySessionIfNotSent(resState);
                    } else {
                        // TODO: make cache with error results.
                        KESUS_PROXY_LOG_WARN("Resource \"" << resourcePaths[i] << "\" session initialization error: " << KesusErrorToString(resResult.GetError()));
                        ProcessSubscribeResourceError(resResult.GetError().GetStatus(), resState);
                    }
                }
            }
        } // else it was old request that was retried.
    }

    void Handle(NKesus::TEvKesus::TEvResourcesAllocated::TPtr& ev) {
        KESUS_PROXY_LOG_TRACE("ResourcesAllocated(" << ev->Get()->Record << ")");
        const TInstant now = TActivationContext::Now();
        for (const NKikimrKesus::TEvResourcesAllocated::TResourceInfo& allocatedInfo : ev->Get()->Record.GetResourcesInfo()) {
            TResourceState* res = FindResource(allocatedInfo.GetResourceId());
            if (!res) {
                continue;
            }
            if (allocatedInfo.GetStateNotification().GetStatus() == Ydb::StatusIds::SUCCESS) {
                const auto amount = allocatedInfo.GetAmount();
                KESUS_PROXY_LOG_TRACE("Kesus allocated {\"" << res->Resource << "\", " << amount << "}");
                if (allocatedInfo.HasEffectiveProps()) { // changed
                    res->SetProps(allocatedInfo.GetEffectiveProps(), ServerVersion);
                }
                res->SetAvailable(res->Available + amount);
                res->LastAllocated = now;
                res->AllocStats.OnResourceAllocated(now, amount);
                res->TotalAllocated += amount;
                res->Counters.ReceivedFromKesus += amount;
                CheckState(*res);
                res->AddUpdate(GetProxyUpdateEv());
            } else {
                KESUS_PROXY_LOG_WARN("Resource [" << res->Resource << "] is broken: " << KesusErrorToString(allocatedInfo.GetStateNotification()));
                BreakResource(*res, GetProxyUpdateEv());
            }
        }
    }

    void Handle(TEvPrivate::TEvOfflineResourceAllocation::TPtr& ev) {
        if (ev->Cookie != OfflineAllocationCookie) { // From previous disconnections
            return;
        }

        KESUS_PROXY_LOG_TRACE("OfflineResourceAllocation(" << PrintResources(*ev->Get()) << ")");
        const TInstant now = TActivationContext::Now();
        for (const TEvPrivate::TEvOfflineResourceAllocation::TResourceInfo& allocatedInfo : ev->Get()->Resources) {
            TResourceState* res = FindResource(allocatedInfo.ResourceId);
            if (!res) {
                continue;
            }
            const bool wasActive = res->SessionIsActive;
            KESUS_PROXY_LOG_TRACE("Allocated {\"" << res->Resource << "\", " << allocatedInfo.Amount << "} offline");
            res->SetAvailable(res->Available + allocatedInfo.Amount);
            res->LastAllocated = now;
            CheckState(*res);
            res->AddUpdate(GetProxyUpdateEv());
            if (wasActive) {
                MarkResourceForOfflineAllocation(*res, now);
            }

            res->Counters.AllocatedOffline += allocatedInfo.Amount;
        }
    }

    void Handle(NKesus::TEvKesus::TEvUpdateConsumptionStateAck::TPtr&) {
    }

    void Handle(NKesus::TEvKesus::TEvSyncResources::TPtr& ev) {
        KESUS_PROXY_LOG_TRACE("SyncResources(" << ev->Get()->Record << ")");
        const TInstant now = TActivationContext::Now();
        for (const NKikimrKesus::TEvSyncResources::TResourceInfo& syncInfo : ev->Get()->Record.GetResourcesInfo()) {
            TResourceState* res = FindResource(syncInfo.GetResourceId());
            if (!res) {
                continue;
            }

            const auto available = syncInfo.GetAvailable();
            KESUS_PROXY_LOG_TRACE("Kesus sync {\"" << res->Resource << "\", " << available << "}");
            while (!res->ReportHistory.empty() && res->ReportHistory.front().ReportId < syncInfo.GetLastReportId()) {
               res->ReportHistory.pop_front();
            }
            double consumedLagCompensation = res->ReportHistory.empty() ? 0 : res->TotalConsumed - res->ReportHistory.front().TotalConsumed;
            double allocatedLagCompensation = res->ReportHistory.empty() ? 0 : res->TotalAllocated - res->ReportHistory.front().TotalAllocated;
            res->SetAvailable(available - consumedLagCompensation + allocatedLagCompensation);
            res->LastAllocated = now;
            CheckState(*res);
            res->AddUpdate(GetProxyUpdateEv());
        }
    }

    void Handle(NKesus::TEvKesus::TEvAccountResourcesAck::TPtr& ev) {
        const auto& result = ev->Get()->Record;
        KESUS_PROXY_LOG_TRACE("AccountResourcesAck(" << result << ")");
        for (int i = 0; i < result.GetResourcesInfo().size(); ++i) {
            const auto& resInfo = result.GetResourcesInfo(i);
            if (TResourceState* res = FindResource(resInfo.GetResourceId())) {
                res->HistoryAccepted = Max(res->HistoryAccepted, TInstant::MicroSeconds(resInfo.GetAcceptedUs()));
            }
        }
    }

    THolder<TEvQuota::TEvProxyUpdate> CreateUpdateEvent(TEvQuota::EUpdateState state = TEvQuota::EUpdateState::Normal) const {
        return MakeHolder<TEvQuota::TEvProxyUpdate>(QuoterId, state);
    }

    TString PrintResources(const TEvQuota::TEvProxyUpdate& ev) const {
        TStringBuilder ret;
        ret << "[";
        for (size_t i = 0; i < ev.Resources.size(); ++i) {
            ret << (i > 0 ? ", { " : "{ ");
            const auto& update = ev.Resources[i];
            const TResourceState* res = FindResource(update.ResourceId);
            if (res) {
                ret << "\"" << res->Resource << "\"";
            } else {
                ret << update.ResourceId;
            }
            ret << ", " << update.ResourceState;
            for (size_t j = 0; j < update.Update.size(); ++j) {
                const auto& updateTick = update.Update[j];
                ret << ", {"<< updateTick.Channel << ": " << updateTick.Policy << "(" << updateTick.Rate << ", " << updateTick.Ticks << ")}";
            }
            ret << " }";
        }
        ret << "]";
        return std::move(ret);
    }

    TString PrintResources(const TEvQuota::TEvProxyStats& stats) const {
        TStringBuilder ret;
        ret << "[";
        bool first = true;
        for (const TEvQuota::TProxyStat& stat : stats.Stats) {
            ret << (first ? "{" : ", {");
            first = false;
            if (const auto* res = FindResource(stat.ResourceId)) {
                ret << "\"" << res->Resource << "\"";
            } else {
                ret << stat.ResourceId;
            }
            ret << ", Consumed: " << stat.Consumed << ", Queue: " << stat.QueueWeight << "}";
        }
        ret << "]";
        return std::move(ret);
    }

    TString PrintResources(const TEvPrivate::TEvOfflineResourceAllocation& alloc) {
        TStringBuilder ret;
        ret << "[";
        bool first = true;
        for (const TEvPrivate::TEvOfflineResourceAllocation::TResourceInfo& resInfo : alloc.Resources) {
            ret << (first ? "{ " : ", { ");
            first = false;
            if (const auto* res = FindResource(resInfo.ResourceId)) {
                ret << "\"" << res->Resource << "\"";
            } else {
                ret << resInfo.ResourceId;
            }
            ret << ", " << resInfo.Amount << " }";
        }
        ret << "]";
        return std::move(ret);
    }

    void SendToService(THolder<TEvQuota::TEvProxyUpdate>&& ev) {
        KESUS_PROXY_LOG_TRACE("ProxyUpdate(" << ev->QuoterState << ", " << PrintResources(*ev) << ")");
        Send(QuoterServiceId, std::move(ev));
    }

    void CheckState(TResourceState& res) {
        if (res.SessionIsActive && res.Available >= res.ResourceBucketMaxSize + res.QueueWeight) {
            ActivateSession(res, false);
        } else if (!res.SessionIsActive && res.Available < res.ResourceBucketMinSize + res.QueueWeight) {
            ActivateSession(res);
        }
    }

    void CheckAccountingReport(TResourceState& res, TInstant now) {
        if (res.LastAccountingReport + res.AccountingReportPeriod < now && res.PendingAccountingReport) {
            ReportAccountingSession(res);
            // `LastAccountingReport` must be aligned to send resources' stats in one message
            // in case they have the same `ReportPeriod`
            Y_ASSERT(res.AccountingReportPeriod < TDuration::Max());
            ui64 periodUs = res.AccountingReportPeriod.MicroSeconds();
            res.LastAccountingReport = TInstant::MicroSeconds(now.MicroSeconds() / periodUs * periodUs);
        }
    }

    void CheckReplicationReport(TResourceState& res, TInstant now) {
        if (res.LastReplicationReport + res.ReplicationReportPeriod < now) {
            ReportReplicationSession(res);
            // `LastReplicationReport` must be aligned to send resources' stats in one message
            // in case they have the same `ReportPeriod`
            Y_ASSERT(res.ReplicationReportPeriod < TDuration::Max());
            ui64 periodUs = res.ReplicationReportPeriod.MicroSeconds();
            res.LastReplicationReport = TInstant::MicroSeconds(now.MicroSeconds() / periodUs * periodUs);
        }
    }

    static TString GetLogPrefix(const TVector<TString>& path) {
        return TStringBuilder() << "[" << CanonizePath(path) << "]: ";
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::QUOTER_PROXY_ACTOR;
    }

    TKesusQuoterProxy(ui64 quoterId, const NSchemeCache::TSchemeCacheNavigate::TEntry& navEntry, const TActorId& quoterServiceId, THolder<ITabletPipeFactory> tabletPipeFactory)
        : QuoterServiceId(quoterServiceId)
        , QuoterId(quoterId)
        , Path(navEntry.Path)
        , LogPrefix(GetLogPrefix(Path))
        , KesusInfo(navEntry.KesusInfo)
        , TabletPipeFactory(std::move(tabletPipeFactory))
    {
        Y_ABORT_UNLESS(KesusInfo);
        Y_ABORT_UNLESS(GetKesusTabletId());
        Y_ABORT_UNLESS(TabletPipeFactory);
        Y_UNUSED(QuoterId);

        QUOTER_SYSTEM_DEBUG(DebugInfo->KesusQuoterProxies.emplace(CanonizePath(Path), this));
    }

    ~TKesusQuoterProxy() {
        QUOTER_SYSTEM_DEBUG(DebugInfo->KesusQuoterProxies.erase(CanonizePath(Path)));
    }

    void Bootstrap() {
        KESUS_PROXY_LOG_INFO("Created kesus quoter proxy. Tablet id: " << GetKesusTabletId());
        Counters.Init(CanonizePath(Path));
        Become(&TThis::StateFunc);
        ConnectToKesus(false);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMon::TEvHttpInfo, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
            hFunc(TEvQuota::TEvProxyRequest, Handle);
            hFunc(TEvQuota::TEvProxyStats, Handle);
            hFunc(TEvQuota::TEvProxyCloseSession, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(NKesus::TEvKesus::TEvSubscribeOnResourcesResult, Handle);
            hFunc(NKesus::TEvKesus::TEvResourcesAllocated, Handle);
            hFunc(NKesus::TEvKesus::TEvUpdateConsumptionStateAck, Handle);
            hFunc(NKesus::TEvKesus::TEvAccountResourcesAck, Handle);
            hFunc(TEvPrivate::TEvOfflineResourceAllocation, Handle);
            hFunc(NKesus::TEvKesus::TEvSyncResources, Handle);
            IgnoreFunc(NKesus::TEvKesus::TEvReportResourcesAck);
            default:
                KESUS_PROXY_LOG_WARN("TKesusQuoterProxy::StateFunc unexpected event type# "
                    << ev->GetTypeRewrite()
                    << " event: "
                    << ev->ToString());
                Y_DEBUG_ABORT("Unknown event");
                break;
        }

        ScheduleOfflineAllocation();
        SendDeferredEvents();
    }

    ui64 GetKesusTabletId() const {
        return KesusInfo->Description.GetKesusTabletId();
    }

    NTabletPipe::TClientConfig GetPipeConnectionOptions(bool reconnection) {
        NTabletPipe::TClientConfig cfg;
        cfg.CheckAliveness = true;
        cfg.RetryPolicy = {
            .RetryLimitCount = 3u,
            .DoFirstRetryInstantly = !reconnection
        };
        return cfg;
    }

    void CleanupPreviousConnection() {
        if (KesusPipeClient) {
            NTabletPipe::CloseClient(SelfId(), KesusPipeClient);
        }
        CookieToResourcePath.clear(); // we will resend all requests with new cookies
    }

    void ConnectToKesus(bool reconnection) {
        if (reconnection) {
            KESUS_PROXY_LOG_INFO("Reconnecting to kesus");
        } else {
            KESUS_PROXY_LOG_DEBUG("Connecting to kesus");
        }
        CleanupPreviousConnection();

        KesusPipeClient =
            Register(
                TabletPipeFactory->CreateTabletPipe(
                    SelfId(),
                    GetKesusTabletId(),
                    GetPipeConnectionOptions(reconnection)));
        Connected = false;
    }

    void PassAway() override {
        if (KesusPipeClient) {
            NTabletPipe::CloseClient(SelfId(), KesusPipeClient);
        }
        TActorBootstrapped::PassAway();
    }
};

struct TDefaultTabletPipeFactory : public ITabletPipeFactory {
    IActor* CreateTabletPipe(const NActors::TActorId& owner, ui64 tabletId, const NKikimr::NTabletPipe::TClientConfig& config) override {
        return NTabletPipe::CreateClient(owner, tabletId, config);
    }
};

THolder<ITabletPipeFactory> ITabletPipeFactory::GetDefaultFactory() {
    return MakeHolder<TDefaultTabletPipeFactory>();
}

IActor* CreateKesusQuoterProxy(ui64 quoterId, const NSchemeCache::TSchemeCacheNavigate::TEntry& navEntry, const TActorId& quoterServiceId, THolder<ITabletPipeFactory> tabletPipeFactory) {
    return new TKesusQuoterProxy(quoterId, navEntry, quoterServiceId, std::move(tabletPipeFactory));
}

TKesusResourceAllocationStatistics::TKesusResourceAllocationStatistics(size_t windowSize)
    : BestPrevStat(windowSize)
    , Stat(windowSize)
{
    Y_ASSERT(windowSize >= 2);
}

void TKesusResourceAllocationStatistics::SetProps(const NKikimrKesus::TStreamingQuoterResource& props) {
    DefaultAllocationDelta = TDuration::MilliSeconds(100);
    DefaultAllocationAmount = props.GetHierarchicalDRRResourceConfig().GetMaxUnitsPerSecond() / 10;
}

void TKesusResourceAllocationStatistics::OnConnected() {
    if (Stat.AvailSize() >= BestPrevStat.AvailSize()) {
        BestPrevStat = std::move(Stat);
    }
    Stat.Clear();
}

void TKesusResourceAllocationStatistics::OnResourceAllocated(TInstant now, double amount) {
    Stat.PushBack({now, amount});
}

std::pair<TDuration, double> TKesusResourceAllocationStatistics::GetAverageAllocationParams() const {
    if (Stat.AvailSize() >= 2 && Stat.AvailSize() >= BestPrevStat.AvailSize()) {
        return GetAverageAllocationParams(Stat);
    }
    if (BestPrevStat.AvailSize() >= 2) {
        return GetAverageAllocationParams(BestPrevStat);
    }

    Y_ASSERT(DefaultAllocationDelta != TDuration::Zero());
    Y_ASSERT(DefaultAllocationAmount > 0);
    return {DefaultAllocationDelta, DefaultAllocationAmount};
}

std::pair<TDuration, double> TKesusResourceAllocationStatistics::GetAverageAllocationParams(const TSimpleRingBuffer<TStatItem>& stat) {
    const TDuration window = stat[stat.TotalSize() - 1].Time - stat[stat.FirstIndex()].Time;
    double totalAmount = 0;
    for (size_t i = stat.FirstIndex(), size = stat.TotalSize(); i < size; ++i) {
        totalAmount += stat[i].Amount;
    }
    return {window / (stat.AvailSize() - 1), totalAmount / stat.AvailSize()};
}

}
}
