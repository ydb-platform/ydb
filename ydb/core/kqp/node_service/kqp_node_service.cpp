#include "kqp_node_service.h"
#include "kqp_node_state.h"
#include "kqp_query_control_plane.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/mon/mon.h>

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_actor.h>
#include <ydb/core/kqp/rm_service/kqp_resource_estimation.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/kqp/runtime/kqp_read_actor.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>
#include <ydb/core/kqp/runtime/kqp_write_actor_settings.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/common/shutdown/events.h>

#include <ydb/library/wilson_ids/wilson.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/async/wait_for_event.h>

#include <util/string/join.h>

namespace NKikimr {
namespace NKqp {

using namespace NActors;

namespace {

// Min interval between stats send from scan/compute actor to executor
constexpr TDuration MinStatInterval = TDuration::MilliSeconds(20);
// Max interval in case of no activity
constexpr TDuration MaxStatInterval = TDuration::Seconds(1);

class TKqpNodeService : public TActorBootstrapped<TKqpNodeService> {
    using TBase = TActorBootstrapped<TKqpNodeService>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_NODE_SERVICE;
    }

    TKqpNodeService(const NKikimrConfig::TTableServiceConfig& config,
        std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
        std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> caFactory,
        const TIntrusivePtr<TKqpCounters>& counters,
        NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
        const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup)
        : Config(config.GetResourceManager())
        , Counters(counters)
        , ResourceManager_(std::move(resourceManager))
        , CaFactory_(std::move(caFactory))
        , AsyncIoFactory(std::move(asyncIoFactory))
        , FederatedQuerySetup(federatedQuerySetup)
    {
        CaFactory_->AccountDefaultPoolInScheduler.store(config.GetComputeSchedulerSettings().GetAccountDefaultPool());
        if (config.HasIteratorReadsRetrySettings()) {
            SetIteratorReadsRetrySettings(config.GetIteratorReadsRetrySettings());
        }
        if (config.HasIteratorReadQuotaSettings()) {
            SetIteratorReadsQuotaSettings(config.GetIteratorReadQuotaSettings());
        }
        if (config.HasWriteActorSettings()) {
            SetWriteActorSettings(config.GetWriteActorSettings());
        }
    }

    void Bootstrap() {
        STLOG_I("Starting KQP Node service",
            (node_id, SelfId().NodeId()));

        State_ = std::make_shared<TNodeState>();

        // Subscribe for TableService config changes
        ui32 tableServiceConfigKind = (ui32) NKikimrConsole::TConfigItem::TableServiceConfigItem;
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
             new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({tableServiceConfigKind}),
             IEventHandle::FlagTrackDelivery);

        NActors::TMon* mon = AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "kqp_node", "KQP Node", false,
                TActivationContext::ActorSystem(), SelfId());
        }

        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
        Become(&TKqpNodeService::WorkState);
    }

private:
    STATEFN(WorkState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqpNode::TEvStartKqpTasksRequest, HandleWork);
            hFunc(TEvKqpNode::TEvCancelKqpTasksRequest, HandleWork);
            hFunc(TEvents::TEvWakeup, HandleWork);
            hFunc(TEvKqp::TEvInitiateShutdownRequest, HandleWork);
            // misc
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleWork);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleWork);
            hFunc(TEvents::TEvUndelivered, HandleWork);
            hFunc(TEvents::TEvPoison, HandleWork);
            hFunc(NMon::TEvHttpInfo, HandleWork);
            default: {
                Y_ABORT("Unexpected event 0x%x for TKqpNodeService in WorkState", ev->GetTypeRewrite());
            }
        }
    }

    STATEFN(ShuttingDownState) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvKqpNode::TEvStartKqpTasksRequest, HandleShuttingDown);
            hFunc(TEvKqpNode::TEvCancelKqpTasksRequest , HandleWork);

            // misc
            hFunc(TEvents::TEvWakeup, HandleShuttingDown);
            hFunc(TEvents::TEvPoison, HandleShuttingDown);
            hFunc(NMon::TEvHttpInfo, HandleShuttingDown);
            hFunc(TEvents::TEvUndelivered, HandleWork);

            IgnoreFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
            IgnoreFunc(NConsole::TEvConsole::TEvConfigNotificationRequest);

            default: {
                STLOG_W("Ignoring unexpected event 0x%x (" << ev->GetTypeName()
                    << ") during graceful shutdown",
                    (node_id, SelfId().NodeId()),
                    (sender, ev->Sender));
            }
        }
    }

    void HandleWork(TEvKqpNode::TEvStartKqpTasksRequest::TPtr ev) {
        NWilson::TSpan sendTasksSpan(TWilsonKqp::KqpNodeSendTasks, NWilson::TTraceId(ev->TraceId), "KqpNode.SendTasks", NWilson::EFlags::AUTO_END);

        const auto executerId = ev->Sender;

        if (!CachedQueryManagerId) {
            CachedQueryManagerId = Register(CreateKqpQueryManager(Counters, State_, ResourceManager_, CaFactory_));
        }

        TActorId queryManagerId;
        bool cancelled = false;
        auto result = State_->AddRequest(executerId, CachedQueryManagerId, cancelled, queryManagerId);
        if (result) {
            CachedQueryManagerId = TActorId{};
        } else if (cancelled) {
            return ReplyError(executerId, ev->Get()->Record, NKikimrKqp::TEvStartKqpTasksResponse::INTERNAL_ERROR,
                ev->Cookie, "Request was cancelled");
        }

        YQL_ENSURE(queryManagerId);
        Send(ev->Forward(queryManagerId));
    }

    void HandleWork(TEvKqpNode::TEvCancelKqpTasksRequest::TPtr& ev) {
        THPTimer timer;
        ui64 txId = ev->Get()->Record.GetTxId();
        const auto executerId = ev->Sender;
        auto& reason = ev->Get()->Record.GetReason();

        STLOG_W("Terminate transaction",
            (node_id, SelfId().NodeId()),
            (tx_id, txId),
            (reason, reason));
        TerminateTx(txId, executerId, reason);

        Counters->NodeServiceProcessCancelTime->Collect(timer.Passed() * SecToUsec);
    }

    void TerminateTx(ui64 txId, TActorId executerId, const TString& reason, NYql::NDqProto::StatusIds_StatusCode status = NYql::NDqProto::StatusIds::UNSPECIFIED) {
        State_->MarkRequestAsCancelled(executerId);

        if (auto tasksToAbort = State_->GetTasksByExecuterId(executerId); !tasksToAbort.empty()) {
            STLOG_E("Node service cancelled the task, because it " << reason,
                (node_id, SelfId().NodeId()),
                (tx_id, txId));
            for (const auto& [taskId, computeActorId]: tasksToAbort) {
                auto abortEv = std::make_unique<TEvKqp::TEvAbortExecution>(status, reason);
                Send(computeActorId, abortEv.release());
            }
        }
    }

    void HandleWork(TEvents::TEvWakeup::TPtr& ev) {
        Schedule(TDuration::Seconds(1), ev->Release().Release());
        auto expiredRequests = State_->ClearExpiredRequests();
        for (auto info : expiredRequests) {
            TerminateTx(std::get<ui64>(info), std::get<TActorId>(info), "reached execution deadline", NYql::NDqProto::StatusIds::TIMEOUT);
        }
    }

    void HandleWork(TEvKqp::TEvInitiateShutdownRequest::TPtr& ev) {
        if (!AppData()->FeatureFlags.GetEnableShuttingDownNodeState()) {
            STLOG_I("Feature flag EnableShuttingDownNodeState is disabled, ignoring shutdown request",
                (node_id, SelfId().NodeId()));
            return;
        }
        STLOG_I("Prepare to shutdown: do not accept any messages from this time",
            (node_id, SelfId().NodeId()));
        ShutdownState_.Reset(ev->Get()->ShutdownState.Get());
        Become(&TKqpNodeService::ShuttingDownState);
    }

    void HandleShuttingDown(TEvKqpNode::TEvStartKqpTasksRequest::TPtr& ev) {
        // in shutting down state do not accept new tasks, but accept local requests
        // continue to process tasks that are already started before shutdown
        auto& msg = ev->Get()->Record;
        if (ev->Sender.NodeId() == SelfId().NodeId()) {
            STLOG_D("Accepting local StartRequest during shutdown",
                (node_id, SelfId().NodeId()),
                (tx_id, msg.GetTxId()));
            HandleWork(ev);
        } else if (msg.HasSupportShuttingDown() && msg.GetSupportShuttingDown()) {
            STLOG_D("Rejecting remote StartRequest in ShuttingDown State",
                (node_id, SelfId().NodeId()),
                (tx_id, msg.GetTxId()));
            ReplyError(ev->Sender, msg, NKikimrKqp::TEvStartKqpTasksResponse::NODE_SHUTTING_DOWN, ev->Cookie);
        } else {
            HandleWork(ev);
        }
    }

    void HandleShuttingDown(TEvents::TEvWakeup::TPtr& ev) {
        HandleWork(ev);
    }

    void HandleShuttingDown(TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

    void HandleShuttingDown(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;
        HTML(str) {
            PRE() {
                str << "This node is in graceful shutdown mode and will not accept new requests." << Endl;
                str << "Current config:" << Endl;
                str << Config.DebugString() << Endl;
                str << Endl;

                str << "Active Transactions:" << Endl;
                State_->DumpInfo(str, ev->Get()->Request.GetParams());
                str << Endl;
            }
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }
private:
    static void HandleWork(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        STLOG_D("Subscribed for config changes");
    }

    void HandleWork(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        auto &event = ev->Get()->Record;

        if (event.GetConfig().GetTableServiceConfig().GetResourceManager().IsInitialized()) {
            Config.Swap(event.MutableConfig()->MutableTableServiceConfig()->MutableResourceManager());

#define FORCE_VALUE(name) if (!Config.Has ## name ()) Config.Set ## name(Config.Get ## name());
            FORCE_VALUE(ComputeActorsCount)
            FORCE_VALUE(ChannelBufferSize)
            FORCE_VALUE(MkqlLightProgramMemoryLimit)
            FORCE_VALUE(MkqlHeavyProgramMemoryLimit)
            FORCE_VALUE(QueryMemoryLimit)
            FORCE_VALUE(PublishStatisticsIntervalSec);
            FORCE_VALUE(MaxTotalChannelBuffersSize);
            FORCE_VALUE(MinChannelBufferSize);
            FORCE_VALUE(MinMemAllocSize);
            FORCE_VALUE(MinMemFreeSize);
#undef FORCE_VALUE

            CaFactory_->ApplyConfig(Config);
            CaFactory_->AccountDefaultPoolInScheduler.store(event.GetConfig().GetTableServiceConfig().GetComputeSchedulerSettings().GetAccountDefaultPool());

            STLOG_I("Updated table service RM config",
                (node_id, SelfId().NodeId()),
                (config, Config.DebugString()));
        }

        if (event.GetConfig().GetTableServiceConfig().HasIteratorReadsRetrySettings()) {
            SetIteratorReadsRetrySettings(event.GetConfig().GetTableServiceConfig().GetIteratorReadsRetrySettings());
        }

        if (event.GetConfig().GetTableServiceConfig().HasIteratorReadQuotaSettings()) {
            SetIteratorReadsQuotaSettings(event.GetConfig().GetTableServiceConfig().GetIteratorReadQuotaSettings());
        }

        if (event.GetConfig().GetTableServiceConfig().HasWriteActorSettings()) {
            SetWriteActorSettings(event.GetConfig().GetTableServiceConfig().GetWriteActorSettings());
        }

        auto responseEv = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEv.Release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void SetIteratorReadsQuotaSettings(const NKikimrConfig::TTableServiceConfig::TIteratorReadQuotaSettings& settings) {
        SetDefaultIteratorQuotaSettings(settings.GetMaxRows(), settings.GetMaxBytes());
    }

    void SetIteratorReadsRetrySettings(const NKikimrConfig::TTableServiceConfig::TIteratorReadsRetrySettings& settings) {
        auto ptr = MakeIntrusive<NKikimr::NKqp::TIteratorReadBackoffSettings>();
        ptr->StartRetryDelay = TDuration::MilliSeconds(settings.GetStartDelayMs());
        ptr->MaxShardAttempts = settings.GetMaxShardRetries();
        ptr->MaxShardResolves = settings.GetMaxShardResolves();
        ptr->UnsertaintyRatio = settings.GetUnsertaintyRatio();
        ptr->Multiplier = settings.GetMultiplier();
        if (settings.GetMaxTotalRetries()) {
            ptr->MaxTotalRetries = settings.GetMaxTotalRetries();
        }
        if (settings.GetIteratorResponseTimeoutMs()) {
            ptr->ReadResponseTimeout = TDuration::MilliSeconds(settings.GetIteratorResponseTimeoutMs());
        }
        ptr->MaxRetryDelay = TDuration::MilliSeconds(settings.GetMaxDelayMs());
        ptr->MaxRowsProcessingStreamLookup = settings.GetMaxRowsProcessingStreamLookup();
        ptr->MaxTotalBytesQuotaStreamLookup = settings.GetMaxTotalBytesQuotaStreamLookup();
        SetReadIteratorBackoffSettings(ptr);
    }

    void SetWriteActorSettings(const NKikimrConfig::TTableServiceConfig::TWriteActorSettings& settings) {
        auto ptr = MakeIntrusive<NKikimr::NKqp::TWriteActorSettings>();

        ptr->InFlightMemoryLimitPerActorBytes = settings.GetInFlightMemoryLimitPerActorBytes();

        ptr->StartRetryDelay = TDuration::MilliSeconds(settings.GetStartRetryDelayMs());
        ptr->MaxRetryDelay = TDuration::MilliSeconds(settings.GetMaxRetryDelayMs());
        ptr->UnsertaintyRatio = settings.GetUnsertaintyRatio();
        ptr->Multiplier = settings.GetMultiplier();

        ptr->MaxWriteAttempts = settings.GetMaxWriteAttempts();
        ptr->MaxResolveAttempts = settings.GetMaxResolveAttempts();

        NKikimr::NKqp::SetWriteActorSettings(ptr);
    }

    void HandleWork(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case TEvKqpNode::TEvStartKqpTasksResponse::EventType: {
                ui64 txId = ev->Cookie;
                const auto executerId = ev->Sender;
                TStringBuilder reason;
                reason << "executer lost: " << (int) ev->Get()->Reason;
                TerminateTx(txId, executerId, reason, NYql::NDqProto::StatusIds::ABORTED);
                break;
            }

            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                STLOG_C("Failed to deliver subscription request to config dispatcher",
                    (node_id, SelfId().NodeId()));
                break;

            case NConsole::TEvConsole::EvConfigNotificationResponse:
                STLOG_E("Failed to deliver config notification response",
                    (node_id, SelfId().NodeId()));
                break;

            default:
                STLOG_E("Undelivered event with unexpected source type",
                    (node_id, SelfId().NodeId()),
                    (source_type, ev->Get()->SourceType));
                break;
        }
    }

    void HandleWork(TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

    void HandleWork(NMon::TEvHttpInfo::TPtr& ev) {

        const TCgiParameters &cgi = ev->Get()->Request.GetParams();

        auto caId = cgi.Get("ca");
        if (caId) {
            UrlUnescape(caId);
            TActorId computeActorId;
            if (computeActorId.Parse(caId.c_str(), caId.size())) {
                if (computeActorId.NodeId() != SelfId().NodeId()) {
                    TStringStream response;
                    response << "HTTP/1.1 307 Temporary Redirect\r\n";
                    response << "Location: /node/" << computeActorId.NodeId() << "/actors/kqp_node?" << cgi.Print() << "\r\n";
                    response << "Connection: Keep-Alive\r\n";
                    response << "\r\n";
                    Send(ev->Sender, new NMon::TEvHttpInfoRes(response.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
                    return;
                }
                if (State_->ValidateComputeActorId(caId, computeActorId)) {
                    TActivationContext::Send(ev->Forward(computeActorId));
                    return;
                }
            }
        }

        auto exId = cgi.Get("ex");
        if (exId) {
            UrlUnescape(exId);
            TActorId executerId;
            if (executerId.Parse(exId.c_str(), exId.size())) {
                if (executerId.NodeId() != SelfId().NodeId()) {
                    TStringStream response;
                    response << "HTTP/1.1 307 Temporary Redirect\r\n";
                    response << "Location: /node/" << executerId.NodeId() << "/actors/kqp_node?" << cgi.Print() << "\r\n";
                    response << "Connection: Keep-Alive\r\n";
                    response << "\r\n";
                    Send(ev->Sender, new NMon::TEvHttpInfoRes(response.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
                    return;
                }
                if (State_->ValidateKqpExecuterId(exId, executerId)) {
                    TActivationContext::Send(ev->Forward(executerId));
                    return;
                }
            }
        }

        TStringStream str;
        HTML(str) {
            PRE() {
                str << "TKqpNodeService, SelfId=" << SelfId() << Endl;
                str << Endl << "Current config:" << Endl;
                str << Config.DebugString() << Endl;
                State_->DumpInfo(str, cgi);
            }
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

    void PassAway() override {
        if (CachedQueryManagerId) {
            Send(CachedQueryManagerId, new NActors::TEvents::TEvPoison());
        }
        TBase::PassAway();
    }

private:
    void ReplyError(TActorId executerId, const NKikimrKqp::TEvStartKqpTasksRequest& request,
        NKikimrKqp::TEvStartKqpTasksResponse::ENotStartedTaskReason reason, ui64 requestId, const TString& message = "")
    {
        auto ev = MakeHolder<TEvKqpNode::TEvStartKqpTasksResponse>();
        ev->Record.SetTxId(request.GetTxId());
        for (auto& task : request.GetTasks()) {
            auto* resp = ev->Record.AddNotStartedTasks();
            resp->SetTaskId(task.GetId());
            resp->SetReason(reason);
            resp->SetMessage(message);
            resp->SetRequestId(requestId);
        }
        Send(executerId, ev.Release());
    }

private:
    NKikimrConfig::TTableServiceConfig::TResourceManager Config;
    TIntrusivePtr<TKqpCounters> Counters;
    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager_;
    std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> CaFactory_;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
    const std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;

    // state sharded by TxId
    std::shared_ptr<TNodeState> State_;
    TIntrusivePtr<TKqpShutdownState> ShutdownState_;
    TActorId CachedQueryManagerId;
};


} // anonymous namespace

NYql::NDq::TReportStatsSettings ReportStatsSettingsFromProto(const NYql::NDqProto::TComputeRuntimeSettings& runtimeSettings) {
    return NYql::NDq::TReportStatsSettings{
            .MinInterval = runtimeSettings.HasMinStatsSendIntervalMs() ? TDuration::MilliSeconds(runtimeSettings.GetMinStatsSendIntervalMs()) : MinStatInterval,
            .MaxInterval = runtimeSettings.HasMaxStatsSendIntervalMs() ? TDuration::MilliSeconds(runtimeSettings.GetMaxStatsSendIntervalMs()) : MaxStatInterval,
    };
}


IActor* CreateKqpNodeService(const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
    std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> caFactory,
    TIntrusivePtr<TKqpCounters> counters, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup)
{
    return new TKqpNodeService(tableServiceConfig, std::move(resourceManager), std::move(caFactory),
        counters, std::move(asyncIoFactory), federatedQuerySetup);
}

} // namespace NKqp
} // namespace NKikimr
