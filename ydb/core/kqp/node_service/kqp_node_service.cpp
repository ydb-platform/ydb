#include "kqp_node_service.h"
#include "kqp_node_state.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/mon/mon.h>

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_actor.h>
#include <ydb/core/kqp/rm_service/kqp_resource_estimation.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/kqp/runtime/kqp_read_actor.h>
#include <ydb/core/kqp/common/kqp_resolve.h>

#include <ydb/core/base/wilson.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/actors/wilson/wilson_span.h>

#include <util/string/join.h>

namespace NKikimr {
namespace NKqp {

using namespace NActors;

namespace {

#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)

// Min interval between stats send from scan/compute actor to executor
constexpr TDuration MinStatInterval = TDuration::MilliSeconds(20);
// Max interval in case of no activety
constexpr TDuration MaxStatInterval = TDuration::MilliSeconds(100);

template <class TTasksCollection>
TString TasksIdsStr(const TTasksCollection& tasks) {
    TVector<ui64> ids;
    for (auto& task: tasks) {
        ids.push_back(task.GetId());
    }
    return TStringBuilder() << "[" << JoinSeq(", ", ids) << "]";
}

class TKqpNodeService : public TActorBootstrapped<TKqpNodeService> {
    using TBase = TActorBootstrapped<TKqpNodeService>;

    struct TEvPrivate {
        enum EEv {
            EvTimeout = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        struct TEvTimeout : public TEventLocal<TEvTimeout, EEv::EvTimeout> {
            const ui64 TxId;
            const TActorId Requester;

            TEvTimeout(ui64 txId, const TActorId& requester)
                : TxId(txId)
                , Requester(requester) {}
        };
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_NODE_SERVICE;
    }

    TKqpNodeService(const NKikimrConfig::TTableServiceConfig& config, const TIntrusivePtr<TKqpCounters>& counters,
        IKqpNodeComputeActorFactory* caFactory)
        : Config(config.GetResourceManager())
        , Counters(counters)
        , CaFactory(caFactory)
    {
        if (config.HasIteratorReadsRetrySettings()) {
            SetIteratorReadsRetrySettings(config.GetIteratorReadsRetrySettings());
        }
    }

    void Bootstrap() {
        LOG_I("Starting KQP Node service");

        // Subscribe for TableService config changes
        ui32 tableServiceConfigKind = (ui32) NKikimrConsole::TConfigItem::TableServiceConfigItem;
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
             new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({tableServiceConfigKind}),
             IEventHandle::FlagTrackDelivery);

        NActors::TMon* mon = AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "kqp_node", "KQP Node", false,
                TlsActivationContext->ExecutorThread.ActorSystem, SelfId());
        }

        Become(&TKqpNodeService::WorkState);
    }

private:
    STATEFN(WorkState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqpNode::TEvStartKqpTasksRequest, HandleWork);
            hFunc(TEvKqpNode::TEvFinishKqpTask, HandleWork);
            hFunc(TEvKqpNode::TEvCancelKqpTasksRequest, HandleWork);
            hFunc(TEvPrivate::TEvTimeout, HandleWork);
            // misc
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleWork);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleWork);
            hFunc(TEvents::TEvUndelivered, HandleWork);
            hFunc(TEvents::TEvPoison, HandleWork);
            hFunc(NMon::TEvHttpInfo, HandleWork);
            default: {
                Y_FAIL("Unexpected event 0x%x for TKqpResourceManagerService", ev->GetTypeRewrite());
            }
        }
    }

    void HandleWork(TEvKqpNode::TEvStartKqpTasksRequest::TPtr& ev) {
        NWilson::TSpan sendTasksSpan(TWilsonKqp::KqpNodeSendTasks, NWilson::TTraceId(ev->TraceId), "KqpNode.SendTasks", NWilson::EFlags::AUTO_END);

        auto& msg = ev->Get()->Record;
        auto requester = ev->Sender;

        ui64 txId = msg.GetTxId();
        bool isScan = msg.HasSnapshot();

        YQL_ENSURE(msg.GetStartAllOrFail()); // todo: support partial start

        LOG_D("TxId: " << txId << ", new " << (isScan ? "scan " : "") << "compute tasks request from " << requester
            << " with " << msg.GetTasks().size() << " tasks: " << TasksIdsStr(msg.GetTasks()));

        NKqpNode::TTasksRequest request;
        request.Executer = ActorIdFromProto(msg.GetExecuterActorId());

        if (State.Exists(txId, requester)) {
            LOG_E("TxId: " << txId << ", requester: " << requester << ", request already exists");
            return ReplyError(txId, request.Executer, msg, NKikimrKqp::TEvStartKqpTasksResponse::INTERNAL_ERROR);
        }

        NRm::EKqpMemoryPool memoryPool;
        if (msg.GetRuntimeSettings().GetExecType() == NYql::NDqProto::TComputeRuntimeSettings::SCAN) {
            memoryPool = NRm::EKqpMemoryPool::ScanQuery;
        } else if (msg.GetRuntimeSettings().GetExecType() == NYql::NDqProto::TComputeRuntimeSettings::DATA) {
            memoryPool = NRm::EKqpMemoryPool::DataQuery;
        } else {
            memoryPool = NRm::EKqpMemoryPool::Unspecified;
        }

        ui32 requestChannels = 0;
        for (auto& dqTask : *msg.MutableTasks()) {
            auto estimation = EstimateTaskResources(dqTask, Config);
            LOG_D("Resource estimation complete"
                << ", TxId: " << txId << ", task id: " << dqTask.GetId() << ", node id: " << SelfId().NodeId()
                << ", estimated resources: " << estimation.ToString());

            NKqpNode::TTaskContext& taskCtx = request.InFlyTasks[dqTask.GetId()];
            YQL_ENSURE(taskCtx.TaskId == 0);
            taskCtx.TaskId = dqTask.GetId();
            taskCtx.Memory = estimation.TotalMemoryLimit;
            taskCtx.Channels = estimation.ChannelBuffersCount;
            taskCtx.ChannelSize = estimation.ChannelBufferMemoryLimit;

            LOG_D("TxId: " << txId << ", task: " << taskCtx.TaskId << ", requested memory: " << taskCtx.Memory);

            requestChannels += estimation.ChannelBuffersCount;
            request.TotalMemory += taskCtx.Memory;
        }

        LOG_D("TxId: " << txId << ", channels: " << requestChannels
            << ", computeActors: " << msg.GetTasks().size() << ", memory: " << request.TotalMemory);

        ui64 txMemory = State.GetTxMemory(txId, memoryPool) + request.TotalMemory;
        if (txMemory > Config.GetQueryMemoryLimit()) {
            LOG_N("TxId: " << txId << ", requested too many memory: " << request.TotalMemory
                << "(" << txMemory << " for this Tx), limit: " << Config.GetQueryMemoryLimit());

            Counters->RmNotEnoughMemory->Inc();

            return ReplyError(txId, request.Executer, msg, NKikimrKqp::TEvStartKqpTasksResponse::QUERY_MEMORY_LIMIT_EXCEEDED,
                TStringBuilder() << "Required: " << txMemory << ", limit: " << Config.GetQueryMemoryLimit());
        }

        TVector<ui64> allocatedTasks;
        allocatedTasks.reserve(msg.GetTasks().size());
        for (auto& task : request.InFlyTasks) {
            NRm::TKqpResourcesRequest resourcesRequest;
            resourcesRequest.ExecutionUnits = 1;
            resourcesRequest.MemoryPool = memoryPool;

            // !!!!!!!!!!!!!!!!!!!!!
            // we have to allocate memory instead of reserve only. currently, this memory will not be used for request processing.
            resourcesRequest.Memory = Min<double>(task.second.Memory, 1 << 19) /* 512kb limit for check that memory exists for processing with minimal requirements */;

            NRm::TKqpNotEnoughResources resourcesResponse;
            if (!ResourceManager()->AllocateResources(txId, task.first, resourcesRequest, &resourcesResponse)) {
                NKikimrKqp::TEvStartKqpTasksResponse::ENotStartedTaskReason failReason = NKikimrKqp::TEvStartKqpTasksResponse::INTERNAL_ERROR;
                TStringBuilder error;

                if (resourcesResponse.ExecutionUnits()) {
                    error << "TxId: " << txId << ", NodeId: " << SelfId().NodeId() << ", not enough compute actors, requested " << msg.GetTasks().size();
                    LOG_N(error);

                    failReason = NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_EXECUTION_UNITS;
                }

                if (resourcesResponse.ScanQueryMemory()) {
                    error << "TxId: " << txId << ", NodeId: " << SelfId().NodeId() << ", not enough memory, requested " << task.second.Memory;
                    LOG_N(error);

                    failReason = NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY;
                }

                if (resourcesResponse.QueryMemoryLimit()) {
                    error << "TxId: " << txId << ", NodeId: " << SelfId().NodeId() << ", memory limit exceeded, requested " << task.second.Memory;
                    LOG_N(error);

                    failReason = NKikimrKqp::TEvStartKqpTasksResponse::QUERY_MEMORY_LIMIT_EXCEEDED;
                }

                for (ui64 taskId : allocatedTasks) {
                    ResourceManager()->FreeResources(txId, taskId);
                }

                ReplyError(txId, request.Executer, msg, failReason, error);
                return;
            }

            allocatedTasks.push_back(task.first);
        }

        auto reply = MakeHolder<TEvKqpNode::TEvStartKqpTasksResponse>();
        reply->Record.SetTxId(txId);

        NYql::NDq::TComputeMemoryLimits memoryLimits;
        memoryLimits.ChannelBufferSize = 0;
        memoryLimits.MkqlLightProgramMemoryLimit = Config.GetMkqlLightProgramMemoryLimit();
        memoryLimits.MkqlHeavyProgramMemoryLimit = Config.GetMkqlHeavyProgramMemoryLimit();
        if (Config.GetEnableInstantMkqlMemoryAlloc()) {
            memoryLimits.AllocateMemoryFn = [rm = ResourceManager(), memoryPool](const auto& txId, ui64 taskId, ui64 memory) {
                NRm::TKqpResourcesRequest resources;
                resources.MemoryPool = memoryPool;
                resources.Memory = memory;

                if (rm->AllocateResources(std::get<ui64>(txId), taskId, resources)) {
                    return true;
                }

                LOG_W("Can not allocate memory. TxId: " << txId << ", taskId: " << taskId << ", memory: +" << memory);
                return false;
            };
        }

        NYql::NDq::TComputeRuntimeSettings runtimeSettingsBase;
        auto& msgRtSettings = msg.GetRuntimeSettings();
        if (msgRtSettings.GetTimeoutMs() > 0) {
            runtimeSettingsBase.Timeout = TDuration::MilliSeconds(msgRtSettings.GetTimeoutMs());
            request.Deadline = TAppData::TimeProvider->Now() + *runtimeSettingsBase.Timeout;
        }

        runtimeSettingsBase.ExtraMemoryAllocationPool = memoryPool;
        runtimeSettingsBase.FailOnUndelivery = msgRtSettings.GetExecType() != NYql::NDqProto::TComputeRuntimeSettings::SCAN;

        runtimeSettingsBase.StatsMode = msgRtSettings.GetStatsMode();
        runtimeSettingsBase.UseLLVM = msgRtSettings.GetUseLLVM();
        runtimeSettingsBase.UseSpilling = msgRtSettings.GetUseSpilling();

        if (msgRtSettings.HasRlPath()) {
            runtimeSettingsBase.RlPath = msgRtSettings.GetRlPath();
        }

        runtimeSettingsBase.ReportStatsSettings = NYql::NDq::TReportStatsSettings{MinStatInterval, MaxStatInterval};

        TShardsScanningPolicy scanPolicy(Config.GetShardsScanningPolicy());
        auto actorSystem = TlsActivationContext->ActorSystem();

        // start compute actors
        for (int i = 0; i < msg.GetTasks().size(); ++i) {
            auto& dqTask = *msg.MutableTasks(i);
            auto& taskCtx = request.InFlyTasks[dqTask.GetId()];
            YQL_ENSURE(taskCtx.TaskId != 0);

            memoryLimits.ChannelBufferSize = taskCtx.ChannelSize;
            Y_VERIFY_DEBUG(memoryLimits.ChannelBufferSize >= Config.GetMinChannelBufferSize(),
                "actual size: %ld, min: %ld", memoryLimits.ChannelBufferSize, Config.GetMinChannelBufferSize());

            auto runtimeSettings = runtimeSettingsBase;
            runtimeSettings.TerminateHandler = [actorSystem, rm = SelfId(), txId, taskId = dqTask.GetId()]
                (bool success, const NYql::TIssues& issues) {
                    actorSystem->Send(rm, new TEvKqpNode::TEvFinishKqpTask(txId, taskId, success, issues));
                };

            ETableKind tableKind = ETableKind::Unknown;
            {
                NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta meta;
                if (dqTask.GetMeta().UnpackTo(&meta)) {
                    tableKind = (ETableKind)meta.GetTable().GetTableKind();
                    if (tableKind == ETableKind::Unknown) {
                        // For backward compatibility
                        tableKind = meta.GetTable().GetSysViewInfo().empty() ? ETableKind::Datashard : ETableKind::SysView;
                    }
                }
            }

            IActor* computeActor;
            if (tableKind == ETableKind::Datashard || tableKind == ETableKind::Olap) {
                computeActor = CreateKqpScanComputeActor(msg.GetSnapshot(), request.Executer, txId, std::move(dqTask),
                    CreateKqpAsyncIoFactory(Counters), AppData()->FunctionRegistry, runtimeSettings, memoryLimits, scanPolicy,
                    Counters, NWilson::TTraceId(ev->TraceId));
                taskCtx.ComputeActorId = Register(computeActor);
            } else {
                if (Y_LIKELY(!CaFactory)) {
                    computeActor = CreateKqpComputeActor(request.Executer, txId, std::move(dqTask), CreateKqpAsyncIoFactory(Counters),
                        AppData()->FunctionRegistry, runtimeSettings, memoryLimits, NWilson::TTraceId(ev->TraceId));
                    taskCtx.ComputeActorId = Register(computeActor);
                } else {
                    computeActor = CaFactory->CreateKqpComputeActor(request.Executer, txId, std::move(dqTask),
                                                                    runtimeSettings, memoryLimits);
                    taskCtx.ComputeActorId = computeActor->SelfId();
                }
            }

            LOG_D("TxId: " << txId << ", executing task: " << taskCtx.TaskId << " on compute actor: " << taskCtx.ComputeActorId);

            auto* startedTask = reply->Record.AddStartedTasks();
            startedTask->SetTaskId(taskCtx.TaskId);
            ActorIdToProto(taskCtx.ComputeActorId, startedTask->MutableActorId());
        }

        if (runtimeSettingsBase.Timeout) {
            request.TimeoutTimer = CreateLongTimer(TlsActivationContext->AsActorContext(),
                *runtimeSettingsBase.Timeout + /* gap */ TDuration::Seconds(5),
                new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvTimeout(txId, requester)));
        }

        Send(request.Executer, reply.Release(), IEventHandle::FlagTrackDelivery, txId);

        State.NewRequest(txId, requester, std::move(request), memoryPool);
    }

    void HandleWork(TEvKqpNode::TEvFinishKqpTask::TPtr& ev) {
        auto& msg = *ev->Get();

        LOG_D("TxId: " << msg.TxId << ", finish compute task: " << msg.TaskId << ", success: " << msg.Success
            << ", message: " << msg.Issues.ToOneLineString());

        auto task = State.RemoveTask(msg.TxId, msg.TaskId, msg.Success, [this, &msg]
            (const TActorId& requester, const NKqpNode::TTasksRequest& request, const NKqpNode::TTaskContext&, bool finishTx) {
                THolder<IEventBase> ev;

                if (request.InFlyTasks.empty()) {
                    LOG_D("TxId: " << msg.TxId << ", requester: " << requester << " completed");

                    if (request.TimeoutTimer) {
                        Send(request.TimeoutTimer, new TEvents::TEvPoison);
                    }

                    ResourceManager()->FreeResources(msg.TxId);
                } else {
                    LOG_D("TxId: " << msg.TxId << ", finish compute task: " << msg.TaskId
                        << (msg.Success ? "" : " (cancelled)")
                        << ", remains " << request.InFlyTasks.size() << " compute actors and " << request.TotalMemory
                        << " bytes in the current request");

                    ResourceManager()->FreeResources(msg.TxId, msg.TaskId);
                }

                if (finishTx) {
                    LOG_D("TxId: " << msg.TxId << ", requester: " << requester << " completed");
                }
            });

        if (!task) {
            LOG_E("TxId: " << msg.TxId << ", task: " << msg.TaskId << " unknown task");
            return;
        }
    }

    void HandleWork(TEvKqpNode::TEvCancelKqpTasksRequest::TPtr& ev) {
        ui64 txId = ev->Get()->Record.GetTxId();
        auto& reason = ev->Get()->Record.GetReason();

        LOG_W("TxId: " << txId << ", terminate transaction, reason: " << reason);
        TerminateTx(txId, reason);
    }

    void TerminateTx(ui64 txId, const TString& reason) {
        State.RemoveTx(txId, [this, &txId, &reason](const NKqpNode::TTasksRequest& request) {
            LOG_D("TxId: " << txId << ", cancel granted resources");

            if (request.TimeoutTimer) {
                Send(request.TimeoutTimer, new TEvents::TEvPoison);
            }

            ResourceManager()->FreeResources(txId);

            for (auto& [taskId, task] : request.InFlyTasks) {
                auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::UNSPECIFIED, reason);
                Send(task.ComputeActorId, abortEv.Release());
            }
        });
    }

    void HandleWork(TEvPrivate::TEvTimeout::TPtr& ev) {
        ui64 txId = ev->Get()->TxId;
        TActorId requester = ev->Get()->Requester;

        LOG_E("txId: " << txId << ", requester: " << requester << ", execution timeout");

        auto request = State.RemoveRequest(txId, requester);
        if (!request) {
            LOG_E("txId: " << txId << ", requester: " << requester << ", unknown request");
            return;
        }

        ResourceManager()->FreeResources(txId);

        // don't send to executer and compute actors, they have their own timers with smaller timeout
    }

private:
    static void HandleWork(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        LOG_D("Subscribed for config changes");
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
            FORCE_VALUE(EnableInstantMkqlMemoryAlloc);
            FORCE_VALUE(MaxTotalChannelBuffersSize);
            FORCE_VALUE(MinChannelBufferSize);
#undef FORCE_VALUE

            LOG_I("Updated table service config: " << Config.DebugString());
        }

        if (event.GetConfig().GetTableServiceConfig().HasIteratorReadsRetrySettings()) {
            SetIteratorReadsRetrySettings(event.GetConfig().GetTableServiceConfig().GetIteratorReadsRetrySettings());
        }

        auto responseEv = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEv.Release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
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
        SetReadIteratorBackoffSettings(ptr);
    }

    void HandleWork(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case TEvKqpNode::TEvStartKqpTasksResponse::EventType: {
                ui64 txId = ev->Cookie;
                LOG_E("TxId: " << txId << ", executer lost: " << (int) ev->Get()->Reason);

                TerminateTx(txId, "executer lost");
                break;
            }

            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                LOG_C("Failed to deliver subscription request to config dispatcher");
                break;

            case NConsole::TEvConsole::EvConfigNotificationResponse:
                LOG_E("Failed to deliver config notification response");
                break;

            default:
                LOG_E("Undelivered event with unexpected source type: " << ev->Get()->SourceType);
                break;
        }
    }

    void HandleWork(TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

    void HandleWork(NMon::TEvHttpInfo::TPtr& ev) {
        THashMap<ui64, TVector<std::pair<const TActorId, const NKqpNode::TTasksRequest*>>> byTx;
        for (auto& [key, request] : State.Requests) {
            byTx[key.first].emplace_back(key.second, &request);
        }

        TStringStream str;
        HTML(str) {
            PRE() {
                str << "Current config:" << Endl;
                str << Config.DebugString() << Endl;
                str << Endl;

                str << Endl << "Transactions:" << Endl;
                for (auto& [txId, requests] : byTx) {
                    auto& meta = State.Meta[txId];
                    str << "  TxId: " << txId << Endl;
                    str << "    Memory: " << meta.TotalMemory << Endl;
                    str << "    MemoryPool: " << (ui32) meta.MemoryPool << Endl;
                    str << "    Compute actors: " << meta.TotalComputeActors << Endl;
                    str << "    Start time: " << meta.StartTime << Endl;
                    str << "    Requests:" << Endl;
                    for (auto& [requester, request] : requests) {
                        str << "      Requester: " << requester << Endl;
                        str << "        Deadline: " << request->Deadline << Endl;
                        str << "        Memory: " << request->TotalMemory << Endl;
                        str << "        In-fly tasks:" << Endl;
                        for (auto& [taskId, task] : request->InFlyTasks) {
                            str << "          Task: " << taskId << Endl;
                            str << "            Memory: " << task.Memory << Endl;
                            str << "            Channels: " << task.Channels << Endl;
                            str << "            Compute actor: " << task.ComputeActorId << Endl;
                        }
                    }
                }
            }
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

private:
    void ReplyError(ui64 txId, TActorId executer, const NKikimrKqp::TEvStartKqpTasksRequest& request,
        NKikimrKqp::TEvStartKqpTasksResponse::ENotStartedTaskReason reason, const TString& message = "")
    {
        auto ev = MakeHolder<TEvKqpNode::TEvStartKqpTasksResponse>();
        ev->Record.SetTxId(txId);
        for (auto& task : request.GetTasks()) {
            auto* resp = ev->Record.AddNotStartedTasks();
            resp->SetTaskId(task.GetId());
            resp->SetReason(reason);
            resp->SetMessage(message);
        }
        Send(executer, ev.Release());
    }

    NRm::IKqpResourceManager* ResourceManager() {
        if (Y_LIKELY(ResourceManager_)) {
            return ResourceManager_;
        }
        ResourceManager_ = GetKqpResourceManager();
        return ResourceManager_;
    }

private:
    NKikimrConfig::TTableServiceConfig::TResourceManager Config;
    TIntrusivePtr<TKqpCounters> Counters;
    IKqpNodeComputeActorFactory* CaFactory;
    NRm::IKqpResourceManager* ResourceManager_ = nullptr;
    NKqpNode::TState State;
};


} // anonymous namespace

IActor* CreateKqpNodeService(const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    TIntrusivePtr<TKqpCounters> counters, IKqpNodeComputeActorFactory* caFactory)
{
    return new TKqpNodeService(tableServiceConfig, counters, caFactory);
}

} // namespace NKqp
} // namespace NKikimr
