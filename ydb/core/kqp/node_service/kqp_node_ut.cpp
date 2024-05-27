#include <ydb/core/base/statestorage.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_actor.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/tablet/resource_broker_impl.h>

#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#ifndef NDEBUG
const bool DETAILED_LOG = true;
#else
const bool DETAILED_LOG = false;
#endif

namespace NKikimr {
namespace NKqp {

using namespace NKikimrResourceBroker;
using namespace NResourceBroker;

namespace {

TTenantTestConfig MakeTenantTestConfig() {
    TTenantTestConfig cfg = {
        // Domains {name, schemeshard {{ subdomain_names }}}
        {{{DOMAIN1_NAME, SCHEME_SHARD1_ID, {{TENANT1_1_NAME, TENANT1_2_NAME}}}}},
        // HiveId
        HIVE_ID,
        // FakeTenantSlotBroker
        true,
        // FakeSchemeShard
        true,
        // CreateConsole
        false,
        // Nodes
        {{
             // Node0
             {
                 // TenantPoolConfig
                 {
                     // Static slots {tenant, {cpu, memory, network}}
                     {{{DOMAIN1_NAME, {1, 1, 1}}}},
                     "node-type"
                 }
             }
         }},
        // DataCenterCount
        1
    };
    return cfg;
}

TResourceBrokerConfig MakeResourceBrokerTestConfig() {
    TResourceBrokerConfig config;

    auto queue = config.AddQueues();
    queue->SetName("queue_default");
    queue->SetWeight(5);
    queue->MutableLimit()->AddResource(4);

    queue = config.AddQueues();
    queue->SetName("queue_kqp_resource_manager");
    queue->SetWeight(20);
    queue->MutableLimit()->AddResource(4);
    queue->MutableLimit()->AddResource(50'000);

    auto task = config.AddTasks();
    task->SetName("unknown");
    task->SetQueueName("queue_default");
    task->SetDefaultDuration(TDuration::Seconds(5).GetValue());

    task = config.AddTasks();
    task->SetName(NLocalDb::KqpResourceManagerTaskName);
    task->SetQueueName("queue_kqp_resource_manager");
    task->SetDefaultDuration(TDuration::Seconds(5).GetValue());

    config.MutableResourceLimit()->AddResource(10);
    config.MutableResourceLimit()->AddResource(100'000);

    return config;
}

NKikimrConfig::TTableServiceConfig MakeKqpResourceManagerConfig() {
    NKikimrConfig::TTableServiceConfig config;

    config.MutableResourceManager()->SetComputeActorsCount(100);
    config.MutableResourceManager()->SetChannelBufferSize(10);
    config.MutableResourceManager()->SetMinChannelBufferSize(10);
    config.MutableResourceManager()->SetMkqlLightProgramMemoryLimit(1'000);
    config.MutableResourceManager()->SetMkqlHeavyProgramMemoryLimit(10'000);
    config.MutableResourceManager()->SetQueryMemoryLimit(30'000);
    config.MutableResourceManager()->SetPublishStatisticsIntervalSec(0);
    config.MutableResourceManager()->SetEnableInstantMkqlMemoryAlloc(true);

    auto* infoExchangerRetrySettings = config.MutableResourceManager()->MutableInfoExchangerSettings();
    auto* exchangerSettings = infoExchangerRetrySettings->MutableExchangerSettings();
    exchangerSettings->SetStartDelayMs(10);
    exchangerSettings->SetMaxDelayMs(10);

    return config;
}

}

struct TMockComputeActor {
    TActorId ActorId;
    TActorId ExecuterId;
    ui64 TxId;
    NYql::NDqProto::TDqTask Task;
    NYql::NDq::TComputeRuntimeSettings Settings;
    NYql::NDq::TComputeMemoryLimits MemoryLimits;
};

struct TMockKqpComputeActorFactory : public IKqpNodeComputeActorFactory {
    TTestBasicRuntime& Runtime;
    TMap<ui64, TMockComputeActor> Task2Actor;

    TMockKqpComputeActorFactory(TTestBasicRuntime& runtime)
        : Runtime(runtime) {}

    IActor* CreateKqpComputeActor(const TActorId& executerId, ui64 txId, NYql::NDqProto::TDqTask* task,
        const NYql::NDq::TComputeRuntimeSettings& settings, const NYql::NDq::TComputeMemoryLimits& memoryLimits,
        NWilson::TTraceId, TIntrusivePtr<NActors::TProtoArenaHolder>) override
    {
        auto actorId = Runtime.AllocateEdgeActor();
        auto& mock = Task2Actor[task->GetId()];
        mock.ActorId = actorId;
        mock.ExecuterId = executerId;
        mock.TxId = txId;
        mock.Task.Swap(task);
        mock.Settings = settings;
        mock.MemoryLimits = memoryLimits;
        UNIT_ASSERT(mock.MemoryLimits.MemoryQuotaManager->AllocateQuota(mock.MemoryLimits.MkqlLightProgramMemoryLimit));
        static_cast<NYql::NDq::TGuaranteeQuotaManager*>(mock.MemoryLimits.MemoryQuotaManager.get())->Step = 1;
        return Runtime.FindActor(actorId);
    }
};

class KqpNode : public TTestBase {
public:

    ~KqpNode() override {
        CompFactory.Reset();
    }

    void SetUp() override {
        Runtime.Reset(new TTenantTestRuntime(MakeTenantTestConfig()));

        NActors::NLog::EPriority priority = DETAILED_LOG ? NLog::PRI_DEBUG : NLog::PRI_ERROR;
        Runtime->SetLogPriority(NKikimrServices::RESOURCE_BROKER, priority);
        Runtime->SetLogPriority(NKikimrServices::KQP_RESOURCE_MANAGER, priority);
        Runtime->SetLogPriority(NKikimrServices::KQP_NODE, priority);

        auto now = Now();
        Runtime->UpdateCurrentTime(now);

        Counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        KqpCounters = MakeIntrusive<TKqpCounters>(Counters);

        auto resourceBrokerConfig = MakeResourceBrokerTestConfig();
        auto broker = CreateResourceBrokerActor(resourceBrokerConfig, Counters);
        ResourceBrokerActorId = Runtime->Register(broker);
        WaitForBootstrap();

        CompFactory.Reset(new TMockKqpComputeActorFactory(*Runtime));
    }

    void WaitForBootstrap() {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        UNIT_ASSERT(Runtime->DispatchEvents(options));
    }

    IActor* CreateKqpNode(const NKikimrConfig::TTableServiceConfig& config = {}) {
        auto rm = CreateKqpResourceManagerActor(config.GetResourceManager(), KqpCounters, ResourceBrokerActorId);
        ResourceManagerActorId = Runtime->Register(rm);
        Runtime->EnableScheduleForActor(ResourceManagerActorId, true);
        WaitForBootstrap();

        auto FederatedQuerySetup = std::make_optional<TKqpFederatedQuerySetup>({NYql::IHTTPGateway::Make(), nullptr, nullptr, nullptr, {}, {}, {}, nullptr, nullptr});
        auto asyncIoFactory = CreateKqpAsyncIoFactory(KqpCounters, FederatedQuerySetup);
        auto kqpNode = CreateKqpNodeService(config, KqpCounters, CompFactory.Get(), asyncIoFactory);
        KqpNodeActorId = Runtime->Register(kqpNode);
        Runtime->EnableScheduleForActor(KqpNodeActorId, true);
        WaitForBootstrap();
        return kqpNode;
    }

    void SendStartTasksRequest(const TActorId& requester, ui64 txId, const TVector<ui64>& taskIds,
        const TActorId& executer = {})
    {
        auto ev = MakeHolder<TEvKqpNode::TEvStartKqpTasksRequest>();
        ev->Record.SetTxId(txId);
        ActorIdToProto(executer ? executer : requester, ev->Record.MutableExecuterActorId());
        ev->Record.SetStartAllOrFail(true);
        ev->Record.MutableRuntimeSettings()->SetExecType(NYql::NDqProto::TComputeRuntimeSettings::SCAN);

        for (ui64 taskId : taskIds) {
            auto* task = ev->Record.AddTasks();
            task->SetId(taskId);
            task->AddInputs()->AddChannels();
            task->AddOutputs()->AddChannels();
        }

        Runtime->Send(new IEventHandle(KqpNodeActorId, requester, ev.Release()));
    }

    void SendFinishTask(const TActorId& computeActorId, ui64 txId, ui64 taskId, bool success = true,
        const TString& message = "")
    {
        auto ev = new TEvKqpNode::TEvFinishKqpTask(txId, taskId, success, NYql::TIssues({NYql::TIssue(message)}));
        Runtime->Send(new IEventHandle(KqpNodeActorId, computeActorId, ev));
    }

    void DispatchKqpNodePostponedEvents(const TActorId& edge) {
        auto ev = new NConsole::TEvConsole::TEvConfigNotificationRequest;
        Runtime->Send(new IEventHandle(KqpNodeActorId, edge, ev), 0, true);
        Runtime->GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(edge);
    }

    void AssertResourceBrokerSensors(i64 cpu, i64 mem, i64 enqueued, i64 finished, i64 infly) {
        auto q = Counters->GetSubgroup("queue", "queue_kqp_resource_manager");
//        Cerr << "-- queue_kqp_resource_manager\n";
//        q->OutputPlainText(Cerr, " ");
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("CPUConsumption")->Val(), cpu);
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("MemoryConsumption")->Val(), mem);
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("EnqueuedTasks")->Val(), enqueued);
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("FinishedTasks")->Val(), finished);
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("InFlyTasks")->Val(), infly);

        auto t = Counters->GetSubgroup("task", "kqp_query");
//        Cerr << "-- kqp_query\n";
//        t->OutputPlainText(Cerr, " ");
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("CPUConsumption")->Val(), cpu);
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("MemoryConsumption")->Val(), mem);
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("EnqueuedTasks")->Val(), enqueued);
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("FinishedTasks")->Val(), finished);
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("InFlyTasks")->Val(), infly);
    }


    UNIT_TEST_SUITE(KqpNode);
        UNIT_TEST(CommonCase);
        UNIT_TEST(ExtraAllocation);
        UNIT_TEST(NotEnoughMemory);
        UNIT_TEST(NotEnoughMemory_Extra);
        UNIT_TEST(NotEnoughComputeActors);
        UNIT_TEST(ResourceBrokerNotEnoughResources);
        UNIT_TEST(ResourceBrokerNotEnoughResources_Extra);
        UNIT_TEST(ExecuterLost);
        UNIT_TEST(TerminateTx);
    UNIT_TEST_SUITE_END();

    void CommonCase();
    void ExtraAllocation();
    void NotEnoughMemory();
    void NotEnoughMemory_Extra();
    void NotEnoughComputeActors();
    void ResourceBrokerNotEnoughResources();
    void ResourceBrokerNotEnoughResources_Extra();
    void ExecuterLost();
    void TerminateTx();

private:
    THolder<TTestBasicRuntime> Runtime;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TIntrusivePtr<TKqpCounters> KqpCounters;
    THolder<TMockKqpComputeActorFactory> CompFactory;
    TActorId ResourceBrokerActorId;
    TActorId ResourceManagerActorId;
    TActorId KqpNodeActorId;
};
UNIT_TEST_SUITE_REGISTRATION(KqpNode);

void KqpNode::CommonCase() {
    auto cfg = MakeKqpResourceManagerConfig();
    CreateKqpNode(cfg);

    TActorId sender1 = Runtime->AllocateEdgeActor();
    TActorId sender2 = Runtime->AllocateEdgeActor();

    const ui64 additionalSize = 2 * 10;
    const ui64 fullMkqlLimit = 1'000;
//    const ui64 taskSize = fullMkqlLimit + additionalSize;
    const ui64 tasksSize12 = fullMkqlLimit + 2 * additionalSize;
    const ui64 tasksSize22 = 2 * tasksSize12; //for 2 requests

    // first request
    SendStartTasksRequest(sender1, /* txId */ 1, /* taskIds */ {1, 2});
    {
        auto answer = Runtime->GrabEdgeEvent<TEvKqpNode::TEvStartKqpTasksResponse>(sender1);
        auto& record = answer->Get()->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, record.GetTxId());
        UNIT_ASSERT_VALUES_EQUAL(2, record.GetStartedTasks().size());
        UNIT_ASSERT_VALUES_EQUAL(0, record.GetNotStartedTasks().size());
        UNIT_ASSERT_VALUES_EQUAL(2, CompFactory->Task2Actor.size());

        UNIT_ASSERT(CompFactory->Task2Actor.contains(1));
        UNIT_ASSERT(CompFactory->Task2Actor.contains(2));

        auto& memoryLimits = CompFactory->Task2Actor.begin()->second.MemoryLimits;
        UNIT_ASSERT_VALUES_EQUAL(10, memoryLimits.ChannelBufferSize);
        UNIT_ASSERT_VALUES_EQUAL(1'000, memoryLimits.MkqlLightProgramMemoryLimit);
        UNIT_ASSERT_VALUES_EQUAL(10'000, memoryLimits.MkqlHeavyProgramMemoryLimit);

        UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmComputeActors->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmMemory->Val(), tasksSize12);

        AssertResourceBrokerSensors(0, tasksSize12, 0, 0, 2);
    }

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    {
        TVector<NKikimrKqp::TKqpNodeResources> snapshot;
        std::atomic<int> ready = 0;
        GetKqpResourceManager(ResourceManagerActorId.NodeId())->RequestClusterResourcesInfo(
            [&](TVector<NKikimrKqp::TKqpNodeResources>&& resources) {
                snapshot = std::move(resources);
                ready = 1;
            });

        while (ready.load() != 1) {
            Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
        }

        UNIT_ASSERT_VALUES_EQUAL(1, snapshot.size());

        NKikimrKqp::TKqpNodeResources& payload = snapshot[0];
        UNIT_ASSERT_VALUES_EQUAL(1, payload.GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(98, payload.GetExecutionUnits());
        UNIT_ASSERT_VALUES_EQUAL(1, payload.GetMemory().size());
        UNIT_ASSERT_VALUES_EQUAL((ui32) NRm::EKqpMemoryPool::ScanQuery, payload.GetMemory()[0].GetPool());
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetResourceManager().GetQueryMemoryLimit() - tasksSize12,
                                 payload.GetMemory()[0].GetAvailable());
    }

    // attempt to request resources for the same txId/requester
    SendStartTasksRequest(sender1, /* txId */ 1, /* taskIds */ {3, 4});
    {
        auto answer = Runtime->GrabEdgeEvent<TEvKqpNode::TEvStartKqpTasksResponse>(sender1);
        auto& record = answer->Get()->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, record.GetTxId());
        UNIT_ASSERT_EQUAL(2, record.GetNotStartedTasks().size());
        for (auto& notStartedTask : record.GetNotStartedTasks()) {
            UNIT_ASSERT_EQUAL(NKikimrKqp::TEvStartKqpTasksResponse::INTERNAL_ERROR, notStartedTask.GetReason());
        }

        AssertResourceBrokerSensors(0, tasksSize12, 0, 0, 2);
    }

    // second request
    SendStartTasksRequest(sender2, /* txId */ 2, /* taskIds */ {3, 4});
    {
        auto answer = Runtime->GrabEdgeEvent<TEvKqpNode::TEvStartKqpTasksResponse>(sender2);
        auto& record = answer->Get()->Record;

        UNIT_ASSERT_VALUES_EQUAL(2, record.GetTxId());
        UNIT_ASSERT_VALUES_EQUAL(2, record.GetStartedTasks().size());
        UNIT_ASSERT_VALUES_EQUAL(0, record.GetNotStartedTasks().size());
        UNIT_ASSERT_VALUES_EQUAL(4, CompFactory->Task2Actor.size());

        UNIT_ASSERT(CompFactory->Task2Actor.contains(1));
        UNIT_ASSERT(CompFactory->Task2Actor.contains(2));
        UNIT_ASSERT(CompFactory->Task2Actor.contains(3));
        UNIT_ASSERT(CompFactory->Task2Actor.contains(4));

        UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmComputeActors->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmMemory->Val(), tasksSize22);

        AssertResourceBrokerSensors(0, tasksSize22, 0, 0, 4);
    }

    // request extra resources for taskId 4
    {
        NKikimr::TActorSystemStub stub;

        bool allocated = CompFactory->Task2Actor[4].MemoryLimits.MemoryQuotaManager->AllocateQuota(100);
        UNIT_ASSERT(allocated);
        DispatchKqpNodePostponedEvents(sender1);
        UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmComputeActors->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmMemory->Val(), tasksSize22 + 100);
        AssertResourceBrokerSensors(0, tasksSize22 + 100, 0, 1, 4);
    }

    // complete tasks
    for (ui64 taskId : {1, 2, 3, 4}) {
        ui64 extraMem = taskId < 4 ? 100 : 0;

        auto mockCA = CompFactory->Task2Actor[taskId];
        CompFactory->Task2Actor.erase(taskId);

        SendFinishTask(mockCA.ActorId, taskId < 3 ? 1 : 2, taskId);
        {
            UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmComputeActors->Val(), (i64) CompFactory->Task2Actor.size());
            const ui64 tasksMemMkql = fullMkqlLimit * (4 - taskId) / 2;
            UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmMemory->Val(), (i64) (4 - taskId) * additionalSize + extraMem + tasksMemMkql);

            AssertResourceBrokerSensors(0, (4 - taskId) * additionalSize + extraMem + tasksMemMkql, 0, 1 + taskId, 4 - taskId);
        }
    }

    AssertResourceBrokerSensors(0, 0, 0, 5, 0);

    {
        NKikimr::TActorSystemStub stub;

        CompFactory->Task2Actor.clear();
    }
}

void KqpNode::ExtraAllocation() {
    auto cfg = MakeKqpResourceManagerConfig();
    cfg.MutableResourceManager()->SetQueryMemoryLimit(100'000);
    CreateKqpNode(cfg);

    TActorId sender1 = Runtime->AllocateEdgeActor();

    const ui64 mkqlLimit = 1'000;
    const ui64 additionalSize = 2 * 10;
    const ui64 taskSize2 = mkqlLimit + additionalSize * 2;

    SendStartTasksRequest(sender1, /* txId */ 1, /* taskIds */ {1, 2});
    Runtime->GrabEdgeEvent<TEvKqpNode::TEvStartKqpTasksResponse>(sender1);

    // memory granted
    {
        NKikimr::TActorSystemStub stub;

        bool allocated = CompFactory->Task2Actor[1].MemoryLimits.MemoryQuotaManager->AllocateQuota(100);
        UNIT_ASSERT(allocated);
        DispatchKqpNodePostponedEvents(sender1);

        UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmComputeActors->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmMemory->Val(), taskSize2 + 100);
        AssertResourceBrokerSensors(0, taskSize2 + 100, 0, 1, 2);
    }

    // too big request
    {
        NKikimr::TActorSystemStub stub;

        bool allocated = CompFactory->Task2Actor[1].MemoryLimits.MemoryQuotaManager->AllocateQuota(50'000);
        UNIT_ASSERT(!allocated);
        DispatchKqpNodePostponedEvents(sender1);

        UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmComputeActors->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmMemory->Val(), taskSize2 + 100);
        UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmNotEnoughMemory->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmNotEnoughComputeActors->Val(), 0);
        AssertResourceBrokerSensors(0, taskSize2 + 100, 0, 1, 2);
    }

    {
        NKikimr::TActorSystemStub stub;

        CompFactory->Task2Actor.clear();
    }
}

void KqpNode::NotEnoughMemory() {
    auto cfg = MakeKqpResourceManagerConfig();
    cfg.MutableResourceManager()->SetChannelBufferSize(100'000);
    CreateKqpNode(cfg);

    TActorId sender1 = Runtime->AllocateEdgeActor();

    SendStartTasksRequest(sender1, 1, {1});
    {
        auto answer = Runtime->GrabEdgeEvent<TEvKqpNode::TEvStartKqpTasksResponse>(sender1);
        auto& record = answer->Get()->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, record.GetTxId());
        UNIT_ASSERT_VALUES_EQUAL(1, record.GetNotStartedTasks().size());
        auto& task = record.GetNotStartedTasks()[0];
        UNIT_ASSERT_EQUAL(NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY, task.GetReason());
        UNIT_ASSERT_STRING_CONTAINS(task.GetMessage(), "Not enough memory for query, requested: 201000");
    }

    AssertResourceBrokerSensors(0, 0, 0, 0, 0);
}

void KqpNode::NotEnoughMemory_Extra() {
    CreateKqpNode(MakeKqpResourceManagerConfig());

    TActorId sender1 = Runtime->AllocateEdgeActor();

    const ui64 mkqlLimit = 1'000;
    const ui64 additionalSize = 2 * 10;
    const ui64 taskSize2 = mkqlLimit + additionalSize * 2;

    // first request
    SendStartTasksRequest(sender1, /* txId */ (ui64)1, /* taskIds */ {1, 2});
    {
        auto answer = Runtime->GrabEdgeEvent<TEvKqpNode::TEvStartKqpTasksResponse>(sender1);
        auto& record = answer->Get()->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, record.GetTxId());
        UNIT_ASSERT_VALUES_EQUAL(0, record.GetNotStartedTasks().size());
        UNIT_ASSERT_VALUES_EQUAL(2, record.GetStartedTasks().size());
        UNIT_ASSERT_VALUES_EQUAL(2, CompFactory->Task2Actor.size());

        UNIT_ASSERT(CompFactory->Task2Actor.contains(1));
        UNIT_ASSERT(CompFactory->Task2Actor.contains(2));

        auto& memoryLimits = CompFactory->Task2Actor.begin()->second.MemoryLimits;
        UNIT_ASSERT_VALUES_EQUAL(10, memoryLimits.ChannelBufferSize);
        UNIT_ASSERT_VALUES_EQUAL(1'000, memoryLimits.MkqlLightProgramMemoryLimit);
        UNIT_ASSERT_VALUES_EQUAL(10'000, memoryLimits.MkqlHeavyProgramMemoryLimit);

        UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmComputeActors->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmMemory->Val(), taskSize2);

        AssertResourceBrokerSensors(0, taskSize2, 0, 0, 2);
    }

    {
        NKikimr::TActorSystemStub stub;

        bool allocated = CompFactory->Task2Actor[1].MemoryLimits.MemoryQuotaManager->AllocateQuota(1'000'000);
        UNIT_ASSERT(!allocated);
    }

    DispatchKqpNodePostponedEvents(sender1);

    UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmComputeActors->Val(), 2);
    UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmMemory->Val(), taskSize2);
    UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmNotEnoughMemory->Val(), 1);
    UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmNotEnoughComputeActors->Val(), 0);

    AssertResourceBrokerSensors(0, taskSize2, 0, 0, 2);

    {
        NKikimr::TActorSystemStub stub;

        CompFactory->Task2Actor.clear();
    }
}

void KqpNode::NotEnoughComputeActors() {
    auto cfg = MakeKqpResourceManagerConfig();
    cfg.MutableResourceManager()->SetComputeActorsCount(4);
    CreateKqpNode(cfg);

    TActorId sender1 = Runtime->AllocateEdgeActor();

    SendStartTasksRequest(sender1, /* txId */ (ui64)1, /* taskIds */ {1, 2, 3, 4, 5});
    {
        auto answer = Runtime->GrabEdgeEvent<TEvKqpNode::TEvStartKqpTasksResponse>(sender1);
        auto& record = answer->Get()->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, record.GetTxId());
        UNIT_ASSERT_VALUES_EQUAL(5, record.GetNotStartedTasks().size());
        for (auto& task : record.GetNotStartedTasks()) {
            UNIT_ASSERT_EQUAL(NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_EXECUTION_UNITS, task.GetReason());
        }
    }

    AssertResourceBrokerSensors(0, 0, 0, 4, 0);

    {
        NKikimr::TActorSystemStub stub;

        CompFactory->Task2Actor.clear();
    }
}

void KqpNode::ResourceBrokerNotEnoughResources() {
    auto cfg = MakeKqpResourceManagerConfig();
    cfg.MutableResourceManager()->SetChannelBufferSize(6'000);
    cfg.MutableResourceManager()->SetQueryMemoryLimit(49'000);
    CreateKqpNode(cfg);

    TActorId sender1 = Runtime->AllocateEdgeActor();
    TActorId sender2 = Runtime->AllocateEdgeActor();

    SendStartTasksRequest(sender1, 1, {1, 2});
    {
        auto answer = Runtime->GrabEdgeEvent<TEvKqpNode::TEvStartKqpTasksResponse>(sender1);
        Y_UNUSED(answer);
        AssertResourceBrokerSensors(0, 2 * (6000 * 2 + 1000 / 2), 0, 0, 2);
    }

    SendStartTasksRequest(sender2, 2, {3, 4});
    {
        auto answer = Runtime->GrabEdgeEvent<TEvKqpNode::TEvStartKqpTasksResponse>(sender2);
        auto& record = answer->Get()->Record;

        UNIT_ASSERT_VALUES_EQUAL(2, record.GetTxId());
        UNIT_ASSERT_VALUES_EQUAL(2, record.GetNotStartedTasks().size());
        for (auto& task : record.GetNotStartedTasks()) {
            Cerr << TEvStartKqpTasksResponse_ENotStartedTaskReason_Name(task.GetReason()) << Endl;
            UNIT_ASSERT_EQUAL(NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY, task.GetReason());
        }
    }

    AssertResourceBrokerSensors(0, 2 * (6000 * 2 + 1000 / 2), 0, 1, 2);

    {
        NKikimr::TActorSystemStub stub;

        CompFactory->Task2Actor.clear();
    }
}

void KqpNode::ResourceBrokerNotEnoughResources_Extra() {
    auto cfg = MakeKqpResourceManagerConfig();
    cfg.MutableResourceManager()->SetChannelBufferSize(6'000);
    cfg.MutableResourceManager()->SetQueryMemoryLimit(49'000);
    CreateKqpNode(cfg);

    TActorId sender1 = Runtime->AllocateEdgeActor();

    SendStartTasksRequest(sender1, 1, {1, 2});
    {
        auto answer = Runtime->GrabEdgeEvent<TEvKqpNode::TEvStartKqpTasksResponse>(sender1);
        Y_UNUSED(answer);
        AssertResourceBrokerSensors(0, 2 * (6000 * 2 + 1000 / 2), 0, 0, 2);
    }

    {
        NKikimr::TActorSystemStub stub;

        bool allocated = CompFactory->Task2Actor[1].MemoryLimits.MemoryQuotaManager->AllocateQuota(2 * (6000 * 2 + 1000 / 2));
        UNIT_ASSERT(!allocated);
    }

    AssertResourceBrokerSensors(0, 2 * (6000 * 2 + 1000 / 2), 0, 0, 2);

    {
        NKikimr::TActorSystemStub stub;

        CompFactory->Task2Actor.clear();
    }
}

void KqpNode::ExecuterLost() {
    CreateKqpNode(MakeKqpResourceManagerConfig());

    TActorId sender1 = Runtime->AllocateEdgeActor();
    SendStartTasksRequest(sender1, 1, {1, 2});

    Runtime->GrabEdgeEvent<TEvKqpNode::TEvStartKqpTasksResponse>(sender1);

    {
        NKikimr::TActorSystemStub stub;

        bool allocated = CompFactory->Task2Actor[1].MemoryLimits.MemoryQuotaManager->AllocateQuota(100);
        UNIT_ASSERT(allocated);
        DispatchKqpNodePostponedEvents(sender1);
    }

    Runtime->Send(new IEventHandle(KqpNodeActorId, {},
                                   new TEvents::TEvUndelivered(
                                       TEvKqpNode::TEvStartKqpTasksResponse::EventType,
                                       TEvents::TEvUndelivered::EReason::ReasonActorUnknown),
                                   0, 1));

    for (auto& [taskId, computeActor] : CompFactory->Task2Actor) {
        auto abortEvent = Runtime->GrabEdgeEvent<TEvKqp::TEvAbortExecution>(computeActor.ActorId);
        UNIT_ASSERT_VALUES_EQUAL("executer lost: 1", abortEvent->Get()->Record.GetLegacyMessage());
    }

    {
        NKikimr::TActorSystemStub stub;
        TMap<ui64, TMockComputeActor> Task2ActorEmpty;
        CompFactory->Task2Actor.swap(Task2ActorEmpty);
        Task2ActorEmpty.clear();
    }

    size_t iterations = 30;
    while (KqpCounters->RmComputeActors->Val() != 0 && iterations > 0) {
        Sleep(TDuration::MilliSeconds(300));
        iterations--;
        Cerr << "waiting compute actors to complete" << Endl;
    }

    UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmComputeActors->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmMemory->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmNotEnoughMemory->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmNotEnoughComputeActors->Val(), 0);

    AssertResourceBrokerSensors(0, 0, 0, 3, 0);

    {
        NKikimr::TActorSystemStub stub;

        CompFactory->Task2Actor.clear();
    }
}

void KqpNode::TerminateTx() {
    CreateKqpNode(MakeKqpResourceManagerConfig());

    TActorId executer = Runtime->AllocateEdgeActor();
    SendStartTasksRequest(executer, 1, {1});

    TActorId sender1 = Runtime->AllocateEdgeActor();
    SendStartTasksRequest(sender1, 1, {2}, executer);

    TActorId sender2 = Runtime->AllocateEdgeActor();
    SendStartTasksRequest(sender2, 1, {3}, executer);

    {
        NKikimr::TActorSystemStub stub;

        bool allocated = CompFactory->Task2Actor[1].MemoryLimits.MemoryQuotaManager->AllocateQuota(100);
        UNIT_ASSERT(allocated);
        DispatchKqpNodePostponedEvents(sender1);
    }

    const ui64 taskSize = 1'000 + 2 * 10;

    UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmComputeActors->Val(), 3);
    UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmMemory->Val(), 3 * taskSize + 100);
    UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmNotEnoughMemory->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmNotEnoughComputeActors->Val(), 0);

    AssertResourceBrokerSensors(0, 3 * taskSize + 100, 0, 1, 3);

    {
        // terminate tx
        auto cancelEvent = MakeHolder<TEvKqpNode::TEvCancelKqpTasksRequest>();
        cancelEvent->Record.SetTxId(1);
        cancelEvent->Record.SetReason("terminate");
        Runtime->Send(new IEventHandle(KqpNodeActorId, executer, cancelEvent.Release()));

        for (auto&[taskId, computeActor] : CompFactory->Task2Actor) {
            auto abortEvent = Runtime->GrabEdgeEvent<TEvKqp::TEvAbortExecution>(computeActor.ActorId);
            UNIT_ASSERT_VALUES_EQUAL("terminate", abortEvent->Get()->Record.GetLegacyMessage());
        }

        {
            NKikimr::TActorSystemStub stub;
            TMap<ui64, TMockComputeActor> Task2ActorEmpty;
            CompFactory->Task2Actor.swap(Task2ActorEmpty);
            Task2ActorEmpty.clear();
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmComputeActors->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmMemory->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmNotEnoughMemory->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(KqpCounters->RmNotEnoughComputeActors->Val(), 0);

    AssertResourceBrokerSensors(0, 0, 0, 4, 0);

    {
        NKikimr::TActorSystemStub stub;

        CompFactory->Task2Actor.clear();
    }
}

} // namespace NKqp
} // namespace NKikimr
