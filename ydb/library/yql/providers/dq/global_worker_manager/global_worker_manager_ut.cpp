#include "global_worker_manager.h"
#include <ydb/library/yql/providers/dq/worker_manager/local_worker_manager.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/yson/node/node_io.h>
#include <yt/cpp/mapreduce/interface/fluent.h>
#include <ydb/library/yql/providers/common/metrics/metrics_registry.h>
#include <ydb/library/yql/providers/dq/actors/events/events.h>
#include <ydb/library/yql/providers/dq/common/attrs.h>
#include <ydb/library/yql/providers/dq/actors/dynamic_nameserver.h>
#include <ydb/library/yql/providers/dq/actors/resource_allocator.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/dq/integration/transform/yql_dq_task_transform.h>
#include <ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/dq/transform/yql_common_dq_transform.h>
#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_local.h>

using namespace NYql;
using namespace NActors;
using namespace NProto;
using namespace NDqs;

namespace {

class TMockLock: public TActor<TMockLock> {
public:
    TMockLock()
        : TActor(&TMockLock::Main)
    {
    }

    STATEFN(Main) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }
};

class TTestCoordinationHelper: public ICoordinationHelper {
public:

    TTestCoordinationHelper(ui32 nodeId)
    : NodeId(nodeId)
    {}

    ui32 GetNodeId() override {
        return NodeId;
    }

    ui32 GetNodeId(const TMaybe<ui32> nodeId, const TMaybe<TString>& grpcPort, ui32 minNodeId, ui32 maxNodeId, const THashMap<TString, TString>& attributes) override {
        Y_UNUSED(nodeId);
        Y_UNUSED(grpcPort);
        Y_UNUSED(minNodeId);
        Y_UNUSED(maxNodeId);
        Y_UNUSED(attributes);
        return NodeId;
    }

    TString GetHostname() override {
        return "localhost";
    }

    TString GetIp() override {
        return "::1";
    }

    NActors::IActor* CreateLockOnCluster(NActors::TActorId ytWrapper, const TString& prefix, const TString& lockName, bool temporary) override {
        Y_UNUSED(ytWrapper);
        Y_UNUSED(prefix);
        Y_UNUSED(lockName);
        Y_UNUSED(temporary);

        return new TMockLock();
    }

    NActors::IActor* CreateLock(const TString& lockName, bool temporary) override {
        Y_UNUSED(lockName);
        Y_UNUSED(temporary);
        return new TMockLock();
    }

    NActors::IActor* CreateServiceNodePinger(const IServiceNodeResolver::TPtr& ptr, const TResourceManagerOptions& rmOptions, const THashMap<TString, TString>& attributes = {}) override {
        Y_UNUSED(ptr);
        Y_UNUSED(rmOptions);
        Y_UNUSED(attributes);
        return nullptr;
    }

    void StartRegistrator(NActors::TActorSystem* actorSystem) override {
        Y_UNUSED(actorSystem);
    }

    void StartGlobalWorker(NActors::TActorSystem* actorSystem, const TVector<TResourceManagerOptions>& resourceUploaderOptions, IMetricsRegistryPtr metricsRegistry) override {
        Y_UNUSED(actorSystem);
        Y_UNUSED(resourceUploaderOptions);
        Y_UNUSED(metricsRegistry);
    }

    void StartCleaner(NActors::TActorSystem* actorSystem, const TMaybe<TString>& role) override {
        Y_UNUSED(actorSystem);
        Y_UNUSED(role);
    }

    NYql::IServiceNodeResolver::TPtr CreateServiceNodeResolver(NActors::TActorSystem* actorSystem, const TVector<TString>& hostPortPairs) override {
        Y_UNUSED(actorSystem);
        Y_UNUSED(hostPortPairs);

        return NYql::IServiceNodeResolver::TPtr();
    }

    const NProto::TDqConfig::TYtCoordinator& GetConfig() override {
        return YtCoordinator_;
    }

    const NActors::TActorId GetWrapper(NActors::TActorSystem* actorSystem) override {
        Y_UNUSED(actorSystem);
        return NActors::TActorId();
    }

    const NActors::TActorId GetWrapper(NActors::TActorSystem* actorSystem, const TString& cluster, const TString& user, const TString& token) override {
        Y_UNUSED(actorSystem);
        Y_UNUSED(cluster);
        Y_UNUSED(user);
        Y_UNUSED(token);
        return NActors::TActorId();
    }

    const NActors::TActorId GetWrapper() override {
        return NActors::TActorId();
    }

    TWorkerRuntimeData* GetRuntimeData() override {
        return nullptr;
    }

    void Stop(NActors::TActorSystem* actorSystem) override {
        Y_UNUSED(actorSystem);
    }

    TString GetRevision() override {
        return TString();
    }

private:
    ui32 NodeId;
    NProto::TDqConfig::TYtCoordinator YtCoordinator_;
};
}

class TGlobalWorkerManagerTest: public TTestBase {
public:
    UNIT_TEST_SUITE(TGlobalWorkerManagerTest);
    UNIT_TEST(TestTasksAllocation1)
    UNIT_TEST(TestTasksAllocation2)
    UNIT_TEST(TestTasksAllocation3)
    UNIT_TEST(TestTasksAllocation4)
    UNIT_TEST(TestTasksAllocation5)
    UNIT_TEST_SUITE_END();

    void SetUp() override {
        // Service node: 0
        // Worker nodes: 1..nodesNumber
        const ui32 nodesNumber = 7;
        ActorRuntime_.Reset(new NActors::TTestActorRuntimeBase(nodesNumber, true));

        // Name server.
        TIntrusivePtr<TTableNameserverSetup> nameserverTable = new TTableNameserverSetup();
        THashSet<ui32> staticNodeId;

        for (ui32 i = 0; i < 100; i++) {
            nameserverTable->StaticNodeTable[i] = std::make_pair(ToString(i), i);
        }
        ActorRuntime_->AddLocalService(GetNameserviceActorId(),
            TActorSetupCmd(NYql::NDqs::CreateDynamicNameserver(nameserverTable), TMailboxType::Simple, 0));

        // GWM
        TVector<TResourceManagerOptions> uploadResourcesOptions;
        const TIntrusivePtr<TTestCoordinationHelper> coordPtr = new TTestCoordinationHelper(NodeId());
        const TIntrusivePtr<TSensorsGroup> sensorsPtr = new TSensorsGroup();
        const auto gwmActor = CreateGlobalWorkerManager(coordPtr, uploadResourcesOptions, CreateMetricsRegistry(sensorsPtr), NProto::TDqConfig::TScheduler());
        ActorRuntime_->AddLocalService(MakeWorkerManagerActorID(NodeId()),
            TActorSetupCmd{gwmActor, TMailboxType::Simple, 0});

        // Local WM.
        FunctionRegistry_ = CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        auto dqCompFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory({
            NYql::GetCommonDqFactory(), 
            NKikimr::NMiniKQL::GetYqlFactory()
        });
    
        auto dqTaskTransformFactory = NYql::CreateCompositeTaskTransformFactory({
            NYql::CreateCommonDqTaskTransformFactory()
        });

        auto patternCache = std::make_shared<NKikimr::NMiniKQL::TComputationPatternLRUCache>(NKikimr::NMiniKQL::TComputationPatternLRUCache::Config(200_MB, 200_MB));

        auto factory = NTaskRunnerProxy::CreateFactory(FunctionRegistry_.Get(), dqCompFactory, dqTaskTransformFactory, patternCache, true);
        for (ui32 i = 1; i < nodesNumber; i++) {
            NYql::NDqs::TLocalWorkerManagerOptions lwmOptions;
            lwmOptions.TaskRunnerInvokerFactory = new NDqs::TTaskRunnerInvokerFactory();
            lwmOptions.TaskRunnerActorFactory = NYql::NDq::NTaskRunnerActor::CreateTaskRunnerActorFactory(
                lwmOptions.Factory, lwmOptions.TaskRunnerInvokerFactory);
            lwmOptions.FunctionRegistry = FunctionRegistry_.Get();
            lwmOptions.Factory = factory;
            auto localWM = CreateLocalWorkerManager(lwmOptions);
            ActorRuntime_->AddLocalService(MakeWorkerManagerActorID(NodeId(i)),
                TActorSetupCmd{localWM, TMailboxType::Simple, 0}, i);
            ActorRuntime_->AddLocalService(GetNameserviceActorId(),
                TActorSetupCmd(NYql::NDqs::CreateDynamicNameserver(nameserverTable), TMailboxType::Simple, 0), i);
        }

        ActorRuntime_->Initialize();

        for (ui32 i = 1; i < nodesNumber; i++) {
            ActorRuntime_->GetLogSettings(i)->Append(
                NKikimrServices::EServiceKikimr_MIN,
                NKikimrServices::EServiceKikimr_MAX,
                NKikimrServices::EServiceKikimr_Name
            );
            TString explanation;
            auto err = ActorRuntime_->GetLogSettings(i)->SetLevel(NActors::NLog::PRI_EMERG, NKikimrServices::KQP_COMPUTE, explanation); //do not care about CA errors in this test"
            Y_ABORT_IF(err);
        }

        NActors::TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, nodesNumber);
        ActorRuntime_->DispatchEvents(options);

        auto attributes = NYT::BuildYsonNodeFluently()
            .BeginMap()
            .Item(NCommonAttrs::ACTOR_NODEID_ATTR).Value(NodeId())
            .Item(NCommonAttrs::HOSTNAME_ATTR).Value("localhost")
            .Item(NCommonAttrs::GRPCPORT_ATTR).Value("1")
            .Item(NCommonAttrs::UPLOAD_EXECUTABLE_ATTR).Value("true")
            .EndMap();

        // Init GWM.
        auto leaderEv = MakeHolder<TEvBecomeLeader>(1, "1", NYT::NodeToYsonString(attributes));
        const auto dummyActor = ActorRuntime_->AllocateEdgeActor();
        ActorRuntime_->Send(new IEventHandle(MakeWorkerManagerActorID(NodeId()), dummyActor,  leaderEv.Release()));

        auto statusEv = MakeHolder<TEvClusterStatus>();
        const auto statusSender = ActorRuntime_->AllocateEdgeActor();
        ActorRuntime_->Send(new IEventHandle(MakeWorkerManagerActorID(NodeId()), statusSender, statusEv.Release()));
        auto resp = ActorRuntime_->GrabEdgeEvent<TEvClusterStatusResponse>(statusSender);

        // Fake file for GWM.
        TFile outFile("/tmp/test", CreateAlways | ARW);
        outFile.Close();
    }

    void TearDown() override {
        ActorRuntime_.Reset();
        remove("/tmp/test");
    }

    void TestTasksAllocation1() {
        TVector<TVector<TString>> filesPerNode = {
            {"file0", "file11"},
            {"file0", "file22"},
            {"file0", "file33", "file333"},
            {"file0", "file44"},
            {"file0", "file55", "file555"},
            {"file0", "file66", "file6"}
        };

        TVector<TVector<TString>> filesPerTask = {
            {"file333"} // Expect mapping on node 3.
        };

        TVector<ui32> expectedTasksMapping = {3};

        CheckTasksAllocation(filesPerNode, filesPerTask, expectedTasksMapping);
    }

    void TestTasksAllocation2() {
        TVector<TVector<TString>> filesPerNode = {
            {"file0", "file11"},
            {"file0", "file22"},
            {"file0", "file33", "file333"},
            {"file0", "file44"},
            {"file0", "file55", "file555"},
            {"file0", "file66", "file6"}
        };

        TVector<TVector<TString>> filesPerTask = {
            {"file55", "file0"}, // Expect mapping on node 5.
            {"file0", "file11"} // Expect mapping on node 1.
        };

        TVector<ui32> expectedTasksMapping = {5, 1};

        CheckTasksAllocation(filesPerNode, filesPerTask, expectedTasksMapping);
    }

    void TestTasksAllocation3() {
        TVector<TVector<TString>> filesPerNode = {
            {"file0", "file11"},
            {"file0", "file22"},
            {"file0", "file33", "file333"},
            {"file0", "file44"},
            {"file0", "file55", "file555"},
            {"file0", "file66", "file6"}
        };

        TVector<TVector<TString>> filesPerTask = {
            {"file0", "file11"}, // Expect mapping on node 1.
            {"file55", "file0"}, // Expect mapping on node 5.
            {"file44", "file0"} // Expect mapping on node 4.
        };

        TVector<ui32> expectedTasksMapping = {1, 5, 4};

        CheckTasksAllocation(filesPerNode, filesPerTask, expectedTasksMapping);
    }

    void TestTasksAllocation4() {
        TVector<TVector<TString>> filesPerNode = {
            {"file0", "file11"},
            {"file0", "file22"},
            {"file0", "file33", "file333"},
            {"file0", "file44"},
            {"file0", "file55", "file555"},
            {"file0", "file66", "file6"}
        };

        TVector<TVector<TString>> filesPerTask = {
            {"file11"}, // Expect mapping on node 1.
            {"file0", "file333", "file33"}, // Expect mapping on node 3.
            {"file0", "file22"}, // Expect mapping on node 2.
            {"file0", "file555"} // Expect mapping on node 5.
        };

        TVector<ui32> expectedTasksMapping = {1, 3, 2, 5};

        CheckTasksAllocation(filesPerNode, filesPerTask, expectedTasksMapping);
    }

    void TestTasksAllocation5() {
        TVector<TVector<TString>> filesPerNode = {
            {"file0", "file11"},
            {"file0", "file22"},
            {"file0", "file33", "file333"},
            {"file0", "file44"},
            {"file0", "file55", "file555"},
            {"file0", "file66", "file5"}
        };

        TVector<TVector<TString>> filesPerTask = {
            {"file0", "file5", "file66"}, // Expect mapping on node 6.
            {"file0", "file333"}, // Expect mapping on node 3.
            {"file0", "file11"}, // Expect mapping on node 1.
            {"file0", "file44"}, // Expect mapping on node 4.
            {"file0", "file55", "file555"} // Expect mapping on node 5.
        };

        TVector<ui32> expectedTasksMapping = {6, 3, 1, 4, 5};

        CheckTasksAllocation(filesPerNode, filesPerTask, expectedTasksMapping);
    }

    void CheckTasksAllocation(
        TVector<TVector<TString>>& filesPerNode,
        TVector<TVector<TString>>& filesPerTask,
        const TVector<ui32>& expectedTasksMapping
    ) {
        Y_ASSERT(expectedTasksMapping.size() == filesPerTask.size());
        Y_ASSERT(filesPerTask.size() <= filesPerNode.size());

        SetupWorkerNodes(filesPerNode);

        const auto execActor = ActorRuntime_->AllocateEdgeActor();
        const auto allocatorActor = RegisterResourceAllocator(filesPerTask.size(), execActor);

        auto allocateRequest = MakeAllocationRequest(filesPerTask);

        ActorRuntime_->Send(new IEventHandle(MakeWorkerManagerActorID(NodeId()), allocatorActor, allocateRequest.Release()));

        auto resp = ActorRuntime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(execActor);

        auto allocatedWorkers = resp->Get()->Record.GetWorkers();

        UNIT_ASSERT_VALUES_EQUAL(filesPerTask.size(), allocatedWorkers.WorkerSize());
        for (ui32 task = 0; task < expectedTasksMapping.size(); ++task) {
            ui32 expectedNode = expectedTasksMapping[task];
            UNIT_ASSERT_VALUES_EQUAL(NodeId(expectedNode), allocatedWorkers.worker(task).GetNodeId());
        }
    }

    TActorId RegisterResourceAllocator(const ui32 workersCount, const TActorId& execActor) const {
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto gwmActor = MakeWorkerManagerActorID(NodeId());
        TVector<NYql::NDqProto::TDqTask> tasks(workersCount);
        auto allocator = CreateResourceAllocator(gwmActor, execActor, execActor, workersCount, "TraceId", new TDqConfiguration(), counters, tasks);
        const auto allocatorId = ActorRuntime_->Register(allocator);
        return allocatorId;
    }

    void SetupWorkerNodes(TVector<TVector<TString>>& filesOnNodes) const {
        ui32 nodeId = 1;
        for (const auto& nodeFiles : filesOnNodes) {
            RegisterNodeInGwm(NodeId(nodeId++), nodeFiles);
        }
    }

    THolder<TEvAllocateWorkersRequest> MakeAllocationRequest(TVector<TVector<TString>>& filesPerTask) const {
        auto allocateRequest = MakeHolder<TEvAllocateWorkersRequest>(filesPerTask.size(), "Username");
        allocateRequest->Record.SetTraceId("TraceId");
        allocateRequest->Record.SetCreateComputeActor(true);
        THashSet<TString> allFiles;

        for (const auto& tf : filesPerTask) {
            *allocateRequest->Record.AddTask() = NYql::NDqProto::TDqTask{};
            Yql::DqsProto::TWorkerFilter taskFiles;
            for (const auto& f : tf) {
                Yql::DqsProto::TFile file;
                file.SetObjectId(f + "_id");
                file.SetLocalPath("/tmp/test");
                *taskFiles.AddFile() = file;
                allFiles.emplace(f);
            }
            *allocateRequest->Record.AddWorkerFilterPerTask() = taskFiles;
        }

        for (const auto& f : allFiles) {
            Yql::DqsProto::TFile file;
            file.SetObjectId(f + "_id");
            file.SetLocalPath("/tmp/test");
            *allocateRequest->Record.AddFiles() = file;
        }

        return allocateRequest;
    }

    ui32 NodeId(ui32 id = 0) const {
        return ActorRuntime_->GetNodeId(id);
    }

    void RegisterNodeInGwm(const ui32 nodeId, const TVector<TString>& files) const {
        Yql::DqsProto::RegisterNodeRequest req;
        req.SetCapabilities(Yql::DqsProto::RegisterNodeRequest::ECAP_RUNEXE);
        req.SetNodeId(nodeId);
        req.SetPort(1);
        req.SetRole("worker_node");
        req.SetAddress("Address");
        req.SetRevision(ToString(GetProgramCommitId()));
        req.SetRunningWorkers(1);
        req.SetClusterName("plato");
        req.MutableKnownNodes()->Add(NodeId());
        req.MutableKnownNodes()->Add(nodeId);
        req.MutableGuid()->SetDw0(1);
        req.MutableGuid()->SetDw1(1);
        req.MutableGuid()->SetDw2(1);
        req.MutableGuid()->SetDw3(nodeId);

        for (const auto& file : files) {
            auto fileOnNode = req.AddFilesOnNode();
            fileOnNode->SetObjectId(file + "_id");
        }

        auto ev = MakeHolder<TEvRegisterNode>(req);
        const auto sender = ActorRuntime_->AllocateEdgeActor();
        ActorRuntime_->Send(new IEventHandle(MakeWorkerManagerActorID(NodeId()), sender, ev.Release()));

        ActorRuntime_->GrabEdgeEvent<TEvRegisterNodeResponse>(sender);
    }

    THolder<NActors::TTestActorRuntimeBase> ActorRuntime_;
    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> FunctionRegistry_;
};

UNIT_TEST_SUITE_REGISTRATION(TGlobalWorkerManagerTest)
