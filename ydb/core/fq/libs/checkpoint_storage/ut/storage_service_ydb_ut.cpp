#include <ydb/core/fq/libs/checkpoint_storage/storage_service.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/checkpointing_common/defs.h>
#include <ydb/core/fq/libs/checkpoint_storage/events/events.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <ydb/library/yql/dq/actors/compute/dq_checkpoints.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <library/cpp/retry/retry.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>

#include <util/system/env.h>

namespace NFq {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString GraphId = "graph_graphich";
ui64 Generation = 17;

const TCheckpointId CheckpointId1(17, 1);
const TCheckpointId CheckpointId2(17, 2);
const TCheckpointId CheckpointId3(17, 3);

constexpr TDuration TestTimeout = TDuration::Seconds(30);

using TRuntimePtr = std::unique_ptr<TTestActorRuntime>;

////////////////////////////////////////////////////////////////////////////////

template <bool UseYdbSdk>
class TStorageServiceTest : public NUnitTest::TTestBase{
    using TSelf = TStorageServiceTest<UseYdbSdk>;

public:
    void SetUp() override {
        Init();
    }

    void Init(bool enableGc = false) {
        if constexpr (UseYdbSdk) {
            InitSdkConnection(enableGc);
        } else {
            InitLocalConnection();
        }
        WaitBootstrapped();
        Cerr << "\n--------------------------- INIT FINISHED ---------------------------\n";
    }

    void InitSdkConnection(bool enableGc) {
        Runtime = std::make_unique<TTestBasicRuntime>(1, true);
        Runtime->SetLogPriority(NKikimrServices::STREAMS_STORAGE_SERVICE, NLog::PRI_DEBUG);

        NConfig::TCheckpointCoordinatorConfig config;
        config.SetEnabled(true);
        auto& storageConfig = *config.MutableStorage();
        storageConfig.SetEndpoint(GetEnv("YDB_ENDPOINT"));
        storageConfig.SetDatabase(GetEnv("YDB_DATABASE"));
        storageConfig.SetToken("");
        storageConfig.SetTablePrefix(CreateGuidAsString());

        auto& gcConfig = *config.MutableCheckpointGarbageConfig();
        gcConfig.SetEnabled(enableGc);

        auto driverConfig = NYdb::TDriverConfig();
        NYdb::TDriver driver(driverConfig);
        auto credFactory = NKikimr::CreateYdbCredentialsProviderFactory;
        auto storageService = NewCheckpointStorageService(config, "id", credFactory, std::move(driver), MakeIntrusive<::NMonitoring::TDynamicCounters>());

        Runtime->AddLocalService(
            NYql::NDq::MakeCheckpointStorageID(),
            TActorSetupCmd(storageService.release(), TMailboxType::Simple, 0));

        SetupTabletServices(*Runtime);
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void InitLocalConnection() {
        MsgBusPort = PortManager.GetPort(2134);
        GrpcPort = PortManager.GetPort(2135);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBuiltinDomain(true);
        ServerSettings = MakeHolder<Tests::TServerSettings>(MsgBusPort, authConfig);
        ServerSettings->AppConfig->MutableFeatureFlags()->SetEnableStreamingQueries(true);

        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableStreamingQueries(true);
        ServerSettings->SetFeatureFlags(featureFlags);

        ServerSettings->SetEnableScriptExecutionOperations(true);
        ServerSettings->SetInitializeFederatedQuerySetupFactory(true);
        ServerSettings->SetGrpcPort(GrpcPort);
        ServerSettings->NodeCount = 1;
        Server = MakeHolder<Tests::TServer>(*ServerSettings);
        Client = MakeHolder<Tests::TClient>(*ServerSettings);
        GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);
        GetRuntime()->SetLogPriority(NKikimrServices::STREAMS_STORAGE_SERVICE, NActors::NLog::PRI_DEBUG);
        GetRuntime()->SetDispatchTimeout(TestTimeout);
        Server->EnableGRpc(GrpcPort);
        Client->InitRootScheme();
    }

    void WaitBootstrapped() {
        TActorId sender = GetRuntime()->AllocateEdgeActor();
        while (true) {
            try {
                auto request = std::make_unique<TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest>("aaa");
                GetRuntime()->Send(new IEventHandle(NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));
                const auto event = GetRuntime()->template GrabEdgeEvent<TEvCheckpointStorage::TEvGetCheckpointsMetadataResponse>(sender, TDuration::Seconds(1));
                 if (event && event->Get()->Issues.Empty()) {
                    break;
                }
            } catch (TEmptyEventQueueException&) {
            }
        }

        while (true) {
            try {
                auto request = std::make_unique<TEvCheckpointStorage::TEvRegisterCoordinatorRequest>(TCoordinatorId{"test", 777});
                GetRuntime()->Send(new IEventHandle(NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));
                const auto event = GetRuntime()->template GrabEdgeEvent<TEvCheckpointStorage::TEvRegisterCoordinatorResponse>(sender, TDuration::Seconds(1));
                 if (event && event->Get()->Issues.Empty()) {
                    break;
                }
            } catch (TEmptyEventQueueException&) {
            }
        }
    }

    TTestActorRuntime* GetRuntime() {
        if constexpr (UseYdbSdk) {
            return Runtime.get();
        } else {
            return Server->GetRuntime();
        }
    }

    void RegisterCoordinator(
        const TCoordinatorId& coordinatorId,
        bool expectFailure = false)
    {
        TActorId sender = GetRuntime()->AllocateEdgeActor();
        auto request = std::make_unique<TEvCheckpointStorage::TEvRegisterCoordinatorRequest>(coordinatorId);
                
        GetRuntime()->Send(new IEventHandle(
            NYql::NDq::MakeCheckpointStorageID(), sender, request.release(), IEventHandle::FlagTrackDelivery));

        TAutoPtr<IEventHandle> handle;

        auto* event = GetRuntime()->template GrabEdgeEvent<TEvCheckpointStorage::TEvRegisterCoordinatorResponse>(handle);
        UNIT_ASSERT(event);

        if (expectFailure) {
            UNIT_ASSERT(!event->Issues.Empty());
        } else {
            UNIT_ASSERT_C(event->Issues.Empty(), event->Issues.ToOneLineString());
        }
    }

    void RegisterDefaultCoordinator() {
        TCoordinatorId coordinatorId(GraphId, Generation);
        RegisterCoordinator(coordinatorId);
    }

    template <class TResponse>
    void CheckCheckpointResponse(
        const TCheckpointId& checkpointId,
        bool expectFailure = false)
    {
        TAutoPtr<IEventHandle> handle;
        auto* event = GetRuntime()->template GrabEdgeEvent<TResponse>(handle);
        UNIT_ASSERT(event);

        if (expectFailure) {
            UNIT_ASSERT(!event->Issues.Empty());
        } else {
            UNIT_ASSERT_C(event->Issues.Empty(), event->Issues.ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(event->CheckpointId, checkpointId);
        }
    }

    void CreateCheckpoint(
        const TString& graphId,
        ui64 generation,
        const TCheckpointId& checkpointId,
        bool expectFailure = false)
    {
        TActorId sender = GetRuntime()->AllocateEdgeActor();

        TCoordinatorId coordinatorId(graphId, generation);
        auto request = std::make_unique<TEvCheckpointStorage::TEvCreateCheckpointRequest>(coordinatorId, checkpointId, 0, NProto::TCheckpointGraphDescription());
        GetRuntime()->Send(new IEventHandle(
            NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

        CheckCheckpointResponse<TEvCheckpointStorage::TEvCreateCheckpointResponse>(
            checkpointId,
            expectFailure);
    }

    void AbortCheckpoint(
        const TString& graphId,
        ui64 generation,
        const TCheckpointId& checkpointId,
        bool expectFailure = false)
    {
        TActorId sender = GetRuntime()->AllocateEdgeActor();

        TCoordinatorId coordinatorId(graphId, generation);
        auto request = std::make_unique<TEvCheckpointStorage::TEvAbortCheckpointRequest>(coordinatorId, checkpointId, "test reason");
        GetRuntime()->Send(new IEventHandle(
            NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

        CheckCheckpointResponse<TEvCheckpointStorage::TEvAbortCheckpointResponse>(
            checkpointId,
            expectFailure);
    }

    template <class TRequest, class TResponse>
    void CheckpointOperation(
        const TString& graphId,
        ui64 generation,
        const TCheckpointId& checkpointId,
        bool expectFailure = false)
    {
        TActorId sender = GetRuntime()->AllocateEdgeActor();

        TCoordinatorId coordinatorId(graphId, generation);
        std::unique_ptr<TRequest> request;

        if constexpr (std::is_same_v<TRequest, TEvCheckpointStorage::TEvCompleteCheckpointRequest>)
            request = std::make_unique<TRequest>(coordinatorId, checkpointId, 100, NYql::NDqProto::CHECKPOINT_TYPE_SNAPSHOT);
        else
            request = std::make_unique<TRequest>(coordinatorId, checkpointId, 100);
        GetRuntime()->Send(new IEventHandle(
            NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

        CheckCheckpointResponse<TResponse>(
            checkpointId,
            expectFailure);
    }

    void PendingCommitCheckpoint(
        const TString& graphId,
        ui64 generation,
        const TCheckpointId& checkpointId,
        bool expectFailure = false) {
        CheckpointOperation<TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest, TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusResponse>(
            graphId, generation, checkpointId, expectFailure);
    }
    void CompleteCheckpoint(
        const TString& graphId,
        ui64 generation,
        const TCheckpointId& checkpointId,
        bool expectFailure = false) {
        CheckpointOperation<TEvCheckpointStorage::TEvCompleteCheckpointRequest, TEvCheckpointStorage::TEvCompleteCheckpointResponse>(
            graphId, generation, checkpointId, expectFailure);
    }

    TCheckpoints GetCheckpoints(
        const TString& graphId)
    {
        TActorId sender = GetRuntime()->AllocateEdgeActor();
        auto request = std::make_unique<TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest>(graphId);
        GetRuntime()->Send(new IEventHandle(
            NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

        TAutoPtr<IEventHandle> handle;
        auto* event = GetRuntime()->template GrabEdgeEvent<TEvCheckpointStorage::TEvGetCheckpointsMetadataResponse>(handle);
        UNIT_ASSERT(event);
        UNIT_ASSERT(event->Issues.Empty());
        return event->Checkpoints;
    }

    void SaveState(
        ui64 taskId,
        const TCheckpointId& checkpointId,
        const TString& blob)
    {
        TActorId sender = GetRuntime()->AllocateEdgeActor();
        TCoordinatorId coordinatorId(GraphId, Generation);

        // XXX use proper checkpointId
        auto checkpoint = NYql::NDqProto::TCheckpoint();
        checkpoint.SetGeneration(checkpointId.CoordinatorGeneration);
        checkpoint.SetId(checkpointId.SeqNo);
        auto request = std::make_unique<NYql::NDq::TEvDqCompute::TEvSaveTaskState>(GraphId, taskId, checkpoint);
        request->State.MiniKqlProgram.ConstructInPlace().Data.Blob = blob;
        GetRuntime()->Send(new IEventHandle(NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

        TAutoPtr<IEventHandle> handle;
        auto* event = GetRuntime()->template GrabEdgeEvent<NYql::NDq::TEvDqCompute::TEvSaveTaskStateResult>(handle);
        UNIT_ASSERT(event);
        UNIT_ASSERT_C(event->Record.GetStatus() == NYql::NDqProto::TEvSaveTaskStateResult::OK, event->Record.DebugString());
    }

    TString GetState(
        ui64 taskId,
        const TString& graphId,
        const TCheckpointId& checkpointId)
    {
        TActorId sender = GetRuntime()->AllocateEdgeActor();

        auto checkpoint = NYql::NDqProto::TCheckpoint();
        checkpoint.SetGeneration(checkpointId.CoordinatorGeneration);
        checkpoint.SetId(checkpointId.SeqNo);
        auto request = std::make_unique<NYql::NDq::TEvDqCompute::TEvGetTaskState>(graphId, std::vector{taskId}, checkpoint, Generation);
        GetRuntime()->Send(new IEventHandle(
            NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

        TAutoPtr<IEventHandle> handle;
        auto* event = GetRuntime()->template GrabEdgeEvent<NYql::NDq::TEvDqCompute::TEvGetTaskStateResult>(handle);
        UNIT_ASSERT(event);
        UNIT_ASSERT(event->Issues.Empty());
        UNIT_ASSERT(!event->States.empty());

        return event->States[0].MiniKqlProgram->Data.Blob;
    }

    void CreateCompletedCheckpoint(
        const TString& graphId,
        ui64 generation,
        const TCheckpointId& checkpointId)
    {
        CreateCheckpoint(graphId, generation, checkpointId, false);
        PendingCommitCheckpoint(graphId, generation, checkpointId, false);
        CompleteCheckpoint(graphId, generation, checkpointId, false);
    }

    TString MakeState(const TString& value) {
        TString nodesState;
        auto mkqlState = NKikimr::NMiniKQL::TOutputSerializer::MakeSimpleBlobState(value, 0);
        NKikimr::NMiniKQL::TNodeStateHelper::AddNodeState(nodesState, mkqlState.AsStringRef());
        return nodesState;
    }

    UNIT_TEST_SUITE_DEMANGLE(TSelf);
    UNIT_TEST(ShouldRegister);
    UNIT_TEST(ShouldNotRegisterPrevGeneration);
    UNIT_TEST(ShouldRegisterNextGeneration);
    UNIT_TEST(ShouldCreateCheckpoint);
    UNIT_TEST(ShouldNotCreateCheckpointWhenUnregistered);
    UNIT_TEST(ShouldNotCreateCheckpointTwice);
    UNIT_TEST(ShouldNotCreateCheckpointAfterGenerationChanged);
    UNIT_TEST(ShouldGetCheckpoints);
    UNIT_TEST(ShouldPendingAndCompleteCheckpoint);
    UNIT_TEST(ShouldAbortCheckpoint);
    UNIT_TEST(ShouldNotPendingCheckpointWithoutCreation);
    UNIT_TEST(ShouldNotCompleteCheckpointWithoutCreation);
    UNIT_TEST(ShouldNotAbortCheckpointWithoutCreation);
    UNIT_TEST(ShouldNotCompleteCheckpointWithoutPending);
    UNIT_TEST(ShouldNotPendingCheckpointGenerationChanged);
    UNIT_TEST(ShouldNotCompleteCheckpointGenerationChanged);
    UNIT_TEST(ShouldSaveState);
    UNIT_TEST(ShouldGetState);
    UNIT_TEST(ShouldUseGc);
    UNIT_TEST_SUITE_END();

    void ShouldRegister() {
        RegisterDefaultCoordinator();
    }

    /*
 *  We weakened registration condition at while, registration with the same generation
 *  is not possible
 *
    Y_UNIT_TEST_F(ShouldNotRegisterSameTwice)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldNotRegisterSameTwice");

        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(runtime, coordinator1);
        RegisterCoordinator(runtime, coordinator1, true);
    }
*/

    void ShouldNotRegisterPrevGeneration() {
        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(coordinator1);

        TCoordinatorId coordinator2(GraphId, Generation - 1);
        RegisterCoordinator(coordinator2, true);
    }

    void ShouldRegisterNextGeneration() {
        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(coordinator1);

        TCoordinatorId coordinator2(GraphId, Generation + 1);
        RegisterCoordinator(coordinator2);

        // try register prev generation again
        RegisterCoordinator(coordinator1, true);
    }

    void ShouldCreateCheckpoint() {
        RegisterDefaultCoordinator();
        CreateCheckpoint(GraphId, Generation, CheckpointId1, false);
    }

    void ShouldNotCreateCheckpointWhenUnregistered() {
        CreateCheckpoint(GraphId, Generation, CheckpointId1, true);
    }

    void ShouldNotCreateCheckpointTwice() {
        RegisterDefaultCoordinator();
        CreateCheckpoint(GraphId, Generation, CheckpointId1, false);
        CreateCheckpoint(GraphId, Generation, CheckpointId1, true);
    }

    void ShouldNotCreateCheckpointAfterGenerationChanged() {
        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(coordinator1);
        CreateCheckpoint(GraphId, Generation, CheckpointId1, false);

        TCoordinatorId coordinator2(GraphId, Generation + 1);
        RegisterCoordinator(coordinator2);

        // second checkpoint, but with previous generation
        CreateCheckpoint(GraphId, Generation, CheckpointId2, true);
    }

    void ShouldGetCheckpoints() {
        RegisterDefaultCoordinator();
        CreateCheckpoint(GraphId, Generation, CheckpointId1, false);
        CreateCheckpoint(GraphId, Generation, CheckpointId2, false);
        CreateCheckpoint(GraphId, Generation, CheckpointId3, false);

        auto checkpoints = GetCheckpoints(GraphId);
        UNIT_ASSERT_VALUES_EQUAL(checkpoints.size(), 3UL);

        THashSet<TCheckpointId, TCheckpointIdHash> checkpoinIds;
        for (const auto& checkpoint: checkpoints) {
            UNIT_ASSERT(checkpoint.Status == ECheckpointStatus::Pending);
            checkpoinIds.insert(checkpoint.CheckpointId);
        }

        UNIT_ASSERT(checkpoinIds.contains(CheckpointId1));
        UNIT_ASSERT(checkpoinIds.contains(CheckpointId2));
        UNIT_ASSERT(checkpoinIds.contains(CheckpointId3));
    }

    void ShouldPendingAndCompleteCheckpoint() {
        RegisterDefaultCoordinator();
        CreateCheckpoint(GraphId, Generation, CheckpointId1, false);
        PendingCommitCheckpoint(GraphId, Generation, CheckpointId1, false);

        CreateCheckpoint(GraphId, Generation, CheckpointId2, false);
        PendingCommitCheckpoint(GraphId, Generation, CheckpointId2, false);
        CompleteCheckpoint(GraphId, Generation, CheckpointId2, false);

        auto checkpoints = GetCheckpoints(GraphId);
        UNIT_ASSERT_VALUES_EQUAL(checkpoints.size(), 2UL);

        for (const auto& checkpoint: checkpoints) {
            if (checkpoint.CheckpointId == CheckpointId1) {
                UNIT_ASSERT(checkpoint.Status == ECheckpointStatus::PendingCommit);
            } else if (checkpoint.CheckpointId == CheckpointId2) {
                UNIT_ASSERT(checkpoint.Status == ECheckpointStatus::Completed);
            } else {
                UNIT_ASSERT(false);
            }
        }
    }

    void ShouldAbortCheckpoint() {
        RegisterDefaultCoordinator();
        CreateCheckpoint(GraphId, Generation, CheckpointId1, false);
        PendingCommitCheckpoint(GraphId, Generation, CheckpointId1, false);

        CreateCheckpoint(GraphId, Generation, CheckpointId2, false);
        PendingCommitCheckpoint(GraphId, Generation, CheckpointId2, false);
        CompleteCheckpoint(GraphId, Generation, CheckpointId2, false);

        AbortCheckpoint(GraphId, Generation, CheckpointId1, false);
        AbortCheckpoint(GraphId, Generation, CheckpointId2, false);

        auto checkpoints = GetCheckpoints(GraphId);
        UNIT_ASSERT_VALUES_EQUAL(checkpoints.size(), 2UL);

        for (const auto& checkpoint: checkpoints) {
            UNIT_ASSERT(checkpoint.Status == ECheckpointStatus::Aborted);
        }
    }

    void ShouldNotPendingCheckpointWithoutCreation() {
        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(coordinator1);

        PendingCommitCheckpoint(GraphId, Generation, CheckpointId1, true);
    }

    void ShouldNotCompleteCheckpointWithoutCreation() {
        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(coordinator1);

        CompleteCheckpoint(GraphId, Generation, CheckpointId1, true);
    }

    void ShouldNotAbortCheckpointWithoutCreation() {
        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(coordinator1);

        AbortCheckpoint(GraphId, Generation, CheckpointId1, true);
    }

    void ShouldNotCompleteCheckpointWithoutPending() {
        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(coordinator1);
        CreateCheckpoint(GraphId, Generation, CheckpointId1, false);

        CompleteCheckpoint(GraphId, Generation, CheckpointId1, true);
    }

    void ShouldNotPendingCheckpointGenerationChanged() {
        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(coordinator1);
        CreateCheckpoint(GraphId, Generation, CheckpointId1, false);

        auto nextGen = Generation + 1;
        TCoordinatorId coordinator2(GraphId, nextGen);
        RegisterCoordinator(coordinator2);

        PendingCommitCheckpoint(GraphId, Generation, CheckpointId1, true);
    }

    void ShouldNotCompleteCheckpointGenerationChanged() {
        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(coordinator1);
        CreateCheckpoint(GraphId, Generation, CheckpointId1, false);
        PendingCommitCheckpoint(GraphId, Generation, CheckpointId1, false);

        auto nextGen = Generation + 1;
        TCoordinatorId coordinator2(GraphId, nextGen);
        RegisterCoordinator(coordinator2);

        CompleteCheckpoint(GraphId, Generation, CheckpointId1, true);
    }

    void ShouldSaveState() {
        NKikimr::NMiniKQL::TScopedAlloc Alloc(__LOCATION__);

        RegisterDefaultCoordinator();
        CreateCheckpoint(GraphId, Generation, CheckpointId1, false);

        SaveState(1317, CheckpointId1, MakeState("some random state"));
    }

    void ShouldGetState() {
        NKikimr::NMiniKQL::TScopedAlloc Alloc(__LOCATION__);

        RegisterDefaultCoordinator();
        CreateCheckpoint(GraphId, Generation, CheckpointId1, false);
        auto state = MakeState("some random state");
        SaveState(1317, CheckpointId1, state);

        auto actual = GetState(1317, GraphId, CheckpointId1);
        UNIT_ASSERT_VALUES_EQUAL(state, actual);
    }

    void ShouldUseGc() {
        Init(true);
        RegisterDefaultCoordinator();
        CreateCompletedCheckpoint(GraphId, Generation, CheckpointId1);
        CreateCompletedCheckpoint(GraphId, Generation, CheckpointId2);
        CreateCompletedCheckpoint(GraphId, Generation, CheckpointId3);

        TCheckpoints checkpoints;
        DoWithRetry<yexception>([&]() {
            checkpoints = GetCheckpoints(GraphId);
            if (checkpoints.size() != 1) {
                throw yexception() << "gc not finished yet";
            }
        }, TRetryOptions(100, TDuration::MilliSeconds(100)), true);
    }

private:
    TPortManager PortManager;
    ui16 MsgBusPort = 0;
    ui16 GrpcPort = 0;
    THolder<Tests::TServerSettings> ServerSettings;
    THolder<Tests::TServer> Server;
    THolder<Tests::TClient> Client;
    THolder<NYdb::TDriver> YdbDriver;
    THolder<NYdb::NTable::TTableClient> TableClient;
    THolder<NYdb::NTable::TSession> TableClientSession;

    TRuntimePtr Runtime;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

using TStorageServiceSdkTest = TStorageServiceTest<true>; 
using TStorageServiceLocalTest = TStorageServiceTest<false>;

UNIT_TEST_SUITE_REGISTRATION(TStorageServiceSdkTest);
UNIT_TEST_SUITE_REGISTRATION(TStorageServiceLocalTest);

} // namespace NFq
