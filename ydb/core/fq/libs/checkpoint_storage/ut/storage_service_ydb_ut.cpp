#include <ydb/core/fq/libs/checkpoint_storage/events/events.h>
#include <ydb/core/fq/libs/checkpoint_storage/storage_service.h>
#include <ydb/core/fq/libs/checkpoint_storage/ydb_checkpoint_storage.h>
#include <ydb/core/fq/libs/checkpoint_storage/ydb_state_storage.h>
#include <ydb/core/fq/libs/ydb/local_session.h>
#include <ydb/core/fq/libs/ydb/sdk_session.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/checkpointing_common/defs.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/retry/retry.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/system/env.h>
#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>

#include <functional>

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

// Local session construction must run in an actor activation context.
class TSnapshotSessionActor: public TActorBootstrapped<TSnapshotSessionActor> {
public:
    explicit TSnapshotSessionActor(NThreading::TPromise<ISession::TPtr> promise)
        : Promise(std::move(promise))
    {}

    void Bootstrap() {
        Promise.SetValue(CreateLocalSession());
        PassAway();
    }

private:
    NThreading::TPromise<ISession::TPtr> Promise;
};

class TSnapshotStorageCallActor: public TActorBootstrapped<TSnapshotStorageCallActor> {
public:
    using TFuture = NThreading::TFuture<NYql::TIssues>;
    TSnapshotStorageCallActor(NThreading::TPromise<TFuture> promise, std::function<TFuture()> operation)
        : Promise(std::move(promise)), Operation(std::move(operation))
    {}

    void Bootstrap() {
        Promise.SetValue(Operation());
        PassAway();
    }

private:
    NThreading::TPromise<TFuture> Promise;
    std::function<TFuture()> Operation;
};

template <bool UseYdbSdk>
class TStorageServiceTest: public NUnitTest::TTestBase {
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
        TablePrefix = CreateGuidAsString();
        storageConfig.SetTablePrefix(TablePrefix);

        auto& gcConfig = *config.MutableCheckpointGarbageConfig();
        gcConfig.SetEnabled(enableGc);

        auto driverConfig = NYdb::TDriverConfig();
        NYdb::TDriver driver(driverConfig);
        auto credFactory = NKikimr::CreateYdbCredentialsProviderFactory;
        auto storageService =
            NewCheckpointStorageService(config, "id", credFactory, std::move(driver), MakeIntrusive<::NMonitoring::TDynamicCounters>());

        Runtime->AddLocalService(NYql::NDq::MakeCheckpointStorageID(), TActorSetupCmd(storageService.release(), TMailboxType::Simple, 0));

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
        ServerSettings->AppConfig->MutableTableServiceConfig()->MutableQueryLimits()->SetResultRowsLimit(5);

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
                const auto event =
                    GetRuntime()->template GrabEdgeEvent<TEvCheckpointStorage::TEvGetCheckpointsMetadataResponse>(sender, TDuration::Seconds(1));
                if (event && event->Get()->Issues.Empty()) {
                    break;
                }
            } catch (TEmptyEventQueueException&) {
            }
        }

        while (true) {
            try {
                auto request = std::make_unique<TEvCheckpointStorage::TEvRegisterCoordinatorRequest>(TCoordinatorId{ "test", 777 });
                GetRuntime()->Send(new IEventHandle(NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));
                const auto event =
                    GetRuntime()->template GrabEdgeEvent<TEvCheckpointStorage::TEvRegisterCoordinatorResponse>(sender, TDuration::Seconds(1));
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

    void RegisterCoordinator(const TCoordinatorId& coordinatorId, bool expectFailure = false)
    {
        TActorId sender = GetRuntime()->AllocateEdgeActor();
        auto request = std::make_unique<TEvCheckpointStorage::TEvRegisterCoordinatorRequest>(coordinatorId);

        GetRuntime()->Send(new IEventHandle(NYql::NDq::MakeCheckpointStorageID(), sender, request.release(), IEventHandle::FlagTrackDelivery));

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
    void CheckCheckpointResponse(const TCheckpointId& checkpointId, bool expectFailure = false)
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

    void CreateCheckpoint(const TString& graphId, ui64 generation, const TCheckpointId& checkpointId, bool expectFailure = false,
        const NProto::TCheckpointGraphDescription& description = {})
    {
        TActorId sender = GetRuntime()->AllocateEdgeActor();

        TCoordinatorId coordinatorId(graphId, generation);
        auto request = std::make_unique<TEvCheckpointStorage::TEvCreateCheckpointRequest>(
            coordinatorId, checkpointId, 0, description);
        GetRuntime()->Send(new IEventHandle(NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

        CheckCheckpointResponse<TEvCheckpointStorage::TEvCreateCheckpointResponse>(checkpointId, expectFailure);
    }

    void AbortCheckpoint(const TString& graphId, ui64 generation, const TCheckpointId& checkpointId, bool expectFailure = false)
    {
        TActorId sender = GetRuntime()->AllocateEdgeActor();

        TCoordinatorId coordinatorId(graphId, generation);
        auto request = std::make_unique<TEvCheckpointStorage::TEvAbortCheckpointRequest>(coordinatorId, checkpointId, "test reason");
        GetRuntime()->Send(new IEventHandle(NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

        CheckCheckpointResponse<TEvCheckpointStorage::TEvAbortCheckpointResponse>(checkpointId, expectFailure);
    }

    template <class TRequest, class TResponse>
    void CheckpointOperation(const TString& graphId, ui64 generation, const TCheckpointId& checkpointId, bool expectFailure = false)
    {
        TActorId sender = GetRuntime()->AllocateEdgeActor();

        TCoordinatorId coordinatorId(graphId, generation);
        std::unique_ptr<TRequest> request;

        if constexpr (std::is_same_v<TRequest, TEvCheckpointStorage::TEvCompleteCheckpointRequest>) {
            request = std::make_unique<TRequest>(coordinatorId, checkpointId, 100, NYql::NDqProto::CHECKPOINT_TYPE_SNAPSHOT);
        } else {
            request = std::make_unique<TRequest>(coordinatorId, checkpointId, 100);
        }
        GetRuntime()->Send(new IEventHandle(NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

        CheckCheckpointResponse<TResponse>(checkpointId, expectFailure);
    }

    void PendingCommitCheckpoint(const TString& graphId, ui64 generation, const TCheckpointId& checkpointId, bool expectFailure = false) {
        CheckpointOperation<TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest,
            TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusResponse>(graphId, generation, checkpointId, expectFailure);
    }

    void CompleteCheckpoint(const TString& graphId, ui64 generation, const TCheckpointId& checkpointId, bool expectFailure = false) {
        CheckpointOperation<TEvCheckpointStorage::TEvCompleteCheckpointRequest, TEvCheckpointStorage::TEvCompleteCheckpointResponse>(
            graphId, generation, checkpointId, expectFailure);
    }

    TCheckpoints GetCheckpoints(const TString& graphId)
    {
        TActorId sender = GetRuntime()->AllocateEdgeActor();
        auto request = std::make_unique<TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest>(graphId);
        GetRuntime()->Send(new IEventHandle(NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

        TAutoPtr<IEventHandle> handle;
        auto* event = GetRuntime()->template GrabEdgeEvent<TEvCheckpointStorage::TEvGetCheckpointsMetadataResponse>(handle);
        UNIT_ASSERT(event);
        UNIT_ASSERT(event->Issues.Empty());
        return event->Checkpoints;
    }

    void SaveState(ui64 taskId, const TCheckpointId& checkpointId, const TString& blob)
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

    TString GetState(ui64 taskId, const TString& graphId, const TCheckpointId& checkpointId)
    {
        TActorId sender = GetRuntime()->AllocateEdgeActor();

        auto checkpoint = NYql::NDqProto::TCheckpoint();
        checkpoint.SetGeneration(checkpointId.CoordinatorGeneration);
        checkpoint.SetId(checkpointId.SeqNo);
        auto request = std::make_unique<NYql::NDq::TEvDqCompute::TEvGetTaskState>(graphId, std::vector{ taskId }, checkpoint, Generation);
        GetRuntime()->Send(new IEventHandle(NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

        TAutoPtr<IEventHandle> handle;
        auto* event = GetRuntime()->template GrabEdgeEvent<NYql::NDq::TEvDqCompute::TEvGetTaskStateResult>(handle);
        UNIT_ASSERT(event);
        UNIT_ASSERT(event->Issues.Empty());
        UNIT_ASSERT(!event->States.empty());

        return event->States[0].MiniKqlProgram->Data.Blob;
    }

    void CreateCompletedCheckpoint(const TString& graphId, ui64 generation, const TCheckpointId& checkpointId)
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
    UNIT_TEST(ShouldTablesHaveAutoPartitioning);
    UNIT_TEST(ShouldDeleteGraphViaEvent);
    UNIT_TEST(SnapshotReadsMetadataGraphAndAllRowsStable);
    UNIT_TEST(SnapshotReadsMetadataGraphAndAllRowsAcrossDelete);
    UNIT_TEST(SnapshotReadsMetadataGraphAndAllRowsAcrossDrop);
    UNIT_TEST(SnapshotRejectsMissingMetadata);
    UNIT_TEST(SnapshotRejectsContinuationAfterRollback);
    UNIT_TEST(SnapshotFailureAfterMetadataCannotMoveToFreshSession);
    UNIT_TEST(SnapshotDetectsTruncatedResult);
    UNIT_TEST(SnapshotRejectsClosedSdkSession);
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
        for (const auto& checkpoint : checkpoints) {
            UNIT_ASSERT(checkpoint.Status == ECheckpointStatus::Pending);
            checkpoinIds.insert(checkpoint.CheckpointId);
        }

        UNIT_ASSERT(checkpoinIds.contains(CheckpointId1));
        UNIT_ASSERT(checkpoinIds.contains(CheckpointId2));
        UNIT_ASSERT(checkpoinIds.contains(CheckpointId3));
    }

    void ShouldPendingAndCompleteCheckpoint() {
        Init(true);
        RegisterDefaultCoordinator();
        CreateCheckpoint(GraphId, Generation, CheckpointId1, false);
        PendingCommitCheckpoint(GraphId, Generation, CheckpointId1, false);

        CreateCheckpoint(GraphId, Generation, CheckpointId2, false);
        PendingCommitCheckpoint(GraphId, Generation, CheckpointId2, false);
        CompleteCheckpoint(GraphId, Generation, CheckpointId2, false);

        auto checkpoints = GetCheckpoints(GraphId);
        UNIT_ASSERT_VALUES_EQUAL(checkpoints.size(), 1UL);
        UNIT_ASSERT(checkpoints[0].Status == ECheckpointStatus::Completed);
    }

    void ShouldAbortCheckpoint() {
        Init(true);
        RegisterDefaultCoordinator();
        CreateCheckpoint(GraphId, Generation, CheckpointId1, false);
        PendingCommitCheckpoint(GraphId, Generation, CheckpointId1, false);
        AbortCheckpoint(GraphId, Generation, CheckpointId1, false);

        CreateCheckpoint(GraphId, Generation, CheckpointId2, false);
        PendingCommitCheckpoint(GraphId, Generation, CheckpointId2, false);
        CompleteCheckpoint(GraphId, Generation, CheckpointId2, false);
        AbortCheckpoint(GraphId, Generation, CheckpointId2, false);

        auto checkpoints = GetCheckpoints(GraphId);
        UNIT_ASSERT_VALUES_EQUAL(checkpoints.size(), 1UL);

        for (const auto& checkpoint : checkpoints) {
            UNIT_ASSERT_C(checkpoint.Status == ECheckpointStatus::Aborted, "checkpoint.Status " << checkpoint.Status);
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
        size_t count = 20;
        ui64 taskId1 = 1316;
        ui64 taskId2 = 1317;

        for (size_t seqNo = 0; seqNo < count; ++seqNo) {
            auto checkpointId = TCheckpointId(Generation, seqNo);
            CreateCheckpoint(GraphId, Generation, checkpointId, false);
            auto state1 = MakeState(TStringBuilder() << "some random state " << seqNo << " " << taskId1);
            SaveState(taskId1, checkpointId, state1);
            auto state2 = MakeState(TStringBuilder() << "some random state " << seqNo << " " << taskId2);
            SaveState(taskId2, checkpointId, state2);
        }

        ui64 seqNo = 10;
        auto checkpointId = TCheckpointId(Generation, seqNo);
        auto actual1 = GetState(taskId1, GraphId, checkpointId);
        auto expected1 = MakeState(TStringBuilder() << "some random state " << seqNo << " " << taskId1);
        UNIT_ASSERT_VALUES_EQUAL(expected1, actual1);

        auto actual2 = GetState(taskId2, GraphId, checkpointId);
        auto expected2 = MakeState(TStringBuilder() << "some random state " << seqNo << " " << taskId2);
        UNIT_ASSERT_VALUES_EQUAL(expected2, actual2);
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

    // Verifies that all checkpoint storage tables have auto-partitioning enabled
    // and MinPartitionsCount=1, for both local (TTableCreator) and SDK creation paths.
    void ShouldTablesHaveAutoPartitioning() {
        TString endpoint;
        TString database;
        TString tablePathPrefix;

        if constexpr (UseYdbSdk) {
            // SDK mode: tables are at YDB_DATABASE/TablePrefix/tableName
            endpoint = GetEnv("YDB_ENDPOINT");
            database = GetEnv("YDB_DATABASE");
            tablePathPrefix = database + "/" + TablePrefix;
        } else {
            // Local mode: the storage proxy uses CHECKPOINTS_TABLE_PREFIX =
            // ".metadata/streaming/checkpoints" relative to the tenant root.
            endpoint = TStringBuilder() << "localhost:" << GrpcPort;
            database = Server->GetRuntime()->GetAppData().TenantName;
            tablePathPrefix = database + "/.metadata/streaming/checkpoints";
        }

        auto driverConfig = NYdb::TDriverConfig().SetEndpoint(endpoint);
        NYdb::TDriver driver(driverConfig);
        NYdb::NTable::TTableClient tableClient(driver);

        // Obtain a session to call DescribeTable (session-level method)
        auto sessionResult = tableClient.CreateSession().GetValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(), "Failed to create YDB session: " << sessionResult.GetIssues().ToOneLineString());
        auto session = sessionResult.GetSession();

        // Check each of the three tables created by TCheckpointStorage::Init()
        auto checkTable = [&](const TString& tableName) {
            TString fullPath = tablePathPrefix + "/" + tableName;
            auto describeResult = session.DescribeTable(fullPath).GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(),
                "Failed to describe table '" << tableName << "' at path '" << fullPath << "': " << describeResult.GetIssues().ToOneLineString());

            const auto& tableDesc = describeResult.GetTableDescription();
            const auto& partSettings = tableDesc.GetPartitioningSettings();

            // Verify auto-partitioning by size is enabled
            auto partBySize = partSettings.GetPartitioningBySize();
            UNIT_ASSERT_C(partBySize.has_value(), "Partitioning-by-size setting is absent for table '" << tableName << "'");
            UNIT_ASSERT_C(*partBySize, "Partitioning-by-size is not enabled for table '" << tableName << "'");

            // Verify MinPartitionsCount == 1
            UNIT_ASSERT_VALUES_EQUAL_C(partSettings.GetMinPartitionsCount(), 1u, "MinPartitionsCount != 1 for table '" << tableName << "'");
        };

        checkTable("coordinators_sync");
        checkTable("checkpoints_metadata");
        checkTable("checkpoints_graphs_description");
        checkTable("states");

        driver.Stop(true);
    }

    // No RetryOperation surrounds an attempt: losing this session/transaction
    // must never turn a later page into a read at a different snapshot.
    ISession::TPtr NewSnapshotSession(NYdb::NTable::TTableClient& client) {
        if constexpr (UseYdbSdk) {
            auto result = client.CreateSession().GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return CreateSdkSession(result.GetSession());
        } else {
            auto promise = NThreading::NewPromise<ISession::TPtr>();
            GetRuntime()->Register(new TSnapshotSessionActor(promise));
            return promise.GetFuture().GetValueSync();
        }
    }

    TString SnapshotPrefix() {
        return UseYdbSdk ? GetEnv("YDB_DATABASE") + "/" + TablePrefix
                         : GetRuntime()->GetAppData().TenantName + "/.metadata/streaming/checkpoints";
    }

    NYdb::NTable::TDataQueryResult SnapshotQuery(ISession::TPtr session, const TString& sql,
        ISession::TTxControl tx, std::shared_ptr<NYdb::TParamsBuilder> params = {})
    {
        auto result = session->ExecuteDataQuery(
            TStringBuilder() << "PRAGMA TablePathPrefix(\"" << SnapshotPrefix() << "\"); " << sql,
            tx, std::move(params), NYdb::NTable::TExecDataQuerySettings().ClientTimeout(TestTimeout)
                .OperationTimeout(TestTimeout)).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        if (auto transaction = result.GetTransaction()) {
            Cerr << "snapshot test mode=" << (UseYdbSdk ? "sdk" : "local")
                 << " transaction=" << transaction->GetId() << " deadline=" << TestTimeout << Endl;
        }
        session->UpdateTransaction(result.GetTransaction());
        for (const auto& set : result.GetResultSets()) {
            UNIT_ASSERT_C(!set.Truncated(), "A truncated snapshot page is not a successful read");
        }
        return result;
    }

    TString StatePredicate(ui64 taskId) {
        return TStringBuilder() << "graph_id = \"" << GraphId << "\" AND task_id = " << taskId
            << "ul AND coordinator_generation = 17ul AND seq_no = 1ul";
    }

    enum class ESnapshotInterleaving { Stable, GarbageCollection, Drop };

    void SnapshotConsistency(ESnapshotInterleaving interleaving) {
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        RegisterDefaultCoordinator();
        NProto::TCheckpointGraphDescription description;
        description.MutableGraph()->SetGraphId(GraphId);
        description.MutableGraph()->AddTasks()->SetId(1);
        description.MutableGraph()->AddTasks()->SetId(2);
        CreateCheckpoint(GraphId, Generation, CheckpointId1, false, description);
        SaveState(1, CheckpointId1, MakeState("first task state"));
        SaveState(2, CheckpointId1, MakeState("second task state"));
        PendingCommitCheckpoint(GraphId, Generation, CheckpointId1);
        CompleteCheckpoint(GraphId, Generation, CheckpointId1);

        NYdb::TDriver driver(NYdb::TDriverConfig{});
        NYdb::NTable::TTableClient client(driver, NYdb::NTable::TClientSettings()
            .DiscoveryEndpoint(GetEnv("YDB_ENDPOINT")).Database(GetEnv("YDB_DATABASE")));
        auto writer = NewSnapshotSession(client);
        auto reader = NewSnapshotSession(client);
        TVector<TString> expected;
        // Split actual writer-produced serialized bytes into four real table rows
        // per task. Preserve the writer's compression/type columns on every row.
        for (ui64 taskId : {1ul, 2ul}) {
            auto original = SnapshotQuery(writer, "SELECT blob FROM states WHERE " + StatePredicate(taskId) + ";",
                ISession::TTxControl::BeginAndCommitTx());
            auto parser = original.GetResultSetParser(0);
            UNIT_ASSERT(parser.TryNextRow());
            TString bytes = *parser.ColumnParser("blob").GetOptionalString();
            UNIT_ASSERT(!parser.TryNextRow());
            UNIT_ASSERT(bytes.size() >= 4);
            TVector<TString> chunks;
            for (ui64 i = 0; i < 4; ++i) {
                chunks.push_back(bytes.substr(bytes.size() * i / 4, bytes.size() * (i + 1) / 4 - bytes.size() * i / 4));
                expected.push_back(chunks.back());
            }
            for (ui64 i : {1ul, 2ul, 3ul, 0ul}) {
                auto params = std::make_shared<NYdb::TParamsBuilder>();
                params->AddParam("$chunk").String(chunks[i]).Build();
                SnapshotQuery(writer, TStringBuilder() << "DECLARE $chunk AS String; UPSERT INTO states "
                    << "(graph_id, task_id, coordinator_generation, seq_no, blob_seq_num, blob, type, is_compressed) "
                    << "SELECT graph_id, task_id, coordinator_generation, seq_no, " << i << "ul AS blob_seq_num, $chunk AS blob, type, is_compressed "
                    << "FROM states WHERE " << StatePredicate(taskId) << " AND blob_seq_num = 0ul;",
                    ISession::TTxControl::BeginAndCommitTx(), params);
            }
        }
        const TString metadataSql = TStringBuilder() << "SELECT status, graph_description_id FROM checkpoints_metadata WHERE graph_id = \"" << GraphId << "\";";
        const auto snapshotDeadline = TInstant::Now() + TestTimeout;
        auto metadata = SnapshotQuery(reader, metadataSql, ISession::TTxControl::BeginTx(true));
        UNIT_ASSERT(reader->HasActiveTransaction());
        const auto snapshotTransaction = metadata.GetTransaction();
        if constexpr (UseYdbSdk) {
            UNIT_ASSERT(snapshotTransaction);
            UNIT_ASSERT(!snapshotTransaction->GetId().empty());
        }
        auto metadataParser = metadata.GetResultSetParser(0);
        UNIT_ASSERT(metadataParser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(*metadataParser.ColumnParser("status").GetOptionalUint8(), static_cast<ui8>(ECheckpointStatus::Completed));
        const TString graphDescriptionId = *metadataParser.ColumnParser("graph_description_id").GetOptionalString();
        UNIT_ASSERT(!metadataParser.TryNextRow());
        const TString graphSql = TStringBuilder() << "SELECT graph_description FROM checkpoints_graphs_description WHERE id = \"" << graphDescriptionId << "\";";
        auto graphBefore = SnapshotQuery(writer, graphSql, ISession::TTxControl::BeginAndCommitTx());
        auto beforeParser = graphBefore.GetResultSetParser(0);
        UNIT_ASSERT(beforeParser.TryNextRow());
        const TString expectedGraph = description.SerializeAsString();
        UNIT_ASSERT_VALUES_EQUAL(*beforeParser.ColumnParser("graph_description").GetOptionalString(), expectedGraph);
        UNIT_ASSERT(!beforeParser.TryNextRow());
        if (interleaving != ESnapshotInterleaving::Stable) {
            // The deletion operations use a separate connection, and their
            // completion fences every later snapshot continuation read.
            if (interleaving == ESnapshotInterleaving::Drop) {
                DeleteSnapshotGraph();
            } else {
                GarbageCollectSnapshotGraph(driver);
            }
        }
        auto graph = SnapshotQuery(reader, graphSql, ISession::TTxControl::ContinueTx());
        if constexpr (UseYdbSdk) {
            UNIT_ASSERT(graph.GetTransaction());
            UNIT_ASSERT_VALUES_EQUAL(graph.GetTransaction()->GetId(), snapshotTransaction->GetId());
        }
        auto graphParser = graph.GetResultSetParser(0);
        UNIT_ASSERT(graphParser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(*graphParser.ColumnParser("graph_description").GetOptionalString(), expectedGraph);
        UNIT_ASSERT(!graphParser.TryNextRow());
        size_t row = 0;
        for (ui64 taskId : {1ul, 2ul}) {
            for (ui64 chunk = 0; chunk < 4; ++chunk) {
                auto result = SnapshotQuery(reader, TStringBuilder() << "SELECT blob FROM states WHERE "
                    << StatePredicate(taskId) << " AND blob_seq_num = " << chunk << "ul;",
                    taskId == 2 && chunk == 3 ? ISession::TTxControl::ContinueAndCommitTx() : ISession::TTxControl::ContinueTx());
                if constexpr (UseYdbSdk) {
                    UNIT_ASSERT(result.GetTransaction());
                    UNIT_ASSERT_VALUES_EQUAL(result.GetTransaction()->GetId(), snapshotTransaction->GetId());
                }
                auto parser = result.GetResultSetParser(0);
                UNIT_ASSERT(parser.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser("blob").GetOptionalString(), expected[row++]);
                UNIT_ASSERT(!parser.TryNextRow());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(row, 8);
        UNIT_ASSERT_C(TInstant::Now() < snapshotDeadline, "Snapshot attempt exceeded its 30-second deadline");
        UNIT_ASSERT(!reader->HasActiveTransaction());
        if (interleaving != ESnapshotInterleaving::Stable) {
            auto after = SnapshotQuery(writer, metadataSql, ISession::TTxControl::BeginAndCommitTx(true));
            UNIT_ASSERT_VALUES_EQUAL(after.GetResultSet(0).RowsCount(), 0);
            auto graphAfter = SnapshotQuery(writer, graphSql, ISession::TTxControl::BeginAndCommitTx(true));
            UNIT_ASSERT_VALUES_EQUAL(graphAfter.GetResultSet(0).RowsCount(), 0);
            auto stateAfter = SnapshotQuery(writer, "SELECT blob FROM states WHERE " + StatePredicate(1) + ";",
                ISession::TTxControl::BeginAndCommitTx(true));
            UNIT_ASSERT_VALUES_EQUAL(stateAfter.GetResultSet(0).RowsCount(), 0);
        }
    }

    void DeleteSnapshotGraph() {
        auto sender = GetRuntime()->AllocateEdgeActor();
        GetRuntime()->Send(new IEventHandle(NYql::NDq::MakeCheckpointStorageID(), sender,
            new TEvCheckpointStorage::TEvDeleteGraphRequest(GraphId)));
        TAutoPtr<IEventHandle> handle;
        auto* event = GetRuntime()->template GrabEdgeEvent<TEvCheckpointStorage::TEvDeleteGraphResponse>(handle, TestTimeout);
        UNIT_ASSERT(event);
        UNIT_ASSERT_C(event->Issues.Empty(), event->Issues.ToString());
    }

    void SnapshotReadsMetadataGraphAndAllRowsStable() { SnapshotConsistency(ESnapshotInterleaving::Stable); }
    void SnapshotReadsMetadataGraphAndAllRowsAcrossDelete() { SnapshotConsistency(ESnapshotInterleaving::GarbageCollection); }

    void SnapshotReadsMetadataGraphAndAllRowsAcrossDrop() { SnapshotConsistency(ESnapshotInterleaving::Drop); }

    void GarbageCollectSnapshotGraph(const NYdb::TDriver& driver) {
        IYdbConnection::TPtr connection;
        TExternalStorageSettings settings;
        if constexpr (UseYdbSdk) {
            settings.SetEndpoint(GetEnv("YDB_ENDPOINT"));
            settings.SetDatabase(GetEnv("YDB_DATABASE"));
            settings.SetPathPrefix(TablePrefix);
            connection = CreateSdkYdbConnection(settings, NKikimr::CreateYdbCredentialsProviderFactory, driver);
        } else {
            connection = CreateLocalYdbConnection(GetRuntime()->GetAppData().TenantName, ".metadata/streaming/checkpoints", 10);
        }
        auto metadata = NewYdbCheckpointStorage(settings, CreateEntityIdGenerator("snapshot"), connection);
        auto states = NewYdbStateStorage(TCheckpointStorageSettings{}, connection);
        const auto call = [&](std::function<NThreading::TFuture<NYql::TIssues>()> operation) {
            NThreading::TFuture<NYql::TIssues> future;
            if constexpr (UseYdbSdk) {
                future = operation();
            } else {
                auto promise = NThreading::NewPromise<NThreading::TFuture<NYql::TIssues>>();
                GetRuntime()->Register(new TSnapshotStorageCallActor(promise, std::move(operation)));
                future = promise.GetFuture().GetValueSync();
            }
            auto issues = future.GetValueSync();
            UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        };
        // These are the existing normal-GC operations, in their actual order.
        call([&] { return metadata->MarkCheckpointsGC(GraphId, CheckpointId2); });
        call([&] { return states->DeleteCheckpoints(GraphId, CheckpointId2); });
        call([&] { return metadata->DeleteMarkedCheckpoints(GraphId, CheckpointId2); });
    }

    void SnapshotRejectsMissingMetadata() {
        RegisterDefaultCoordinator();
        CreateCompletedCheckpoint(GraphId, Generation, CheckpointId1);
        NYdb::TDriver driver(NYdb::TDriverConfig{});
        NYdb::NTable::TTableClient client(driver, NYdb::NTable::TClientSettings()
            .DiscoveryEndpoint(GetEnv("YDB_ENDPOINT")).Database(GetEnv("YDB_DATABASE")));
        auto writer = NewSnapshotSession(client);
        SnapshotQuery(writer, "DELETE FROM checkpoints_metadata WHERE graph_id = \"" + GraphId + "\";",
            ISession::TTxControl::BeginAndCommitTx());
        auto reader = NewSnapshotSession(client);
        auto metadata = SnapshotQuery(reader, "SELECT graph_description_id FROM checkpoints_metadata WHERE graph_id = \"" + GraphId + "\";",
            ISession::TTxControl::BeginTx(true));
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetResultSet(0).RowsCount(), 0);
        // A graph row still exists, but cannot authorize an attempt by itself.
        auto graph = SnapshotQuery(writer, "SELECT graph_description FROM checkpoints_graphs_description;",
            ISession::TTxControl::BeginAndCommitTx(true));
        UNIT_ASSERT_VALUES_EQUAL(graph.GetResultSet(0).RowsCount(), 1);
        UNIT_ASSERT(reader->Rollback().GetValueSync().IsSuccess());
    }

    void SnapshotRejectsContinuationAfterRollback() {
        RegisterDefaultCoordinator();
        CreateCompletedCheckpoint(GraphId, Generation, CheckpointId1);
        NYdb::TDriver driver(NYdb::TDriverConfig{});
        NYdb::NTable::TTableClient client(driver, NYdb::NTable::TClientSettings()
            .DiscoveryEndpoint(GetEnv("YDB_ENDPOINT")).Database(GetEnv("YDB_DATABASE")));
        auto reader = NewSnapshotSession(client);
        SnapshotQuery(reader, "SELECT graph_description_id FROM checkpoints_metadata;", ISession::TTxControl::BeginTx(true));
        UNIT_ASSERT(reader->HasActiveTransaction());
        UNIT_ASSERT(reader->Rollback().GetValueSync().IsSuccess());
        UNIT_ASSERT(!reader->HasActiveTransaction());
        auto fresh = NewSnapshotSession(client);
        UNIT_ASSERT(!fresh->HasActiveTransaction());
        // ContinueTx on TSdkSession without a transaction dereferences an empty
        // optional. An attempt must reject here, before dispatching a query.
    }

    void SnapshotFailureAfterMetadataCannotMoveToFreshSession() {
        RegisterDefaultCoordinator();
        CreateCompletedCheckpoint(GraphId, Generation, CheckpointId1);
        NYdb::TDriver driver(NYdb::TDriverConfig{});
        NYdb::NTable::TTableClient client(driver, NYdb::NTable::TClientSettings()
            .DiscoveryEndpoint(GetEnv("YDB_ENDPOINT")).Database(GetEnv("YDB_DATABASE")));
        auto reader = NewSnapshotSession(client);
        SnapshotQuery(reader, "SELECT graph_description_id FROM checkpoints_metadata;", ISession::TTxControl::BeginTx(true));
        // Inject a real database failure after the first successful read.
        auto failed = reader->ExecuteDataQuery("SELECT * FROM `missing_snapshot_table`;",
            ISession::TTxControl::ContinueTx(), {}, NYdb::NTable::TExecDataQuerySettings()
                .ClientTimeout(TestTimeout).OperationTimeout(TestTimeout)).GetValueSync();
        UNIT_ASSERT(!failed.IsSuccess());
        reader.Reset(); // Whole attempt is discarded; do not retry its next page.
        auto fresh = NewSnapshotSession(client);
        UNIT_ASSERT(!fresh->HasActiveTransaction());
    }

    void SnapshotRejectsClosedSdkSession() {
        if constexpr (UseYdbSdk) {
            RegisterDefaultCoordinator();
            CreateCompletedCheckpoint(GraphId, Generation, CheckpointId1);
            NYdb::TDriver driver(NYdb::TDriverConfig{});
            NYdb::NTable::TTableClient client(driver, NYdb::NTable::TClientSettings()
                .DiscoveryEndpoint(GetEnv("YDB_ENDPOINT")).Database(GetEnv("YDB_DATABASE")));
            auto created = client.CreateSession().GetValueSync();
            UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
            auto sdkSession = created.GetSession();
            auto reader = CreateSdkSession(sdkSession);
            SnapshotQuery(reader, "SELECT graph_description_id FROM checkpoints_metadata;", ISession::TTxControl::BeginTx(true));
            auto closed = sdkSession.Close().GetValueSync();
            UNIT_ASSERT_C(closed.IsSuccess(), closed.GetIssues().ToString());
            auto failed = reader->ExecuteDataQuery("SELECT 1;", ISession::TTxControl::ContinueTx(), {},
                NYdb::NTable::TExecDataQuerySettings().ClientTimeout(TestTimeout).OperationTimeout(TestTimeout)).GetValueSync();
            UNIT_ASSERT(!failed.IsSuccess());
            UNIT_ASSERT(failed.GetStatus() == NYdb::EStatus::BAD_SESSION || failed.GetStatus() == NYdb::EStatus::SESSION_EXPIRED);
            reader.Reset();
            auto fresh = NewSnapshotSession(client);
            UNIT_ASSERT(!fresh->HasActiveTransaction());
        }
    }

    void SnapshotDetectsTruncatedResult() {
        // The local fixture explicitly sets ResultRowsLimit=5. An external
        // recipe server's limit is not controlled by this fixture.
        if constexpr (!UseYdbSdk) {
            RegisterDefaultCoordinator();
            for (ui64 seqNo = 1; seqNo <= 8; ++seqNo) {
                CreateCompletedCheckpoint(GraphId, Generation, TCheckpointId(Generation, seqNo));
            }
            NYdb::TDriver driver(NYdb::TDriverConfig{});
            NYdb::NTable::TTableClient client(driver);
            auto reader = NewSnapshotSession(client);
            auto result = reader->ExecuteDataQuery(TStringBuilder()
                << "PRAGMA TablePathPrefix(\"" << SnapshotPrefix() << "\"); SELECT seq_no FROM checkpoints_metadata;",
                ISession::TTxControl::BeginAndCommitTx(true), {}, NYdb::NTable::TExecDataQuerySettings()
                    .ClientTimeout(TestTimeout).OperationTimeout(TestTimeout)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(result.GetResultSet(0).Truncated());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 5);
        }
    }

    void ShouldDeleteGraphViaEvent() {
        RegisterDefaultCoordinator();
        CreateCheckpoint(GraphId, Generation, CheckpointId1, false);
        CreateCheckpoint(GraphId, Generation, CheckpointId2, false);

        // Verify checkpoints exist before deletion
        auto checkpointsBefore = GetCheckpoints(GraphId);
        UNIT_ASSERT_C(!checkpointsBefore.empty(), "Expected checkpoints to exist before deletion");

        // Send TEvDeleteGraphRequest event to StorageProxy
        auto sender = GetRuntime()->AllocateEdgeActor();
        auto request = std::make_unique<TEvCheckpointStorage::TEvDeleteGraphRequest>(GraphId);
        GetRuntime()->Send(new IEventHandle(NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

        // Wait for the response
        TAutoPtr<IEventHandle> handle;
        auto* event = GetRuntime()->template GrabEdgeEvent<TEvCheckpointStorage::TEvDeleteGraphResponse>(handle, TestTimeout);
        UNIT_ASSERT_C(event, "TEvDeleteGraphResponse not received");
        UNIT_ASSERT_C(event->Issues.Empty(), event->Issues.ToOneLineString());

        // Verify all checkpoints are gone
        auto checkpointsAfter = GetCheckpoints(GraphId);
        UNIT_ASSERT_C(checkpointsAfter.empty(), "Expected all checkpoints to be deleted after TEvDeleteGraphRequest");
    }

private:
    TPortManager PortManager;
    ui16 MsgBusPort = 0;
    ui16 GrpcPort = 0;
    TString TablePrefix;   // Stores the table prefix used during SDK initialization
    THolder<Tests::TServerSettings> ServerSettings;
    THolder<Tests::TServer> Server;
    THolder<Tests::TClient> Client;
    THolder<NYdb::TDriver> YdbDriver;
    THolder<NYdb::NTable::TTableClient> TableClient;
    THolder<NYdb::NTable::TSession> TableClientSession;

    TRuntimePtr Runtime;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

using TStorageServiceSdkTest = TStorageServiceTest<true>;
using TStorageServiceLocalTest = TStorageServiceTest<false>;

UNIT_TEST_SUITE_REGISTRATION(TStorageServiceSdkTest);
UNIT_TEST_SUITE_REGISTRATION(TStorageServiceLocalTest);

}   // namespace NFq
