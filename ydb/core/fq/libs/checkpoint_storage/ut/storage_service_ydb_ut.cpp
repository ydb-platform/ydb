#include <ydb/core/fq/libs/checkpoint_storage/storage_service.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/checkpointing_common/defs.h>
#include <ydb/core/fq/libs/checkpoint_storage/events/events.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <ydb/library/yql/dq/actors/compute/dq_checkpoints.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <library/cpp/retry/retry.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
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

using TRuntimePtr = std::unique_ptr<TTestActorRuntime>;

////////////////////////////////////////////////////////////////////////////////

TRuntimePtr PrepareTestActorRuntime(const char* tablePrefix, bool enableGc = false) {
    TRuntimePtr runtime(new TTestBasicRuntime(1, true));
    runtime->SetLogPriority(NKikimrServices::STREAMS_STORAGE_SERVICE, NLog::PRI_DEBUG);

    NConfig::TCheckpointCoordinatorConfig config;
    config.SetEnabled(true);
    auto& checkpointConfig = *config.MutableStorage();
    checkpointConfig.SetEndpoint(GetEnv("YDB_ENDPOINT"));
    checkpointConfig.SetDatabase(GetEnv("YDB_DATABASE"));
    checkpointConfig.SetToken("");
    checkpointConfig.SetTablePrefix(tablePrefix);

    NConfig::TCommonConfig commonConfig;
    commonConfig.SetIdsPrefix("id");

    auto& gcConfig = *config.MutableCheckpointGarbageConfig();
    gcConfig.SetEnabled(enableGc);

    auto credFactory = NKikimr::CreateYdbCredentialsProviderFactory;
    auto yqSharedResources = NFq::TYqSharedResources::Cast(NFq::CreateYqSharedResourcesImpl({}, credFactory, MakeIntrusive<NMonitoring::TDynamicCounters>()));
    auto storageService = NewCheckpointStorageService(config, commonConfig, credFactory, yqSharedResources);

    runtime->AddLocalService(
        NYql::NDq::MakeCheckpointStorageID(),
        TActorSetupCmd(storageService.release(), TMailboxType::Simple, 0));

    SetupTabletServices(*runtime);
    runtime->DispatchEvents({}, TDuration::Zero());

    return runtime;
}

void RegisterCoordinator(
    const TRuntimePtr& runtime,
    const TCoordinatorId& coordinatorId,
    bool expectFailure = false)
{
    TActorId sender = runtime->AllocateEdgeActor();
    auto request = std::make_unique<TEvCheckpointStorage::TEvRegisterCoordinatorRequest>(coordinatorId);
    runtime->Send(new IEventHandle(
        NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

    TAutoPtr<IEventHandle> handle;
    auto* event = runtime->GrabEdgeEvent<TEvCheckpointStorage::TEvRegisterCoordinatorResponse>(handle);
    UNIT_ASSERT(event);

    if (expectFailure) {
        UNIT_ASSERT(!event->Issues.Empty());
    } else {
        UNIT_ASSERT(event->Issues.Empty());
    }
}

void RegisterDefaultCoordinator(const TRuntimePtr& runtime) {
    TCoordinatorId coordinatorId(GraphId, Generation);
    RegisterCoordinator(runtime, coordinatorId);
}

template <class TResponse>
void CheckCheckpointResponse(
    const TRuntimePtr& runtime,
    const TCheckpointId& checkpointId,
    bool expectFailure = false)
{
    TAutoPtr<IEventHandle> handle;
    auto* event = runtime->GrabEdgeEvent<TResponse>(handle);
    UNIT_ASSERT(event);

    if (expectFailure) {
        UNIT_ASSERT(!event->Issues.Empty());
    } else {
        UNIT_ASSERT(event->Issues.Empty());
        UNIT_ASSERT_VALUES_EQUAL(event->CheckpointId, checkpointId);
    }
}

void CreateCheckpoint(
    const TRuntimePtr& runtime,
    const TString& graphId,
    ui64 generation,
    const TCheckpointId& checkpointId,
    bool expectFailure = false)
{
    TActorId sender = runtime->AllocateEdgeActor();

    TCoordinatorId coordinatorId(graphId, generation);
    auto request = std::make_unique<TEvCheckpointStorage::TEvCreateCheckpointRequest>(coordinatorId, checkpointId, 0, NProto::TCheckpointGraphDescription());
    runtime->Send(new IEventHandle(
        NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

    CheckCheckpointResponse<TEvCheckpointStorage::TEvCreateCheckpointResponse>(
        runtime,
        checkpointId,
        expectFailure);
}

void AbortCheckpoint(
    const TRuntimePtr& runtime,
    const TString& graphId,
    ui64 generation,
    const TCheckpointId& checkpointId,
    bool expectFailure = false)
{
    TActorId sender = runtime->AllocateEdgeActor();

    TCoordinatorId coordinatorId(graphId, generation);
    auto request = std::make_unique<TEvCheckpointStorage::TEvAbortCheckpointRequest>(coordinatorId, checkpointId, "test reason");
    runtime->Send(new IEventHandle(
        NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

    CheckCheckpointResponse<TEvCheckpointStorage::TEvAbortCheckpointResponse>(
        runtime,
        checkpointId,
        expectFailure);
}

template <class TRequest, class TResponse>
void CheckpointOperation(
    const TRuntimePtr& runtime,
    const TString& graphId,
    ui64 generation,
    const TCheckpointId& checkpointId,
    bool expectFailure = false)
{
    TActorId sender = runtime->AllocateEdgeActor();

    TCoordinatorId coordinatorId(graphId, generation);
    std::unique_ptr<TRequest> request;

    if constexpr (std::is_same_v<TRequest, TEvCheckpointStorage::TEvCompleteCheckpointRequest>)
        request = std::make_unique<TRequest>(coordinatorId, checkpointId, 100, NYql::NDqProto::TCheckpoint::EType::TCheckpoint_EType_SNAPSHOT);
    else
        request = std::make_unique<TRequest>(coordinatorId, checkpointId, 100);
    runtime->Send(new IEventHandle(
        NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

    CheckCheckpointResponse<TResponse>(
        runtime,
        checkpointId,
        expectFailure);
}

auto PendingCommitCheckpoint = CheckpointOperation<TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest, TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusResponse>;
auto CompleteCheckpoint = CheckpointOperation<TEvCheckpointStorage::TEvCompleteCheckpointRequest, TEvCheckpointStorage::TEvCompleteCheckpointResponse>;

TCheckpoints GetCheckpoints(
    const TRuntimePtr& runtime,
    const TString& graphId)
{
    TActorId sender = runtime->AllocateEdgeActor();

    auto request = std::make_unique<TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest>(graphId);
    runtime->Send(new IEventHandle(
        NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

    TAutoPtr<IEventHandle> handle;
    auto* event = runtime->GrabEdgeEvent<TEvCheckpointStorage::TEvGetCheckpointsMetadataResponse>(handle);
    UNIT_ASSERT(event);
    UNIT_ASSERT(event->Issues.Empty());

    return event->Checkpoints;
}

void SaveState(
    const TRuntimePtr& runtime,
    ui64 taskId,
    const TCheckpointId& checkpointId,
    const TString& blob)
{
    TActorId sender = runtime->AllocateEdgeActor();

    TCoordinatorId coordinatorId(GraphId, Generation);

    // XXX use proper checkpointId
    auto checkpoint = NYql::NDqProto::TCheckpoint();
    checkpoint.SetGeneration(checkpointId.CoordinatorGeneration);
    checkpoint.SetId(checkpointId.SeqNo);
    auto request = std::make_unique<NYql::NDq::TEvDqCompute::TEvSaveTaskState>(GraphId, taskId, checkpoint);
    request->State.MutableMiniKqlProgram()->MutableData()->MutableStateData()->SetBlob(blob);
    runtime->Send(new IEventHandle(NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

    TAutoPtr<IEventHandle> handle;
    auto* event = runtime->GrabEdgeEvent<NYql::NDq::TEvDqCompute::TEvSaveTaskStateResult>(handle);
    UNIT_ASSERT(event);
    UNIT_ASSERT_C(event->Record.GetStatus() == NYql::NDqProto::TEvSaveTaskStateResult::OK, event->Record.DebugString());
}

TString GetState(
    const TRuntimePtr& runtime,
    ui64 taskId,
    const TString& graphId,
    const TCheckpointId& checkpointId)
{
    TActorId sender = runtime->AllocateEdgeActor();

    auto checkpoint = NYql::NDqProto::TCheckpoint();
    checkpoint.SetGeneration(checkpointId.CoordinatorGeneration);
    checkpoint.SetId(checkpointId.SeqNo);
    auto request = std::make_unique<NYql::NDq::TEvDqCompute::TEvGetTaskState>(graphId, std::vector{taskId}, checkpoint, Generation);
    runtime->Send(new IEventHandle(
        NYql::NDq::MakeCheckpointStorageID(), sender, request.release()));

    TAutoPtr<IEventHandle> handle;
    auto* event = runtime->GrabEdgeEvent<NYql::NDq::TEvDqCompute::TEvGetTaskStateResult>(handle);
    UNIT_ASSERT(event);
    UNIT_ASSERT(event->Issues.Empty());
    UNIT_ASSERT(!event->States.empty());

    return event->States[0].GetMiniKqlProgram().GetData().GetStateData().GetBlob();
}

void CreateCompletedCheckpoint(
    const TRuntimePtr& runtime,
    const TString& graphId,
    ui64 generation,
    const TCheckpointId& checkpointId)
{
    CreateCheckpoint(runtime, graphId, generation, checkpointId, false);
    PendingCommitCheckpoint(runtime, graphId, generation, checkpointId, false);
    CompleteCheckpoint(runtime, graphId, generation, checkpointId, false);
}

TString MakeState(NYql::NUdf::TUnboxedValuePod&& value) {
    const TStringBuf savedBuf = value.AsStringRef();
    TString result;
    NKikimr::NMiniKQL::WriteUi32(result, savedBuf.Size());
    result.AppendNoAlias(savedBuf.Data(), savedBuf.Size());
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageServiceTest) {
    Y_UNIT_TEST(ShouldRegister)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldRegister");
        RegisterDefaultCoordinator(runtime);
    }

/*
 *  We weakened registration condition at while, registration with the same generation
 *  is not possible
 *
    Y_UNIT_TEST(ShouldNotRegisterSameTwice)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldNotRegisterSameTwice");

        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(runtime, coordinator1);
        RegisterCoordinator(runtime, coordinator1, true);
    }
*/
    Y_UNIT_TEST(ShouldNotRegisterPrevGeneration)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldNotRegisterPrevGeneration");

        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(runtime, coordinator1);

        TCoordinatorId coordinator2(GraphId, Generation - 1);
        RegisterCoordinator(runtime, coordinator2, true);
    }

    Y_UNIT_TEST(ShouldRegisterNextGeneration)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldRegisterNextGeneration");

        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(runtime, coordinator1);

        TCoordinatorId coordinator2(GraphId, Generation + 1);
        RegisterCoordinator(runtime, coordinator2);

        // try register prev generation again
        RegisterCoordinator(runtime, coordinator1, true);
    }

    Y_UNIT_TEST(ShouldCreateCheckpoint)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldCreateCheckpoint");

        RegisterDefaultCoordinator(runtime);
        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId1, false);
    }

    Y_UNIT_TEST(ShouldNotCreateCheckpointWhenUnregistered)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldNotCreateCheckpointWhenUnregistered");

        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId1, true);
    }

    Y_UNIT_TEST(ShouldNotCreateCheckpointTwice)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldNotCreateCheckpointTwice");

        RegisterDefaultCoordinator(runtime);
        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId1, false);
        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId1, true);
    }

    Y_UNIT_TEST(ShouldNotCreateCheckpointAfterGenerationChanged)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldNotCreateCheckpointAfterGenerationChanged");

        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(runtime, coordinator1);
        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId1, false);

        TCoordinatorId coordinator2(GraphId, Generation + 1);
        RegisterCoordinator(runtime, coordinator2);

        // second checkpoint, but with previous generation
        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId2, true);
    }

    Y_UNIT_TEST(ShouldGetCheckpoints)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldGetCheckpoints");

        RegisterDefaultCoordinator(runtime);
        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId1, false);
        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId2, false);
        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId3, false);

        auto checkpoints = GetCheckpoints(runtime, GraphId);
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

    Y_UNIT_TEST(ShouldPendingAndCompleteCheckpoint)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldPendingAndCompleteCheckpoint");

        RegisterDefaultCoordinator(runtime);
        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId1, false);
        PendingCommitCheckpoint(runtime, GraphId, Generation, CheckpointId1, false);

        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId2, false);
        PendingCommitCheckpoint(runtime, GraphId, Generation, CheckpointId2, false);
        CompleteCheckpoint(runtime, GraphId, Generation, CheckpointId2, false);

        auto checkpoints = GetCheckpoints(runtime, GraphId);
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

    Y_UNIT_TEST(ShouldAbortCheckpoint)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldPendingAndCompleteCheckpoint");

        RegisterDefaultCoordinator(runtime);
        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId1, false);
        PendingCommitCheckpoint(runtime, GraphId, Generation, CheckpointId1, false);

        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId2, false);
        PendingCommitCheckpoint(runtime, GraphId, Generation, CheckpointId2, false);
        CompleteCheckpoint(runtime, GraphId, Generation, CheckpointId2, false);

        AbortCheckpoint(runtime, GraphId, Generation, CheckpointId1, false);
        AbortCheckpoint(runtime, GraphId, Generation, CheckpointId2, false);

        auto checkpoints = GetCheckpoints(runtime, GraphId);
        UNIT_ASSERT_VALUES_EQUAL(checkpoints.size(), 2UL);

        for (const auto& checkpoint: checkpoints) {
            UNIT_ASSERT(checkpoint.Status == ECheckpointStatus::Aborted);
        }
    }

    Y_UNIT_TEST(ShouldNotPendingCheckpointWithoutCreation)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldNotPendingCheckpointWithoutCreation");

        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(runtime, coordinator1);

        PendingCommitCheckpoint(runtime, GraphId, Generation, CheckpointId1, true);
    }

    Y_UNIT_TEST(ShouldNotCompleteCheckpointWithoutCreation)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldNotCompleteCheckpointWithoutCreation");

        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(runtime, coordinator1);

        CompleteCheckpoint(runtime, GraphId, Generation, CheckpointId1, true);
    }

    Y_UNIT_TEST(ShouldNotAbortCheckpointWithoutCreation)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldNotPendingCheckpointWithoutCreation");

        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(runtime, coordinator1);

        AbortCheckpoint(runtime, GraphId, Generation, CheckpointId1, true);
    }

    Y_UNIT_TEST(ShouldNotCompleteCheckpointWithoutPending)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldNotCompleteCheckpointWithoutPending");

        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(runtime, coordinator1);
        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId1, false);

        CompleteCheckpoint(runtime, GraphId, Generation, CheckpointId1, true);
    }

    Y_UNIT_TEST(ShouldNotPendingCheckpointGenerationChanged)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldNotPendingCheckpointGenerationChanged");

        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(runtime, coordinator1);
        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId1, false);

        auto nextGen = Generation + 1;
        TCoordinatorId coordinator2(GraphId, nextGen);
        RegisterCoordinator(runtime, coordinator2);

        PendingCommitCheckpoint(runtime, GraphId, Generation, CheckpointId1, true);
    }

    Y_UNIT_TEST(ShouldNotCompleteCheckpointGenerationChanged)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldNotPendingCheckpointGenerationChanged");

        TCoordinatorId coordinator1(GraphId, Generation);
        RegisterCoordinator(runtime, coordinator1);
        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId1, false);
        PendingCommitCheckpoint(runtime, GraphId, Generation, CheckpointId1, false);

        auto nextGen = Generation + 1;
        TCoordinatorId coordinator2(GraphId, nextGen);
        RegisterCoordinator(runtime, coordinator2);

        CompleteCheckpoint(runtime, GraphId, Generation, CheckpointId1, true);
    }

    Y_UNIT_TEST(ShouldSaveState)
    {
        NKikimr::NMiniKQL::TScopedAlloc Alloc(__LOCATION__);
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldSaveState");

        RegisterDefaultCoordinator(runtime);
        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId1, false);

        SaveState(runtime, 1317, CheckpointId1, MakeState(NKikimr::NMiniKQL::TNodeStateHelper::MakeSimpleBlobState("some random state")));
    }

    Y_UNIT_TEST(ShouldGetState)
    {
        NKikimr::NMiniKQL::TScopedAlloc Alloc(__LOCATION__);
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldGetState");

        RegisterDefaultCoordinator(runtime);
        CreateCheckpoint(runtime, GraphId, Generation, CheckpointId1, false);
        auto state = MakeState(NKikimr::NMiniKQL::TNodeStateHelper::MakeSimpleBlobState("some random state"));
        SaveState(runtime, 1317, CheckpointId1, state);

        auto actual = GetState(runtime, 1317, GraphId, CheckpointId1);
        UNIT_ASSERT_VALUES_EQUAL(state, actual);
    }

    Y_UNIT_TEST(ShouldUseGc)
    {
        auto runtime = PrepareTestActorRuntime("TStorageServiceTestShouldUseGc", true);

        RegisterDefaultCoordinator(runtime);
        CreateCompletedCheckpoint(runtime, GraphId, Generation, CheckpointId1);
        CreateCompletedCheckpoint(runtime, GraphId, Generation, CheckpointId2);
        CreateCompletedCheckpoint(runtime, GraphId, Generation, CheckpointId3);

        TCheckpoints checkpoints;

        DoWithRetry<yexception>([&]() {
            checkpoints = GetCheckpoints(runtime, GraphId);
            if (checkpoints.size() != 1) {
                throw yexception() << "gc not finished yet";
            }
        }, TRetryOptions(100, TDuration::MilliSeconds(100)), true);
    }
};

} // namespace NFq
