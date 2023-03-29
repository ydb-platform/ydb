#include <ydb/core/fq/libs/checkpointing/checkpoint_coordinator.h>
#include <ydb/core/fq/libs/graph_params/proto/graph_params.pb.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/actors/core/executor_pool_basic.h>
#include <library/cpp/actors/core/scheduler_basic.h>

namespace {

using namespace NKikimr;
using namespace NFq;

enum ETestGraphFlags : ui64 {
    InputWithSource = 1,
    SourceWithChannelInOneTask = 2,
};

NYql::NDqProto::TReadyState BuildTestGraph(ui64 flags = 0) {

    NYql::NDqProto::TReadyState result;

    auto* ingress = result.AddTask();
    ingress->SetId(1);
    auto* ingressOutput = ingress->AddOutputs();
    ingressOutput->AddChannels();
    if (flags & ETestGraphFlags::InputWithSource) {
        auto* source = ingress->AddInputs()->MutableSource();
        source->SetType("PqSource");
    }

    auto* map = result.AddTask();
    map->SetId(2);
    auto* mapInput = map->AddInputs();
    mapInput->AddChannels();
    auto* mapOutput = map->AddOutputs();
    mapOutput->AddChannels();
    if (flags & ETestGraphFlags::SourceWithChannelInOneTask) {
        auto* source = map->AddInputs()->MutableSource();
        source->SetType("PqSource");
    }

    auto* egress = result.AddTask();
    egress->SetId(3);
    auto* egressInput = egress->AddInputs();
    egressInput->AddChannels();

    return result;
}

struct TTestBootstrap : public TTestActorRuntime {
    NYql::NDqProto::TReadyState GraphState;
    NConfig::TCheckpointCoordinatorConfig Settings;
    NActors::TActorId StorageProxy;
    NActors::TActorId CheckpointCoordinator;
    NActors::TActorId RunActor;

    NActors::TActorId IngressActor;
    NActors::TActorId MapActor;
    NActors::TActorId EgressActor;

    THashMap<TActorId, ui64> ActorToTask;

    ::NMonitoring::TDynamicCounterPtr Counters = new ::NMonitoring::TDynamicCounters();

    explicit TTestBootstrap(ui64 graphFlags = 0)
        : TTestActorRuntime(true)
        , GraphState(BuildTestGraph(graphFlags))
    {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Initialize(app->Unwrap());
        StorageProxy = AllocateEdgeActor();
        RunActor = AllocateEdgeActor();
        IngressActor = AllocateEdgeActor();
        MapActor = AllocateEdgeActor();
        EgressActor = AllocateEdgeActor();

        ActorIdToProto(IngressActor, GraphState.AddActorId());
        ActorIdToProto(MapActor, GraphState.AddActorId());
        ActorIdToProto(EgressActor, GraphState.AddActorId());

        ActorToTask[IngressActor] = GraphState.GetTask()[0].GetId();
        ActorToTask[MapActor]     = GraphState.GetTask()[1].GetId();
        ActorToTask[EgressActor]  = GraphState.GetTask()[2].GetId();

        Settings = NConfig::TCheckpointCoordinatorConfig();
        Settings.SetEnabled(true);
        Settings.SetCheckpointingPeriodMillis(TDuration::Hours(1).MilliSeconds());
        Settings.SetMaxInflight(1);

        NYql::TDqConfiguration::TPtr DqSettings = MakeIntrusive<NYql::TDqConfiguration>();

        SetLogPriority(NKikimrServices::STREAMS_CHECKPOINT_COORDINATOR, NLog::PRI_DEBUG);

        CheckpointCoordinator = Register(MakeCheckpointCoordinator(
            TCoordinatorId("my-graph-id", 42),
            StorageProxy,
            RunActor,
            Settings,
            Counters,
            NProto::TGraphParams(),
            FederatedQuery::StateLoadMode::FROM_LAST_CHECKPOINT,
            {},
            //
            "my-graph-id",
            {} /* ExecuterId */,
            RunActor,
            DqSettings,
            ::NYql::NCommon::TServiceCounters(Counters, nullptr, ""),
            TDuration::Seconds(3),
            TDuration::Seconds(1)
        ).Release());
        Send(new IEventHandle(CheckpointCoordinator, {}, new NYql::NDqs::TEvReadyState(std::move(GraphState))));

        EnableScheduleForActor(CheckpointCoordinator);
    }
};

} // namespace

namespace NFq {

void MockRegisterCoordinatorResponseEvent(TTestBootstrap& bootstrap, NYql::TIssues issues = NYql::TIssues()) {
    bootstrap.Send(new IEventHandle(
        bootstrap.CheckpointCoordinator,
        bootstrap.StorageProxy,
        new TEvCheckpointStorage::TEvRegisterCoordinatorResponse(std::move(issues))));
}

void MockCheckpointsMetadataResponse(TTestBootstrap& bootstrap, NYql::TIssues issues = NYql::TIssues()) {
    bootstrap.Send(new IEventHandle(
        bootstrap.CheckpointCoordinator,
        bootstrap.StorageProxy,
        new TEvCheckpointStorage::TEvGetCheckpointsMetadataResponse(TVector<TCheckpointMetadata>(), std::move(issues))));
}

void MockCreateCheckpointResponse(TTestBootstrap& bootstrap, TCheckpointId& checkpointId, NYql::TIssues issues = NYql::TIssues()) {
    bootstrap.Send(new IEventHandle(
        bootstrap.CheckpointCoordinator,
        bootstrap.StorageProxy,
        new TEvCheckpointStorage::TEvCreateCheckpointResponse(checkpointId, std::move(issues), "42")));
}

void MockNodeStateSavedEvent(TTestBootstrap& bootstrap, TCheckpointId& checkpointId, TActorId& sender) {
    auto ev = std::make_unique<NYql::NDq::TEvDqCompute::TEvSaveTaskStateResult>();
    ev->Record.MutableCheckpoint()->SetGeneration(checkpointId.CoordinatorGeneration);
    ev->Record.MutableCheckpoint()->SetId(checkpointId.SeqNo);
    ev->Record.SetStatus(NYql::NDqProto::TEvSaveTaskStateResult::OK);
    bootstrap.Send(new IEventHandle(
        bootstrap.CheckpointCoordinator,
        sender,
        ev.release()));
}

void MockNodeStateSaveFailedEvent(TTestBootstrap& bootstrap, TCheckpointId& checkpointId, TActorId& sender) {
    auto ev = std::make_unique<NYql::NDq::TEvDqCompute::TEvSaveTaskStateResult>();
    ev->Record.MutableCheckpoint()->SetGeneration(checkpointId.CoordinatorGeneration);
    ev->Record.MutableCheckpoint()->SetId(checkpointId.SeqNo);
    ev->Record.SetStatus(NYql::NDqProto::TEvSaveTaskStateResult::STORAGE_ERROR);
    bootstrap.Send(new IEventHandle(
        bootstrap.CheckpointCoordinator,
        sender,
        ev.release()));
}

void MockSetCheckpointPendingCommitStatusResponse(TTestBootstrap& bootstrap, TCheckpointId& checkpointId, NYql::TIssues issues = NYql::TIssues()) {
    bootstrap.Send(new IEventHandle(
        bootstrap.CheckpointCoordinator,
        bootstrap.StorageProxy,
        new TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusResponse(checkpointId, std::move(issues))));
}

void MockChangesCommittedEvent(TTestBootstrap& bootstrap, TCheckpointId& checkpointId, TActorId& sender) {
    bootstrap.Send(new IEventHandle(
        bootstrap.CheckpointCoordinator,
        sender,
        new NYql::NDq::TEvDqCompute::TEvStateCommitted(checkpointId.SeqNo, checkpointId.CoordinatorGeneration, bootstrap.ActorToTask[sender])));
}

void MockCompleteCheckpointResponse(TTestBootstrap& bootstrap, TCheckpointId& checkpointId, NYql::TIssues issues = NYql::TIssues()) {
    bootstrap.Send(new IEventHandle(
        bootstrap.CheckpointCoordinator,
        bootstrap.StorageProxy,
        new TEvCheckpointStorage::TEvCompleteCheckpointResponse(checkpointId, std::move(issues))));
}

Y_UNIT_TEST_SUITE(TCheckpointCoordinatorTests) {
    void ShouldTriggerCheckpointImpl(ui64 graphFlags) {
        TTestBootstrap bootstrap(graphFlags);

        Cerr << "Waiting for TEvRegisterCoordinatorRequest (storage)" << Endl;
        bootstrap.GrabEdgeEvent<TEvCheckpointStorage::TEvRegisterCoordinatorRequest>(
            bootstrap.StorageProxy, TDuration::Seconds(10));
        MockRegisterCoordinatorResponseEvent(bootstrap);

        Cerr << "Waiting for TEvGetCheckpointsMetadataRequest (storage)" << Endl;
        bootstrap.GrabEdgeEvent<TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest>(
            bootstrap.StorageProxy, TDuration::Seconds(10));
        MockCheckpointsMetadataResponse(bootstrap);

        Cerr << "Waiting for TEvCreateCheckpointRequest (storage)" << Endl;
        auto updateState = bootstrap.GrabEdgeEvent<TEvCheckpointStorage::TEvCreateCheckpointRequest>(
            bootstrap.StorageProxy, TDuration::Seconds(10));

        auto& checkpointId = updateState->Get()->CheckpointId;
        MockCreateCheckpointResponse(bootstrap, checkpointId);

        Cerr << "Waiting for TEvInjectCheckpointBarrier (ingress)" << Endl;
        bootstrap.GrabEdgeEvent<NYql::NDq::TEvDqCompute::TEvInjectCheckpoint>(
            bootstrap.IngressActor, TDuration::Seconds(10));

        MockNodeStateSavedEvent(bootstrap, checkpointId, bootstrap.IngressActor);
        MockNodeStateSavedEvent(bootstrap, checkpointId, bootstrap.MapActor);
        MockNodeStateSavedEvent(bootstrap, checkpointId, bootstrap.EgressActor);

        Cerr << "Waiting for TEvSetCheckpointPendingCommitStatusRequest (storage)" << Endl;
        bootstrap.GrabEdgeEvent<TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest>(
            bootstrap.StorageProxy, TDuration::Seconds(10));

        MockSetCheckpointPendingCommitStatusResponse(bootstrap, checkpointId);
        Cerr << "Waiting for TEvCommitChanges (ingress)" << Endl;
        bootstrap.GrabEdgeEvent<NYql::NDq::TEvDqCompute::TEvCommitState>(bootstrap.IngressActor, TDuration::Seconds(10));
        Cerr << "Waiting for TEvCommitChanges (egress)" << Endl;
        bootstrap.GrabEdgeEvent<NYql::NDq::TEvDqCompute::TEvCommitState>(bootstrap.EgressActor, TDuration::Seconds(10));

        MockChangesCommittedEvent(bootstrap, checkpointId, bootstrap.IngressActor);
        MockChangesCommittedEvent(bootstrap, checkpointId, bootstrap.MapActor);
        MockChangesCommittedEvent(bootstrap, checkpointId, bootstrap.EgressActor);

        Cerr << "Waiting for TEvCompleteCheckpointRequest (storage)" << Endl;
        auto completed = bootstrap.GrabEdgeEvent<TEvCheckpointStorage::TEvCompleteCheckpointRequest>(
            bootstrap.StorageProxy, TDuration::Seconds(10));
        UNIT_ASSERT(completed.Get() != nullptr);
        MockCompleteCheckpointResponse(bootstrap, checkpointId);
    }

    Y_UNIT_TEST(ShouldTriggerCheckpoint) {
        ShouldTriggerCheckpointImpl(0);
    }

    Y_UNIT_TEST(ShouldTriggerCheckpointWithSource) {
        ShouldTriggerCheckpointImpl(ETestGraphFlags::InputWithSource);
    }

    Y_UNIT_TEST(ShouldTriggerCheckpointWithSourceWithChannel) {
        ShouldTriggerCheckpointImpl(ETestGraphFlags::SourceWithChannelInOneTask);
    }

    Y_UNIT_TEST(ShouldTriggerCheckpointWithSourcesAndWithChannel) {
        ShouldTriggerCheckpointImpl(ETestGraphFlags::InputWithSource | ETestGraphFlags::SourceWithChannelInOneTask);
    }

    Y_UNIT_TEST(ShouldAbortPreviousCheckpointsIfNodeStateCantBeSaved) {
        TTestBootstrap bootstrap{ETestGraphFlags::InputWithSource};

        Cerr << "Waiting for TEvRegisterCoordinatorRequest (storage)" << Endl;
        bootstrap.GrabEdgeEvent<TEvCheckpointStorage::TEvRegisterCoordinatorRequest>(
            bootstrap.StorageProxy, TDuration::Seconds(10));
        MockRegisterCoordinatorResponseEvent(bootstrap);

        Cerr << "Waiting for TEvGetCheckpointsMetadataRequest (storage)" << Endl;
        bootstrap.GrabEdgeEvent<TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest>(
            bootstrap.StorageProxy, TDuration::Seconds(10));
        MockCheckpointsMetadataResponse(bootstrap);

        Cerr << "Waiting for TEvCreateCheckpointRequest (storage)" << Endl;
        auto updateState = bootstrap.GrabEdgeEvent<TEvCheckpointStorage::TEvCreateCheckpointRequest>(
            bootstrap.StorageProxy, TDuration::Seconds(10));
        UNIT_ASSERT(updateState->Get()->NodeCount == 3);

        auto& checkpointId = updateState->Get()->CheckpointId;
        MockCreateCheckpointResponse(bootstrap, checkpointId);

        Cerr << "Waiting for TEvInjectCheckpointBarrier (ingress)" << Endl;
        bootstrap.GrabEdgeEvent<NYql::NDq::TEvDqCompute::TEvInjectCheckpoint>(bootstrap.IngressActor, TDuration::Seconds(10));

        MockNodeStateSavedEvent(bootstrap, checkpointId, bootstrap.IngressActor);
        MockNodeStateSaveFailedEvent(bootstrap, checkpointId, bootstrap.MapActor);
        MockNodeStateSavedEvent(bootstrap, checkpointId, bootstrap.EgressActor);

        Cerr << "Waiting for TEvCompleteCheckpointRequest (storage)" << Endl;
        auto completed = bootstrap.GrabEdgeEvent<TEvCheckpointStorage::TEvAbortCheckpointRequest>(
            bootstrap.StorageProxy, TDuration::Seconds(10));
        UNIT_ASSERT(completed.Get() != nullptr);
    }
}

} // namespace NFq
