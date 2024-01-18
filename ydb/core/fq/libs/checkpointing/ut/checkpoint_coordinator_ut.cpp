#include <ydb/core/fq/libs/checkpointing/checkpoint_coordinator.h>
#include <ydb/core/fq/libs/graph_params/proto/graph_params.pb.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>

#include <google/protobuf/util/message_differencer.h>

namespace {

using namespace NKikimr;
using namespace NFq;

enum ETestGraphFlags : ui64 {
    InputWithSource = 1,
    SourceWithChannelInOneTask = 2,
};

NYql::NDqProto::TReadyState BuildTestGraph(ui64 flags, const TString& sourceType) {

    NYql::NDqProto::TReadyState result;

    auto* ingress = result.AddTask();
    ingress->SetId(1);
    auto* ingressOutput = ingress->AddOutputs();
    ingressOutput->AddChannels();
    if (flags & ETestGraphFlags::InputWithSource) {
        auto* source = ingress->AddInputs()->MutableSource();
        source->SetType(sourceType);
    }

    auto* map = result.AddTask();
    map->SetId(2);
    auto* mapInput = map->AddInputs();
    mapInput->AddChannels();
    auto* mapOutput = map->AddOutputs();
    mapOutput->AddChannels();
    if (flags & ETestGraphFlags::SourceWithChannelInOneTask) {
        auto* source = map->AddInputs()->MutableSource();
        source->SetType(sourceType);
    }

    auto* egress = result.AddTask();
    egress->SetId(3);
    auto* egressInput = egress->AddInputs();
    egressInput->AddChannels();
    auto* sink = egress->AddOutputs()->MutableSink();
    sink->SetType("PqSink");

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
    TCoordinatorId CoordinatorId;
    TCheckpointId CheckpointId;

    THashMap<TActorId, ui64> ActorToTask;

    ::NMonitoring::TDynamicCounterPtr Counters = new ::NMonitoring::TDynamicCounters();

    explicit TTestBootstrap(ui64 graphFlags, const TString& sourceType)
        : TTestActorRuntime(true)
        , GraphState(BuildTestGraph(graphFlags, sourceType))
        , CoordinatorId("my-graph-id", 42)
        , CheckpointId(CoordinatorId.Generation, 1)
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
            CoordinatorId,
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

    bool IsEqual(
        const TEvCheckpointStorage::TEvRegisterCoordinatorRequest& lhs,
        const TEvCheckpointStorage::TEvRegisterCoordinatorRequest& rhs) {
        return IsEqual(lhs.CoordinatorId, rhs.CoordinatorId);
    }

    bool IsEqual(
        const TEvCheckpointStorage::TEvCreateCheckpointRequest& lhs,
        const TEvCheckpointStorage::TEvCreateCheckpointRequest& rhs) {
        return IsEqual(lhs.CoordinatorId, rhs.CoordinatorId)
            && std::tie(lhs.CheckpointId, lhs.NodeCount) == std::tie(rhs.CheckpointId, rhs.NodeCount)
            && lhs.GraphDescription.index() == rhs.GraphDescription.index()
                 && (lhs.GraphDescription.index() == 0 
                    ? std::get<0>(lhs.GraphDescription) == std::get<0>(rhs.GraphDescription)
                    : google::protobuf::util::MessageDifferencer::Equals(std::get<1>(lhs.GraphDescription), std::get<1>(rhs.GraphDescription)));
    }

    bool IsEqual(
        const TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest& lhs,
        const TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest& rhs) {
        return IsEqual(lhs.CoordinatorId, rhs.CoordinatorId)
            && std::tie(lhs.CheckpointId, lhs.StateSizeBytes) == std::tie(rhs.CheckpointId, rhs.StateSizeBytes);
    }

    bool IsEqual(
        const TEvCheckpointStorage::TEvCompleteCheckpointRequest& lhs,
        const TEvCheckpointStorage::TEvCompleteCheckpointRequest& rhs) {
        return IsEqual(lhs.CoordinatorId, rhs.CoordinatorId)
            && std::tie(lhs.CheckpointId, lhs.StateSizeBytes) == std::tie(rhs.CheckpointId, rhs.StateSizeBytes);
    }

    bool IsEqual(
        const TEvCheckpointStorage::TEvAbortCheckpointRequest& lhs,
        const TEvCheckpointStorage::TEvAbortCheckpointRequest& rhs) {
        return IsEqual(lhs.CoordinatorId, rhs.CoordinatorId)
            && std::tie( lhs.CheckpointId, lhs.Reason) == std::tie(rhs.CheckpointId, rhs.Reason);
    }

    bool IsEqual(
        const TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest& lhs,
        const TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest& rhs) {
        return std::tie(lhs.GraphId, lhs.Statuses, lhs.Limit, lhs.LoadGraphDescription) == std::tie(rhs.GraphId, rhs.Statuses, rhs.Limit, rhs.LoadGraphDescription);
    }

    bool IsEqual(
        const NFq::TCoordinatorId& lhs,
        const NFq::TCoordinatorId& rhs) {
        return std::tie(lhs.GraphId, lhs.Generation) == std::tie(rhs.GraphId, rhs.Generation);
    }

    bool IsEqual(
        const NYql::NDq::TEvDqCompute::TEvNewCheckpointCoordinator& lhs,
        const NYql::NDq::TEvDqCompute::TEvNewCheckpointCoordinator& rhs) {
        return google::protobuf::util::MessageDifferencer::Equals(lhs.Record, rhs.Record);
    }

    bool IsEqual(
        const NYql::NDq::TEvDqCompute::TEvInjectCheckpoint& lhs,
        const NYql::NDq::TEvDqCompute::TEvInjectCheckpoint& rhs) {
        return google::protobuf::util::MessageDifferencer::Equals(lhs.Record, rhs.Record);
    }

    bool IsEqual(
        const NYql::NDq::TEvDqCompute::TEvCommitState& lhs,
        const NYql::NDq::TEvDqCompute::TEvCommitState& rhs) {
        return google::protobuf::util::MessageDifferencer::Equals(lhs.Record, rhs.Record);
    }

    bool IsEqual(
        const NYql::NDq::TEvDqCompute::TEvRun& lhs,
        const NYql::NDq::TEvDqCompute::TEvRun& rhs) {
        return google::protobuf::util::MessageDifferencer::Equals(lhs.Record, rhs.Record);
    }

    template <typename TEvent>
    void ExpectEvent(NActors::TActorId actorId, const TEvent& expectedEventValue) {
        auto eventHolder = GrabEdgeEvent<TEvent>(actorId, TDuration::Seconds(10));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        TEvent* actual = eventHolder.Get()->Get();
        UNIT_ASSERT(IsEqual(expectedEventValue, *actual));
    }

    void MockRegisterCoordinatorResponseEvent(NYql::TIssues issues = NYql::TIssues()) {
        Send(new IEventHandle(
            CheckpointCoordinator,
            StorageProxy,
            new TEvCheckpointStorage::TEvRegisterCoordinatorResponse(std::move(issues))));
    }

    void MockNewCheckpointCoordinatorAckEvent(TActorId& sender) {
        Send(new IEventHandle(
            CheckpointCoordinator,
            sender,
            new NYql::NDq::TEvDqCompute::TEvNewCheckpointCoordinatorAck()));
    }

    void MockCheckpointsMetadataResponse(NYql::TIssues issues = NYql::TIssues()) {
        Send(new IEventHandle(
            CheckpointCoordinator,
            StorageProxy,
            new TEvCheckpointStorage::TEvGetCheckpointsMetadataResponse(TVector<TCheckpointMetadata>(), std::move(issues))));
    }

    void MockCreateCheckpointResponse(TCheckpointId& checkpointId, NYql::TIssues issues = NYql::TIssues()) {
        Send(new IEventHandle(
            CheckpointCoordinator,
            StorageProxy,
            new TEvCheckpointStorage::TEvCreateCheckpointResponse(checkpointId, std::move(issues), "42")));
    }

    void MockNodeStateSavedEvent(TCheckpointId& checkpointId, TActorId& sender) {
        auto ev = std::make_unique<NYql::NDq::TEvDqCompute::TEvSaveTaskStateResult>();
        ev->Record.MutableCheckpoint()->SetGeneration(checkpointId.CoordinatorGeneration);
        ev->Record.MutableCheckpoint()->SetId(checkpointId.SeqNo);
        ev->Record.SetStatus(NYql::NDqProto::TEvSaveTaskStateResult::OK);
        ev->Record.SetStateSizeBytes(100);
        Send(new IEventHandle(
            CheckpointCoordinator,
            sender,
            ev.release()));
    }

    void MockNodeStateSaveFailedEvent(TCheckpointId& checkpointId, TActorId& sender) {
        auto ev = std::make_unique<NYql::NDq::TEvDqCompute::TEvSaveTaskStateResult>();
        ev->Record.MutableCheckpoint()->SetGeneration(checkpointId.CoordinatorGeneration);
        ev->Record.MutableCheckpoint()->SetId(checkpointId.SeqNo);
        ev->Record.SetStatus(NYql::NDqProto::TEvSaveTaskStateResult::STORAGE_ERROR);
        Send(new IEventHandle(
            CheckpointCoordinator,
            sender,
            ev.release()));
    }

    void MockSetCheckpointPendingCommitStatusResponse(TCheckpointId& checkpointId, NYql::TIssues issues = NYql::TIssues()) {
        Send(new IEventHandle(
            CheckpointCoordinator,
            StorageProxy,
            new TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusResponse(checkpointId, std::move(issues))));
    }

    void MockChangesCommittedEvent(TCheckpointId& checkpointId, TActorId& sender) {
        Send(new IEventHandle(
            CheckpointCoordinator,
            sender,
            new NYql::NDq::TEvDqCompute::TEvStateCommitted(checkpointId.SeqNo, checkpointId.CoordinatorGeneration, ActorToTask[sender])));
    }

    void MockCompleteCheckpointResponse(TCheckpointId& checkpointId, NYql::TIssues issues = NYql::TIssues()) {
        Send(new IEventHandle(
            CheckpointCoordinator,
            StorageProxy,
            new TEvCheckpointStorage::TEvCompleteCheckpointResponse(checkpointId, std::move(issues))));
    }
};
} // namespace

namespace NFq {

Y_UNIT_TEST_SUITE(TCheckpointCoordinatorTests) {

    class CheckpointsTestHelper : public TTestBootstrap
    {
    public:
        CheckpointsTestHelper(ui64 graphFlags, const TString& sourceType)
            : TTestBootstrap(graphFlags, sourceType) {
        }
        
        void InjectCheckpoint() {
            Cerr << "Waiting for TEvRegisterCoordinatorRequest (storage)" << Endl;
            ExpectEvent(StorageProxy, TEvCheckpointStorage::TEvRegisterCoordinatorRequest(CoordinatorId));

            MockRegisterCoordinatorResponseEvent();

            ExpectEvent(IngressActor, NYql::NDq::TEvDqCompute::TEvNewCheckpointCoordinator(CoordinatorId.Generation, CoordinatorId.GraphId));
            ExpectEvent(MapActor, NYql::NDq::TEvDqCompute::TEvNewCheckpointCoordinator(CoordinatorId.Generation, CoordinatorId.GraphId));
            ExpectEvent(EgressActor, NYql::NDq::TEvDqCompute::TEvNewCheckpointCoordinator(CoordinatorId.Generation, CoordinatorId.GraphId));

            MockNewCheckpointCoordinatorAckEvent(IngressActor);
            MockNewCheckpointCoordinatorAckEvent(MapActor);
            MockNewCheckpointCoordinatorAckEvent(EgressActor);

            Cerr << "Waiting for TEvGetCheckpointsMetadataRequest (storage)" << Endl;
            ExpectEvent(StorageProxy, 
                TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest(
                    CoordinatorId.GraphId, {ECheckpointStatus::PendingCommit, ECheckpointStatus::Completed}, 1, false
                ));

            MockCheckpointsMetadataResponse();

            Cerr << "Waiting for TEvCreateCheckpointRequest (storage)" << Endl;
            NProto::TCheckpointGraphDescription graphDesc;
            graphDesc.MutableGraph()->CopyFrom(NProto::TGraphParams());
            ExpectEvent(StorageProxy, 
                TEvCheckpointStorage::TEvCreateCheckpointRequest(CoordinatorId, CheckpointId, 3, graphDesc
                ));

            MockCreateCheckpointResponse(CheckpointId);

            Cerr << "Waiting for TEvInjectCheckpointBarrier (ingress)" << Endl;

            ExpectEvent(IngressActor, 
                NYql::NDq::TEvDqCompute::TEvInjectCheckpoint(CheckpointId.SeqNo, CheckpointId.CoordinatorGeneration
                ));
            ExpectRun();
        }

        void AllSavedAndCommited() {
            MockNodeStateSavedEvent(CheckpointId, IngressActor);
            MockNodeStateSavedEvent(CheckpointId, MapActor);
            MockNodeStateSavedEvent(CheckpointId, EgressActor);

            Cerr << "Waiting for TEvSetCheckpointPendingCommitStatusRequest (storage)" << Endl;
            ExpectEvent(StorageProxy, 
                TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest(CoordinatorId, CheckpointId, 300
                ));

            MockSetCheckpointPendingCommitStatusResponse(CheckpointId);

            Cerr << "Waiting for TEvCommitChanges (ingress)" << Endl;
            ExpectEvent(IngressActor, 
                NYql::NDq::TEvDqCompute::TEvCommitState(
                    CheckpointId.SeqNo,
                    CheckpointId.CoordinatorGeneration,
                    CoordinatorId.Generation
                ));

            Cerr << "Waiting for TEvCommitChanges (egress)" << Endl;
            ExpectEvent(EgressActor, 
                NYql::NDq::TEvDqCompute::TEvCommitState(
                    CheckpointId.SeqNo,
                    CheckpointId.CoordinatorGeneration,
                    CoordinatorId.Generation
                ));

            MockChangesCommittedEvent(CheckpointId, IngressActor);
            MockChangesCommittedEvent(CheckpointId, EgressActor);

            Cerr << "Waiting for TEvCompleteCheckpointRequest (storage)" << Endl;
            ExpectEvent(StorageProxy, 
                TEvCheckpointStorage::TEvCompleteCheckpointRequest(CoordinatorId, CheckpointId, 300));

            MockCompleteCheckpointResponse(CheckpointId);
        }

        void SaveFailed() {
            MockNodeStateSavedEvent(CheckpointId, IngressActor);
            MockNodeStateSaveFailedEvent(CheckpointId, EgressActor);

            Cerr << "Waiting for TEvAbortCheckpointRequest (storage)" << Endl;
            ExpectEvent(StorageProxy, 
               TEvCheckpointStorage::TEvAbortCheckpointRequest( CoordinatorId, CheckpointId, "Can't save node state"));
        }

        void ExpectRun() {
            ExpectEvent(IngressActor, NYql::NDq::TEvDqCompute::TEvRun());
            ExpectEvent(MapActor, NYql::NDq::TEvDqCompute::TEvRun());
            ExpectEvent(EgressActor, NYql::NDq::TEvDqCompute::TEvRun());
        }
    };

    Y_UNIT_TEST(ShouldTriggerCheckpointWithSource) {
        CheckpointsTestHelper test(ETestGraphFlags::InputWithSource, "PqSource");
        test.InjectCheckpoint();
        test.AllSavedAndCommited();
    }

    Y_UNIT_TEST(ShouldTriggerCheckpointWithSourcesAndWithChannel) {
        CheckpointsTestHelper test(ETestGraphFlags::InputWithSource | ETestGraphFlags::SourceWithChannelInOneTask, "PqSource");
        test.InjectCheckpoint();
        test.AllSavedAndCommited();
    }

    Y_UNIT_TEST(ShouldAbortPreviousCheckpointsIfNodeStateCantBeSaved) {
        CheckpointsTestHelper test(ETestGraphFlags::InputWithSource, "PqSource");
        test.InjectCheckpoint();
        test.SaveFailed();
    }

    Y_UNIT_TEST(ShouldDoNothingIfNoIngressTasks) {
        CheckpointsTestHelper test(ETestGraphFlags::InputWithSource, "S3Source");
        test.ExpectRun();
        bool empty = false;   
        try {
            test.GrabEdgeEvent<TEvCheckpointStorage::TEvRegisterCoordinatorRequest>(test.StorageProxy, TDuration::Seconds(10));
        } catch (TEmptyEventQueueException&) {
            empty = true;
        }
        UNIT_ASSERT(empty);
    }
}

} // namespace NFq
