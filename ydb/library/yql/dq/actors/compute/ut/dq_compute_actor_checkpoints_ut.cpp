#include <ydb/library/yql/dq/actors/compute/dq_checkpoints.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_checkpoints.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {

namespace {

using namespace NActors;
using namespace NDqProto::NDqStateLoadPlan;

struct TRestoreFixture : TDqComputeActorCheckpoints::ICallbacks {
    struct TRuntime : TTestActorRuntimeBase {
        TRuntime() {
            InitNodes();
            AppendToLogSettings(NKikimrServices::EServiceKikimr_MIN, NKikimrServices::EServiceKikimr_MAX,
                NKikimrServices::EServiceKikimr_Name<NLog::EComponent>);
        }
    } Runtime;

    const TActorId Coordinator = Runtime.AllocateEdgeActor();
    const TActorId Storage = Runtime.AllocateEdgeActor();
    TActorId CheckpointsId;
    TDqComputeActorCheckpoints* Checkpoints = nullptr;
    TMaybe<TComputeActorState> LoadedState;
    bool Stopped = false;

    TRestoreFixture() {
        Runtime.RegisterService(MakeCheckpointStorageID(), Storage);
        NDqProto::TDqTask task;
        task.SetId(42);
        Checkpoints = new TDqComputeActorCheckpoints(Coordinator, ui64{1}, TDqTaskSettings(&task), this);
        CheckpointsId = Runtime.Register(Checkpoints);
        Checkpoints->Init(CheckpointsId, CheckpointsId);
        Runtime.Send(new IEventHandle(CheckpointsId, Coordinator,
            new TEvDqCompute::TEvNewCheckpointCoordinator(2, "graph")));
        Runtime.GrabEdgeEvent<TEvDqCompute::TEvNewCheckpointCoordinatorAck>(Coordinator);
    }

    bool ReadyToCheckpoint() const override { return true; }
    void SaveState(const NDqProto::TCheckpoint&, TComputeActorState&) const override {}
    void CommitState(const NDqProto::TCheckpoint&) override {}
    void InjectBarrierToOutputs(const NDqProto::TCheckpoint&) override {}
    void ResumeInputsByCheckpoint() override {}
    TString GetTaskDebugState() const override { return {}; }
    void Start() override {}
    void Stop() override { Stopped = true; }
    void ResumeExecution(EResumeSource) override {}

    void LoadState(TComputeActorState&& state, const NDqProto::TCheckpoint& checkpoint) override {
        UNIT_ASSERT(Stopped);
        UNIT_ASSERT(!LoadedState);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint.GetId(), 7);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint.GetGeneration(), 1);
        LoadedState = std::move(state);
        Checkpoints->AfterStateLoading({});
    }

    void Restore(const TTaskPlan& plan, ui64 cookie = 0) {
        TTaskPlan transportedPlan;
        UNIT_ASSERT(transportedPlan.ParseFromString(plan.SerializeAsString()));
        Runtime.Send(new IEventHandle(CheckpointsId, Coordinator,
            new TEvDqCompute::TEvRestoreFromCheckpoint(7, 1, 2, transportedPlan), 0, cookie));
    }

    void CheckRestored(ui64 cookie = 0) {
        const auto result = Runtime.GrabEdgeEvent<TEvDqCompute::TEvRestoreFromCheckpointResult>(
            Coordinator, TDuration::Seconds(5));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Cookie, cookie);
        UNIT_ASSERT(result->Get()->Record.GetStatus() == NDqProto::TEvRestoreFromCheckpointResult::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetTaskId(), 42);
        UNIT_ASSERT(LoadedState);
        UNIT_ASSERT(LoadedState->MiniKqlProgram);
        UNIT_ASSERT_VALUES_EQUAL(LoadedState->MiniKqlProgram->Data.Version,
            static_cast<ui64>(TDqComputeActorCheckpoints::ComputeActorCurrentStateVersion));
        UNIT_ASSERT(LoadedState->Sinks.empty());
    }
};

TTaskPlan MakeForeignPlan() {
    TTaskPlan plan;
    plan.SetStateType(STATE_TYPE_FOREIGN);
    plan.MutableProgram()->SetStateType(STATE_TYPE_EMPTY);
    plan.AddSinks()->SetStateType(STATE_TYPE_EMPTY);
    return plan;
}

void AddForeignSource(TSourcePlan& source, ui64 taskId, ui64 inputIndex) {
    auto& foreign = *source.AddForeignTasksSources();
    foreign.SetTaskId(taskId);
    foreign.SetInputIndex(inputIndex);
}

void CheckCheckpointSources(bool explicitState) {
    TRestoreFixture fixture;
    auto plan = MakeForeignPlan();
    auto& first = *plan.AddSources();
    first.SetInputIndex(4);
    first.SetStateType(explicitState ? STATE_TYPE_FOREIGN : STATE_TYPE_EMPTY);
    if (explicitState) {
        plan.MutableProgram()->SetStateType(STATE_TYPE_FOREIGN);
        plan.MutableProgram()->SetState("program");
        first.SetState("explicit source");
        first.SetStateVersion(3);
        AddForeignSource(first, 99, 0); // Explicit state overrides this mapping.
    }
    auto& second = *plan.AddSources();
    second.SetInputIndex(6);
    second.SetStateType(STATE_TYPE_FOREIGN);
    AddForeignSource(second, 20, 2);
    AddForeignSource(second, 20, 3);
    fixture.Restore(plan);

    const auto request = fixture.Runtime.GrabEdgeEvent<TEvDqCompute::TEvGetTaskState>(
        fixture.Storage, TDuration::Seconds(5));
    UNIT_ASSERT(request);
    UNIT_ASSERT(!fixture.LoadedState);
    UNIT_ASSERT_VALUES_EQUAL(request->Get()->TaskIds.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(request->Get()->TaskIds.front(), 20);
    auto response = std::make_unique<TEvDqCompute::TEvGetTaskStateResult>(request->Get()->Checkpoint, TIssues{}, 2);
    auto& saved = response->States.emplace_back();
    saved.MiniKqlProgram.ConstructInPlace().Data.Blob = "old program must not be restored";
    for (const auto inputIndex : {2, 3}) {
        auto& source = saved.Sources.emplace_back();
        source.InputIndex = inputIndex;
        source.Data.emplace_back(ToString(inputIndex), 1);
        source.Data.emplace_back("another partition", 2);
    }
    fixture.Runtime.Send(new IEventHandle(fixture.CheckpointsId, fixture.Storage, response.release()));
    fixture.CheckRestored();

    const auto& restored = *fixture.LoadedState;
    UNIT_ASSERT_VALUES_EQUAL(restored.MiniKqlProgram->Data.Blob, explicitState ? "program" : "");
    UNIT_ASSERT_VALUES_EQUAL(restored.Sources.size(), explicitState ? 2 : 1);
    if (explicitState) {
        const auto& source = restored.Sources.front();
        UNIT_ASSERT_VALUES_EQUAL(source.InputIndex, 4);
        UNIT_ASSERT_VALUES_EQUAL(source.DataSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(source.Data.front().Blob, "explicit source");
        UNIT_ASSERT_VALUES_EQUAL(source.Data.front().Version, 3);
    }
    const auto& source = restored.Sources.back();
    UNIT_ASSERT_VALUES_EQUAL(source.InputIndex, 6);
    UNIT_ASSERT_VALUES_EQUAL(source.DataSize(), 4);
    auto data = source.Data.begin();
    for (const auto inputIndex : {2, 3}) {
        UNIT_ASSERT_VALUES_EQUAL(data->Blob, ToString(inputIndex));
        UNIT_ASSERT_VALUES_EQUAL(data->Version, 1);
        ++data;
        UNIT_ASSERT_VALUES_EQUAL(data->Blob, "another partition");
        UNIT_ASSERT_VALUES_EQUAL(data->Version, 2);
        ++data;
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TComputeActorStateRestore) {
    Y_UNIT_TEST(ExplicitStateDoesNotReadCheckpoints) {
        for (const TString& blob : {TString("state"), TString()}) {
            TRestoreFixture fixture;
            auto noReads = fixture.Runtime.AddObserver<TEvDqCompute::TEvGetTaskState>([](auto&) {
                UNIT_FAIL("Explicit state must not read checkpoints");
            });
            auto plan = MakeForeignPlan();
            plan.MutableProgram()->SetStateType(STATE_TYPE_FOREIGN);
            plan.MutableProgram()->SetState(blob);
            auto& source = *plan.AddSources();
            source.SetStateType(STATE_TYPE_FOREIGN);
            source.SetInputIndex(5);
            source.SetState(blob);
            source.SetStateVersion(3);
            AddForeignSource(source, 99, 0);
            fixture.Restore(plan, 123);
            fixture.CheckRestored(123);
            const auto& state = *fixture.LoadedState;
            UNIT_ASSERT_VALUES_EQUAL(state.MiniKqlProgram->Data.Blob, blob);
            UNIT_ASSERT_VALUES_EQUAL(state.MiniKqlProgram->RuntimeVersion, static_cast<ui64>(NDqProto::RUNTIME_VERSION_YQL_1_0));
            UNIT_ASSERT_VALUES_EQUAL(state.Sources.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(state.Sources.front().InputIndex, 5);
            UNIT_ASSERT_VALUES_EQUAL(state.Sources.front().DataSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(state.Sources.front().Data.front().Blob, blob);
            UNIT_ASSERT_VALUES_EQUAL(state.Sources.front().Data.front().Version, 3);
        }
    }

    Y_UNIT_TEST(ExplicitProgramWithoutSourcesDoesNotReadCheckpoints) {
        TRestoreFixture fixture;
        auto noReads = fixture.Runtime.AddObserver<TEvDqCompute::TEvGetTaskState>([](auto&) {
            UNIT_FAIL("Explicit program must not read checkpoints");
        });
        auto plan = MakeForeignPlan();
        plan.MutableProgram()->SetStateType(STATE_TYPE_FOREIGN);
        plan.MutableProgram()->SetState("program");
        fixture.Restore(plan);
        fixture.CheckRestored();
        UNIT_ASSERT_VALUES_EQUAL(fixture.LoadedState->MiniKqlProgram->Data.Blob, "program");
        UNIT_ASSERT(fixture.LoadedState->Sources.empty());
    }

    Y_UNIT_TEST(MixedExplicitAndCheckpointState) {
        CheckCheckpointSources(true);
    }

    Y_UNIT_TEST(CheckpointStateWithoutExplicitOverrides) {
        CheckCheckpointSources(false);
    }
}

} // namespace NYql::NDq
