#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_checkpoints.h>
#include <ydb/library/yql/dq/actors/dq.h>

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
    const TIntrusivePtr<TCheckpointContext> CheckpointContext = MakeIntrusive<TCheckpointContext>();
    TActorId CheckpointsId;
    TDqComputeActorCheckpoints* Checkpoints = nullptr;
    TMaybe<TComputeActorState> LoadedState;
    TMaybe<TString> LoadError;
    bool Stopped = false;

    explicit TRestoreFixture(bool withSink = false) {
        Runtime.RegisterService(MakeCheckpointStorageID(), Storage);
        NDqProto::TDqTask task;
        task.SetId(42);
        if (withSink) {
            task.AddOutputs()->MutableSink()->SetType("MockSink");
        }
        Checkpoints = new TDqComputeActorCheckpoints(Coordinator, ui64{1}, TDqTaskSettings(&task), this, CheckpointContext);
        UNIT_ASSERT_VALUES_EQUAL(CheckpointContext.Get(), Checkpoints->GetCheckpointContext().Get());
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
        Checkpoints->AfterStateLoading(LoadError);
    }

    void Restore(const TTaskPlan& plan, ui64 cookie = 0) {
        TTaskPlan transportedPlan;
        UNIT_ASSERT(transportedPlan.ParseFromString(plan.SerializeAsString()));
        Runtime.Send(new IEventHandle(CheckpointsId, Coordinator,
            new TEvDqCompute::TEvRestoreFromCheckpoint(7, 1, 2, transportedPlan), 0, cookie));
    }

    void CheckRestoreResult(ui64 cookie = 0, ui64 version = TDqComputeActorCheckpoints::ComputeActorCurrentStateVersion) {
        const auto result = Runtime.GrabEdgeEvent<TEvDqCompute::TEvRestoreFromCheckpointResult>(
            Coordinator, TDuration::Seconds(5));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Cookie, cookie);
        const auto expectedStatus = LoadError.Defined()
            ? NDqProto::TEvRestoreFromCheckpointResult::INTERNAL_ERROR
            : NDqProto::TEvRestoreFromCheckpointResult::OK;
        UNIT_ASSERT(result->Get()->Record.GetStatus() == expectedStatus);
        if (LoadError) {
            UNIT_ASSERT_STRING_CONTAINS(IssuesFromMessageAsString(result->Get()->Record.GetIssues()), *LoadError);
        }
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetTaskId(), 42);
        UNIT_ASSERT(LoadedState);
        UNIT_ASSERT(LoadedState->MiniKqlProgram);
        UNIT_ASSERT_VALUES_EQUAL(LoadedState->MiniKqlProgram->Data.Version, version);
        UNIT_ASSERT(LoadedState->Sinks.empty());

        // The transported plan has its own buffers. After loading, the actor must
        // not keep references to them alongside the state handed to the runner.
        const auto checkReleased = [](const TString& blob) {
            if (blob) {
                UNIT_ASSERT_C(blob.IsDetached(), "The restore plan still retains a state blob");
            }
        };
        checkReleased(LoadedState->MiniKqlProgram->Data.Blob);
        for (const auto& source : LoadedState->Sources) {
            for (const auto& data : source.Data) {
                checkReleased(data.Blob);
            }
        }
    }
};

struct TCheckpointContextFixture : TRestoreFixture {
    using TRestoreFixture::TRestoreFixture;

    mutable TMaybe<NDqProto::TCheckpoint> SavingCheckpoint;
    bool FailSave = false;
    ui32 CommitCalls = 0;
    ui32 CommitWakeups = 0;

    void SaveState(const NDqProto::TCheckpoint& checkpoint, TComputeActorState&) const override {
        SavingCheckpoint = CheckpointContext->PendingSaveCheckpoint;
        UNIT_ASSERT(SavingCheckpoint);
        UNIT_ASSERT_VALUES_EQUAL(SavingCheckpoint->GetGeneration(), checkpoint.GetGeneration());
        UNIT_ASSERT_VALUES_EQUAL(SavingCheckpoint->GetId(), checkpoint.GetId());
        Y_ENSURE(!FailSave, "Cannot save checkpoint");
    }

    void CommitState(const NDqProto::TCheckpoint&) override {
        ++CommitCalls;
    }

    void ResumeExecution(EResumeSource source) override {
        if (source == EResumeSource::CheckpointCommit) {
            ++CommitWakeups;
        }
    }

    void Run(std::function<void()> callback) {
        class TCallbackActor final : public TActorBootstrapped<TCallbackActor> {
        public:
            TCallbackActor(std::function<void()> callback, const TActorId& replyTo)
                : Callback(std::move(callback))
                , ReplyTo(replyTo)
            {}

            void Bootstrap() {
                Callback();
                Send(ReplyTo, new TEvents::TEvWakeup());
                PassAway();
            }

        private:
            const std::function<void()> Callback;
            const TActorId ReplyTo;
        };

        const auto replyTo = Runtime.AllocateEdgeActor();
        Runtime.Register(new TCallbackActor(std::move(callback), replyTo));
        UNIT_ASSERT(Runtime.GrabEdgeEvent<TEvents::TEvWakeup>(replyTo, TDuration::Seconds(5)));
    }

    void Commit(ui64 id, ui64 generation = 2, ui64 coordinatorGeneration = 2) {
        Runtime.Send(new IEventHandle(CheckpointsId, Coordinator,
            new TEvDqCompute::TEvCommitState(id, generation, coordinatorGeneration)));
    }

    void ExpectCommitted(ui64 id, ui64 generation = 2) {
        const auto response = Runtime.GrabEdgeEvent<TEvDqCompute::TEvStateCommitted>(Coordinator, TDuration::Seconds(5));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetCheckpoint().GetId(), id);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetCheckpoint().GetGeneration(), generation);
    }
};

NDqProto::TCheckpoint MakeCheckpoint(ui64 id, ui64 generation = 2) {
    NDqProto::TCheckpoint checkpoint;
    checkpoint.SetId(id);
    checkpoint.SetGeneration(generation);
    checkpoint.SetType(NDqProto::CHECKPOINT_TYPE_SNAPSHOT);
    return checkpoint;
}

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

void CheckCheckpointSources(bool explicitState, bool failLoad = false) {
    TRestoreFixture fixture;
    if (failLoad) {
        fixture.LoadError = "Cannot load mixed checkpoint state";
    }
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
    fixture.CheckRestoreResult();

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

void CheckExplicitState(bool failLoad = false) {
    for (const TString& blob : {TString("state"), TString()}) {
        TRestoreFixture fixture;
        if (failLoad) {
            fixture.LoadError = "Cannot load explicit checkpoint state";
        }
        auto noReads = fixture.Runtime.AddObserver<TEvDqCompute::TEvGetTaskState>([](auto&) {
            UNIT_FAIL("Explicit state must not read checkpoints");
        });
        auto plan = MakeForeignPlan();
        plan.MutableProgram()->SetStateType(STATE_TYPE_FOREIGN);
        plan.MutableProgram()->SetState(blob);
        plan.MutableProgram()->SetForeignTaskId(88); // Explicit bytes take precedence, including an empty blob.
        auto& source = *plan.AddSources();
        source.SetStateType(STATE_TYPE_FOREIGN);
        source.SetInputIndex(5);
        source.SetState(blob);
        source.SetStateVersion(3);
        AddForeignSource(source, 99, 0);
        fixture.Restore(plan, 123);
        fixture.CheckRestoreResult(123);
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

} // anonymous namespace

Y_UNIT_TEST_SUITE(TComputeActorCheckpointContext) {
    Y_UNIT_TEST(PendingSaveIncludesSinkState) {
        TCheckpointContextFixture fixture(true);
        const auto context = fixture.CheckpointContext;
        UNIT_ASSERT(!context->PendingSaveCheckpoint);
        UNIT_ASSERT(!context->LastCommittedCheckpoint);
        const auto checkpoint = MakeCheckpoint(7);
        fixture.Run([&] {
            fixture.Checkpoints->RegisterCheckpoint(checkpoint, 1);
            fixture.Checkpoints->DoCheckpoint();
        });
        UNIT_ASSERT(context->PendingSaveCheckpoint);
        UNIT_ASSERT(fixture.SavingCheckpoint);
        UNIT_ASSERT_VALUES_EQUAL(context->PendingSaveCheckpoint->GetId(), 7);
        fixture.Run([&] {
            fixture.Checkpoints->OnSinkStateSaved({}, 0, checkpoint);
        });
        UNIT_ASSERT(fixture.Runtime.GrabEdgeEvent<TEvDqCompute::TEvSaveTaskState>(fixture.Storage, TDuration::Seconds(5)));
        UNIT_ASSERT(!context->PendingSaveCheckpoint);
        UNIT_ASSERT(!context->LastCommittedCheckpoint);
    }

    Y_UNIT_TEST(FailedSaveClearsPendingCheckpoint) {
        TCheckpointContextFixture fixture;
        fixture.FailSave = true;
        const auto context = fixture.CheckpointContext;
        fixture.Run([&] {
            fixture.Checkpoints->RegisterCheckpoint(MakeCheckpoint(7), 1);
            fixture.Checkpoints->DoCheckpoint();
        });
        const auto response = fixture.Runtime.GrabEdgeEvent<TEvDqCompute::TEvSaveTaskStateResult>(fixture.Coordinator, TDuration::Seconds(5));
        UNIT_ASSERT(response);
        UNIT_ASSERT(response->Get()->Record.GetStatus() == NDqProto::TEvSaveTaskStateResult::INTERNAL_ERROR);
        UNIT_ASSERT(fixture.SavingCheckpoint);
        UNIT_ASSERT(!context->PendingSaveCheckpoint);
    }

    Y_UNIT_TEST(CommitWaitsForSinkAndWakesExecution) {
        TCheckpointContextFixture fixture(true);
        const auto context = fixture.CheckpointContext;
        fixture.Commit(7);
        fixture.Runtime.WaitFor("sink commit request", [&] { return fixture.CommitCalls == 1; }, TDuration::Seconds(5));
        UNIT_ASSERT(!context->LastCommittedCheckpoint);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CommitWakeups, 0);
        fixture.Run([&] {
            fixture.Checkpoints->OnSinkStateCommitted(0, MakeCheckpoint(7));
        });
        fixture.ExpectCommitted(7);
        UNIT_ASSERT(context->LastCommittedCheckpoint);
        UNIT_ASSERT_VALUES_EQUAL(context->LastCommittedCheckpoint->GetGeneration(), 2);
        UNIT_ASSERT_VALUES_EQUAL(context->LastCommittedCheckpoint->GetId(), 7);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CommitWakeups, 1);
    }

    Y_UNIT_TEST(CommitWithoutSinksDoesNotRegress) {
        TCheckpointContextFixture fixture;
        const auto context = fixture.CheckpointContext;
        for (const auto id : {7, 7, 6, 8}) {
            fixture.Commit(id);
            fixture.ExpectCommitted(id);
            UNIT_ASSERT_VALUES_EQUAL(context->LastCommittedCheckpoint->GetId(), id == 8 ? 8 : 7);
        }
        // Recommitting an older checkpoint must not regress the generation either.
        fixture.Commit(100, 1);
        fixture.ExpectCommitted(100, 1);
        UNIT_ASSERT_VALUES_EQUAL(context->LastCommittedCheckpoint->GetGeneration(), 2);
        UNIT_ASSERT_VALUES_EQUAL(context->LastCommittedCheckpoint->GetId(), 8);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CommitWakeups, 2);
    }

    Y_UNIT_TEST(NewCoordinatorResetsSharedContext) {
        TCheckpointContextFixture fixture;
        const auto context = fixture.CheckpointContext;
        fixture.Commit(7);
        fixture.ExpectCommitted(7);
        fixture.Run([&] { fixture.Checkpoints->RegisterCheckpoint(MakeCheckpoint(8), 1); });
        fixture.Runtime.Send(new IEventHandle(fixture.CheckpointsId, fixture.Coordinator,
            new TEvDqCompute::TEvNewCheckpointCoordinator(3, "graph")));
        UNIT_ASSERT(fixture.Runtime.GrabEdgeEvent<TEvDqCompute::TEvNewCheckpointCoordinatorAck>(fixture.Coordinator, TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(context.Get(), fixture.Checkpoints->GetCheckpointContext().Get());
        UNIT_ASSERT(!context->PendingSaveCheckpoint);
        UNIT_ASSERT(!context->LastCommittedCheckpoint);
        fixture.Commit(100); // Stale coordinator event must not update the context.
        fixture.Commit(1, 3, 3);
        fixture.ExpectCommitted(1, 3);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CommitCalls, 2);
        UNIT_ASSERT_VALUES_EQUAL(context->LastCommittedCheckpoint->GetGeneration(), 3);
        UNIT_ASSERT_VALUES_EQUAL(context->LastCommittedCheckpoint->GetId(), 1);
    }

    Y_UNIT_TEST(RestoreDoesNotKeepCommitFromPreviousExecution) {
        TCheckpointContextFixture fixture;
        const auto context = fixture.CheckpointContext;
        fixture.Commit(9);
        fixture.ExpectCommitted(9);
        fixture.Restore(MakeForeignPlan());
        fixture.CheckRestoreResult();
        UNIT_ASSERT(!context->LastCommittedCheckpoint);
        fixture.Commit(7, 1); // Restored checkpoint keeps its original generation.
        fixture.ExpectCommitted(7, 1);
        UNIT_ASSERT_VALUES_EQUAL(context->LastCommittedCheckpoint->GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(context->LastCommittedCheckpoint->GetId(), 7);
    }

    Y_UNIT_TEST(ContextOutlivesCheckpointActor) {
        TIntrusiveConstPtr<TCheckpointContext> context;
        {
            TCheckpointContextFixture fixture;
            context = fixture.CheckpointContext;
            fixture.Commit(7);
            fixture.ExpectCommitted(7);
        }
        UNIT_ASSERT(context->LastCommittedCheckpoint);
        UNIT_ASSERT_VALUES_EQUAL(context->LastCommittedCheckpoint->GetGeneration(), 2);
        UNIT_ASSERT_VALUES_EQUAL(context->LastCommittedCheckpoint->GetId(), 7);
    }
}

Y_UNIT_TEST_SUITE(TComputeActorStateRestore) {
    Y_UNIT_TEST(ForeignProgramWithoutSourcesReadsItsTask) {
        TRestoreFixture fixture;
        auto plan = MakeForeignPlan();
        plan.MutableProgram()->SetStateType(STATE_TYPE_FOREIGN);
        plan.MutableProgram()->SetForeignTaskId(20);
        fixture.Restore(plan, 123);
        auto request = fixture.Runtime.GrabEdgeEvent<TEvDqCompute::TEvGetTaskState>(fixture.Storage);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->TaskIds, (std::vector<ui64>{20}));
        auto response = std::make_unique<TEvDqCompute::TEvGetTaskStateResult>(request->Get()->Checkpoint, TIssues{}, 2);
        auto& program = response->States.emplace_back().MiniKqlProgram.ConstructInPlace();
        program.Data = {"program only", TDqComputeActorCheckpoints::ComputeActorCurrentStateVersion};
        fixture.Runtime.Send(new IEventHandle(fixture.CheckpointsId, fixture.Storage, response.release(), 0, request->Cookie));
        fixture.CheckRestoreResult(123);
        UNIT_ASSERT_VALUES_EQUAL(fixture.LoadedState->MiniKqlProgram->Data.Blob, "program only");
        UNIT_ASSERT(fixture.LoadedState->Sources.empty());
    }

    Y_UNIT_TEST(OwnAndEmptyRestorePreserveCookies) {
        for (bool own : {false, true}) {
            TRestoreFixture fixture;
            TTaskPlan plan;
            plan.SetStateType(own ? STATE_TYPE_OWN : STATE_TYPE_EMPTY);
            fixture.Restore(plan, 789);
            if (own) {
                auto request = fixture.Runtime.GrabEdgeEvent<TEvDqCompute::TEvGetTaskState>(fixture.Storage);
                UNIT_ASSERT_VALUES_EQUAL(request->Get()->TaskIds, (std::vector<ui64>{42}));
                auto response = std::make_unique<TEvDqCompute::TEvGetTaskStateResult>(request->Get()->Checkpoint, TIssues{}, 2);
                auto& program = response->States.emplace_back().MiniKqlProgram.ConstructInPlace();
                program.Data = {"own program", TDqComputeActorCheckpoints::ComputeActorCurrentStateVersion};
                fixture.Runtime.Send(new IEventHandle(fixture.CheckpointsId, fixture.Storage, response.release(), 0, request->Cookie));
                fixture.CheckRestoreResult(789);
            } else {
                auto result = fixture.Runtime.GrabEdgeEvent<TEvDqCompute::TEvRestoreFromCheckpointResult>(fixture.Coordinator);
                UNIT_ASSERT_VALUES_EQUAL(result->Cookie, 789);
                UNIT_ASSERT(result->Get()->Record.GetStatus() == NDqProto::TEvRestoreFromCheckpointResult::OK);
                UNIT_ASSERT(!fixture.LoadedState);
            }
        }
    }

    Y_UNIT_TEST(ForeignProgramPreservesVersionsAndCombinesIndependentSources) {
        for (bool sameTask : {false, true}) {
            for (const TString& blob : {TString(), TString("serialized aggregation state")}) {
                TRestoreFixture fixture;
                auto plan = MakeForeignPlan();
                plan.MutableProgram()->SetStateType(STATE_TYPE_FOREIGN);
                plan.MutableProgram()->SetForeignTaskId(20);
                auto& source = *plan.AddSources();
                source.SetStateType(STATE_TYPE_FOREIGN);
                source.SetInputIndex(3);
                AddForeignSource(source, sameTask ? 20 : 10, 1);
                fixture.Restore(plan, 123);
                auto request = fixture.Runtime.GrabEdgeEvent<TEvDqCompute::TEvGetTaskState>(fixture.Storage);
                UNIT_ASSERT_VALUES_EQUAL(request->Cookie, 123);
                UNIT_ASSERT_VALUES_EQUAL(request->Get()->TaskIds.size(), sameTask ? 1 : 2);
                UNIT_ASSERT_VALUES_EQUAL(request->Get()->TaskIds.back(), 20);
                auto response = std::make_unique<TEvDqCompute::TEvGetTaskStateResult>(request->Get()->Checkpoint, TIssues{}, 2);
                for (auto id : request->Get()->TaskIds) {
                    auto& saved = response->States.emplace_back();
                    auto& program = saved.MiniKqlProgram.ConstructInPlace();
                    program.Data = {id == 20 ? TString(blob.data(), blob.size()) : TString("unrelated program"), 17};
                    program.RuntimeVersion = 42;
                    auto& input = saved.Sources.emplace_back();
                    input.InputIndex = 1;
                    input.Data.emplace_back("partition offsets", 3);
                    saved.Sinks.emplace_back(); // Must never be copied from the foreign task.
                }
                fixture.Runtime.Send(new IEventHandle(fixture.CheckpointsId, fixture.Storage, response.release(), 0, request->Cookie));
                fixture.CheckRestoreResult(123, 17);
                UNIT_ASSERT_VALUES_EQUAL(fixture.LoadedState->MiniKqlProgram->Data.Blob, blob);
                UNIT_ASSERT_VALUES_EQUAL(fixture.LoadedState->MiniKqlProgram->RuntimeVersion, 42);
                UNIT_ASSERT_VALUES_EQUAL(fixture.LoadedState->Sources.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(fixture.LoadedState->Sources.front().InputIndex, 3);
                UNIT_ASSERT_VALUES_EQUAL(fixture.LoadedState->Sources.front().Data.front().Blob, "partition offsets");
            }
        }
    }

    Y_UNIT_TEST(MissingForeignStateIsAnError) {
        for (ui32 missing = 0; missing < 3; ++missing) {
            TRestoreFixture fixture;
            auto plan = MakeForeignPlan();
            plan.MutableProgram()->SetStateType(STATE_TYPE_FOREIGN);
            if (missing != 0) {
                plan.MutableProgram()->SetForeignTaskId(20);
            }
            fixture.Restore(plan, 456);
            if (missing != 0) {
                auto request = fixture.Runtime.GrabEdgeEvent<TEvDqCompute::TEvGetTaskState>(fixture.Storage);
                auto response = std::make_unique<TEvDqCompute::TEvGetTaskStateResult>(request->Get()->Checkpoint, TIssues{}, 2);
                if (missing == 1) {
                    response->States.emplace_back();
                }
                fixture.Runtime.Send(new IEventHandle(fixture.CheckpointsId, fixture.Storage, response.release(), 0, request->Cookie));
            }
            if (missing == 2) {
                auto result = fixture.Runtime.GrabEdgeEvent<TEvDqCompute::TEvRestoreFromCheckpointResult>(fixture.Coordinator);
                UNIT_ASSERT_VALUES_EQUAL(result->Cookie, 456);
                UNIT_ASSERT(result->Get()->Record.GetStatus() == NDqProto::TEvRestoreFromCheckpointResult::STORAGE_ERROR);
                UNIT_ASSERT(!result->Get()->Record.GetIssues().empty());
            } else {
                auto result = fixture.Runtime.GrabEdgeEvent<TEvDq::TEvAbortExecution>(fixture.Coordinator);
                UNIT_ASSERT(result->Get()->Record.GetStatusCode() == NDqProto::StatusIds::INTERNAL_ERROR);
                UNIT_ASSERT_STRING_CONTAINS(result->Get()->GetIssues().ToOneLineString(), missing == 0
                    ? "Foreign program checkpoint requires explicit state or a task ID"
                    : "Missing program checkpoint for task 20");
            }
            UNIT_ASSERT(!fixture.LoadedState);
        }
    }

    Y_UNIT_TEST(ExplicitStateDoesNotReadCheckpoints) {
        CheckExplicitState();
    }

    Y_UNIT_TEST(ExplicitStateReleasedAfterLoadError) {
        CheckExplicitState(true);
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
        fixture.CheckRestoreResult();
        UNIT_ASSERT_VALUES_EQUAL(fixture.LoadedState->MiniKqlProgram->Data.Blob, "program");
        UNIT_ASSERT(fixture.LoadedState->Sources.empty());
    }

    Y_UNIT_TEST(MixedExplicitAndCheckpointState) {
        CheckCheckpointSources(true);
    }

    Y_UNIT_TEST(MixedStateReleasedAfterLoadError) {
        CheckCheckpointSources(true, true);
    }

    Y_UNIT_TEST(CheckpointStateWithoutExplicitOverrides) {
        CheckCheckpointSources(false);
    }
}

} // namespace NYql::NDq
