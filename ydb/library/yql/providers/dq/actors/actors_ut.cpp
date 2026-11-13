#include "actor_helpers.h"
#include "graph_execution_events_actor.h"
#include "full_result_writer.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>

#include <yql/essentials/minikql/mkql_node_serialization.h>

#include <util/system/event.h>

#include <atomic>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>


#include <ydb/library/yql/providers/dq/actors/events.h>

#include "result_receiver.h"
#include "result_actor_base.h"

using namespace NActors;
using namespace NYql;
using namespace NYql::NDqs;

Y_UNIT_TEST_SUITE(ResultReceiver) {

auto ResultReceiver(TActorId executerId = {}) {
    TDqConfiguration::TPtr settings = new TDqConfiguration();
    auto receiver = MakeResultReceiver(
        {}, // columns,
        executerId,
        "traceId",
        settings,
        {}, // secureParams
        "", // resultType ?
        {}, // graphExecutionEventsId ?
        false // discard
    );

    return receiver;
}

Y_UNIT_TEST(ReceiveStatus) {
    TTestActorRuntimeBase runtime;
    runtime.Initialize();

    auto sender = runtime.AllocateEdgeActor();
    auto receiverId = runtime.Register(ResultReceiver().Release());
    runtime.Send(new IEventHandle(receiverId, sender, new TEvReadyState(), 0, true));
}

Y_UNIT_TEST(ReceiveError) {
    TTestActorRuntimeBase runtime;
    runtime.Initialize();

    auto executerId = runtime.AllocateEdgeActor();
    auto receiverId = runtime.Register(ResultReceiver(executerId).Release());
    runtime.Send(new IEventHandle(receiverId, {}, new NActors::TEvents::TEvUndelivered(0,0), 0, true));

    auto response = runtime.GrabEdgeEvent<TEvDqFailure>();
    UNIT_ASSERT_EQUAL(response->Record.GetStatusCode(), NYql::NDqProto::StatusIds::UNAVAILABLE);
}

Y_UNIT_TEST(WriteQueue) {
    NYql::NDqs::NExecutionHelpers::TWriteQueue q;
    UNIT_ASSERT(q.empty());

    NYql::NDqs::NExecutionHelpers::TQueueItem item({}, ""); item.Size = 1000;
    q.emplace(item);
    UNIT_ASSERT_EQUAL(q.ByteSize, 1000);

    item.Size = 11;
    q.emplace(item);
    UNIT_ASSERT_EQUAL(q.ByteSize, 1011);

    q.pop();
    UNIT_ASSERT_EQUAL(q.ByteSize, 11);

    q.pop();
    UNIT_ASSERT_EQUAL(q.ByteSize, 0);
}

} // Y_UNIT_TEST_SUITE(ResultReceiver)

namespace {

struct TBlockingPreprocessorState {
    std::atomic<unsigned> Entered{0};
    TManualEvent AllEntered;
    TManualEvent Release;
    TManualEvent MainResponsive;
    TManualEvent AllReplies;
    TManualEvent WritersGone;

    void Block() {
        if (++Entered == 2) {
            AllEntered.Signal();
        }
        Release.WaitI();
    }
};

class TBlockingPreprocessor : public IDqTaskPreprocessor {
public:
    explicit TBlockingPreprocessor(TBlockingPreprocessorState& state)
        : State(state)
    {
    }

    THashMap<TString, TString> GetTaskParams(const THashMap<TString, TString>&, const THashMap<TString, TString>&) override {
        State.Block();
        return {};
    }

    void Finish(bool) override {
        State.Block();
    }

    THolder<IDqFullResultWriter> CreateFullResultWriter() override {
        State.Block();
        return {};
    }

private:
    TBlockingPreprocessorState& State;
};

class TMainPoolProbe : public TActor<TMainPoolProbe> {
public:
    explicit TMainPoolProbe(TBlockingPreprocessorState& state, unsigned expectedReplies = 2)
        : TActor(&TMainPoolProbe::Handler)
        , State(state)
        , ExpectedReplies(expectedReplies)
    {
    }

private:
    TBlockingPreprocessorState& State;
    const unsigned ExpectedReplies;
    unsigned Replies = 0;
    unsigned Gone = 0;

    STRICT_STFUNC(Handler, {
        HFunc(TEvGraphExecutionEvent, OnReply);
        cFunc(TEvents::TEvWakeup::EventType, OnWakeup);
        cFunc(TEvents::TEvGone::EventType, OnGone);
        cFunc(TEvDqFailure::EventType, OnWriterFinished);
    })

    void OnReply(TEvGraphExecutionEvent::TPtr&, const TActorContext&) {
        if (++Replies == ExpectedReplies) {
            State.AllReplies.Signal();
        }
    }

    void OnGone() {
        if (++Gone == 2) {
            State.WritersGone.Signal();
        }
    }

    void OnWriterFinished() {
    }

    void OnWakeup() {
        State.MainResponsive.Signal();
    }
};

THolder<TActorSystemSetup> MakePoolIsolationSetup() {
    auto setup = MakeHolder<TActorSystemSetup>();
    setup->NodeId = 1;
    setup->ExecutorsCount = EExecutorPoolType::TotalCount;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[setup->ExecutorsCount]);
    setup->Executors[EExecutorPoolType::Main] = new TBasicExecutorPool(EExecutorPoolType::Main, 1, 10, "main");
    setup->Executors[EExecutorPoolType::GraphEvents] = new TBasicExecutorPool(EExecutorPoolType::GraphEvents, 2, 10, "blocking");
    setup->Executors[EExecutorPoolType::FullResultWriter] = new TBasicExecutorPool(EExecutorPoolType::FullResultWriter, 2, 10, "writers");
    setup->Scheduler = new TBasicSchedulerThread;
    return setup;
}

void CheckBlockingPreprocessorIsolation(NYql::NDqProto::EGraphExecutionEventType eventType) {
    TBlockingPreprocessorState state;
    auto setup = MakePoolIsolationSetup();
    TActorSystem actorSystem(setup);
    actorSystem.Start();

    const auto probe = actorSystem.Register(new TMainPoolProbe(state), TMailboxType::HTSwap, EExecutorPoolType::Main);
    for (unsigned i = 0; i < 2; ++i) {
        std::vector<IDqTaskPreprocessor::TPtr> preprocessors{new TBlockingPreprocessor(state)};
        const auto graph = RegisterGraphExecutionEventsActor(actorSystem, "blocking-pool-test", std::move(preprocessors));
        auto event = MakeHolder<TEvGraphExecutionEvent>();
        event->Record.SetEventType(eventType);
        if (eventType == NYql::NDqProto::START) {
            event->Record.MutableMessage()->PackFrom(NYql::NDqProto::TGraphExecutionEvent::TExecuteGraphDescriptor{});
        } else if (eventType == NYql::NDqProto::FULL_RESULT) {
            event->Record.MutableMessage()->PackFrom(NYql::NDqProto::TGraphExecutionEvent::TFullResultDescriptor{});
        }
        actorSystem.Send(new IEventHandle(graph, probe, event.Release()));
    }

    const bool allBlocked = state.AllEntered.WaitT(TDuration::Seconds(5));
    actorSystem.Send(new IEventHandle(probe, {}, new TEvents::TEvWakeup));
    const bool mainResponsive = state.MainResponsive.WaitT(TDuration::Seconds(5));
    // Release workers before assertions or shutdown, including when registration regresses.
    state.Release.Signal();
    const bool allReplied = state.AllReplies.WaitT(TDuration::Seconds(5));
    actorSystem.Stop();

    UNIT_ASSERT_C(allBlocked, "Both blocking-pool threads must enter the synchronous preprocessor");
    UNIT_ASSERT_C(mainResponsive, "Main pool must process events while the blocking pool is saturated");
    UNIT_ASSERT_C(allReplied, "Graph actors must reply after synchronous calls complete");
}

class TBlockingFullResultWriter : public IDqFullResultWriter {
public:
    TBlockingFullResultWriter(TBlockingPreprocessorState& state, bool blockFinish)
        : State(state)
        , BlockFinish(blockFinish)
    {
    }

    void AddRow(const NUdf::TUnboxedValuePod&) override {
    }

    void Finish() override {
        if (BlockFinish) {
            State.Block();
        }
    }

    void Abort() override {
        if (!BlockFinish) {
            State.Block();
        }
    }

    ui64 GetRowCount() const override {
        return 0;
    }

private:
    TBlockingPreprocessorState& State;
    const bool BlockFinish;
};

void CheckFullResultWriterIsolation(bool blockFinish) {
    TBlockingPreprocessorState state;
    auto setup = MakePoolIsolationSetup();
    TActorSystem actorSystem(setup);
    actorSystem.Start();
    const auto probe = actorSystem.Register(new TMainPoolProbe(state, 3), TMailboxType::HTSwap, EExecutorPoolType::Main);
    TString resultType;
    {
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NKikimr::NMiniKQL::TTypeEnvironment env(alloc);
        resultType = NKikimr::NMiniKQL::SerializeNode(NKikimr::NMiniKQL::TStructType::Create(0, nullptr, env), env);
    }
    for (unsigned i = 0; i < 2; ++i) {
        auto writer = MakeFullResultWriterActor("writer-pool-test", resultType, MakeHolder<TBlockingFullResultWriter>(state, blockFinish), probe);
        const auto writerId = actorSystem.Register(writer.Release(), TMailboxType::HTSwap, EExecutorPoolType::FullResultWriter);
        if (blockFinish) {
            auto event = MakeHolder<TEvFullResultWriterWriteRequest>();
            event->Record.SetFinish(true);
            actorSystem.Send(new IEventHandle(writerId, probe, event.Release()));
        } else {
            actorSystem.Send(new IEventHandle(writerId, probe, new TEvents::TEvPoison));
        }
    }
    const bool allBlocked = state.AllEntered.WaitT(TDuration::Seconds(5));
    const auto graph = RegisterGraphExecutionEventsActor(actorSystem, "independent-graph", {});
    for (auto eventType : {NDqProto::START, NDqProto::SUCCESS, NDqProto::FAIL}) {
        auto event = MakeHolder<TEvGraphExecutionEvent>();
        event->Record.SetEventType(eventType);
        if (eventType == NDqProto::START) {
            event->Record.MutableMessage()->PackFrom(NDqProto::TGraphExecutionEvent::TExecuteGraphDescriptor{});
        }
        actorSystem.Send(new IEventHandle(graph, probe, event.Release()));
    }
    const bool graphResponsive = state.AllReplies.WaitT(TDuration::Seconds(5));
    state.Release.Signal();
    const bool writersGone = state.WritersGone.WaitT(TDuration::Seconds(5));
    actorSystem.Stop();

    UNIT_ASSERT_C(allBlocked, "Full-result writers must saturate their own pool");
    UNIT_ASSERT_C(graphResponsive, "START, SUCCESS and FAIL must complete while the full-result writer pool is saturated");
    UNIT_ASSERT_C(writersGone, "Writers must finish after their synchronous calls are released");
}

} // namespace

Y_UNIT_TEST_SUITE(GraphExecutionPool) {
    Y_UNIT_TEST(BlockingWriterFinishDoesNotStarveGraphEvents) {
        CheckFullResultWriterIsolation(true);
    }

    Y_UNIT_TEST(BlockingWriterAbortDoesNotStarveGraphEvents) {
        CheckFullResultWriterIsolation(false);
    }

    Y_UNIT_TEST(BlockingStartDoesNotStarveMainPool) {
        CheckBlockingPreprocessorIsolation(NYql::NDqProto::START);
    }

    Y_UNIT_TEST(BlockingCommitDoesNotStarveMainPool) {
        CheckBlockingPreprocessorIsolation(NYql::NDqProto::SUCCESS);
    }

    Y_UNIT_TEST(BlockingAbortDoesNotStarveMainPool) {
        CheckBlockingPreprocessorIsolation(NYql::NDqProto::FAIL);
    }

    Y_UNIT_TEST(BlockingFullResultPreparationDoesNotStarveMainPool) {
        CheckBlockingPreprocessorIsolation(NYql::NDqProto::FULL_RESULT);
    }
}
