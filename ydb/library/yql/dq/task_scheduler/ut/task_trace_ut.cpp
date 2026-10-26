#include <ydb/library/yql/dq/task_scheduler/task_trace.h>
#include <ydb/library/yql/dq/task_scheduler/task_scheduler.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <thread>

using namespace NYql::NDq;

namespace {
NJson::TJsonValue Parse(TStringBuf text) {
    NJson::TJsonValue value;
    UNIT_ASSERT(NJson::ReadJsonTree(text, &value, true));
    return value;
}
NJson::TJsonValue Args() { return NJson::TJsonValue(NJson::JSON_MAP); }
struct TTestRuntime : NActors::TTestActorRuntimeBase {
    TTestRuntime() : NActors::TTestActorRuntimeBase(1, true) { InitNodes(); }
    ~TTestRuntime() override { CleanupNodes(); }
};
}

Y_UNIT_TEST_SUITE(DqTaskTrace) {
    Y_UNIT_TEST(RunnerIntervalContainsPassiveSamplesAndProgress) {
        TQueryTaskTrace trace({}, 7, {.Enabled = true});
        TTaskChannelSnapshot input;
        input.BufferedBytes = 120;
        input.Empty = false;
        input.Finished = false;
        TTaskChannelSnapshot output;
        output.FillLevel = 2;
        NJson::TJsonValue before(NJson::JSON_ARRAY), after(NJson::JSON_ARRAY);
        before.AppendValue(input.ToJson(42, true));
        after.AppendValue(output.ToJson(43, false));
        const auto start = TMonotonic::Now();
        trace.Run(9, {}, start, {.InputRows = 10, .OutputRows = 3, .CpuTimeUs = 15,
            .ElapsedTimeUs = 25, .RunnerInvoked = true}, std::move(before), std::move(after), false);
        const auto json = Parse(trace.Export());
        const auto& run = json["traceEvents"][0];
        UNIT_ASSERT_VALUES_EQUAL(run["ph"].GetString(), "X");
        UNIT_ASSERT_VALUES_EQUAL(run["dur"].GetUInteger(), 25);
        UNIT_ASSERT_VALUES_EQUAL(run["pid"].GetUInteger(), 7);
        UNIT_ASSERT_VALUES_EQUAL(run["tid"].GetUInteger(), 9);
        UNIT_ASSERT_VALUES_EQUAL(run["args"]["progress"]["cpu_us"].GetUInteger(), 15);
        UNIT_ASSERT_VALUES_EQUAL(run["args"]["channels_before"][0]["buffered_bytes"].GetUInteger(), 120);
        UNIT_ASSERT(run["args"]["channels_after"][0]["buffered_bytes"].IsNull());
        UNIT_ASSERT(!json["dqTrace"]["truncated"].GetBoolean());
    }

    Y_UNIT_TEST(EventAndByteLimitsExposeLoss) {
        TQueryTaskTrace trace({}, 1, {.Enabled = true, .MaxEvents = 2});
        for (ui32 i = 0; i < 5; ++i) { trace.Event("event", 1, {}, Args()); }
        const auto json = Parse(trace.Export());
        UNIT_ASSERT_VALUES_EQUAL(json["traceEvents"].GetArraySafe().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(json["dqTrace"]["dropped_events"].GetUInteger(), 3);
        UNIT_ASSERT(json["dqTrace"]["truncated"].GetBoolean());
        TQueryTaskTrace tiny({}, 1, {.Enabled = true, .MaxBytes = 1});
        tiny.Event("event", 1, {}, Args());
        UNIT_ASSERT_VALUES_EQUAL(Parse(tiny.Export())["traceEvents"].GetArraySafe().size(), 0);
        UNIT_ASSERT(Parse(tiny.Export())["dqTrace"]["truncated"].GetBoolean());
    }

    Y_UNIT_TEST(ConcurrentWritersKeepValidBoundedJson) {
        TQueryTaskTrace trace({}, 1, {.Enabled = true, .MaxEvents = 150});
        TVector<std::thread> writers;
        for (ui32 i = 0; i < 4; ++i) {
            writers.emplace_back([&trace, i] {
                for (ui32 j = 0; j < 100; ++j) { trace.Event("event", i, {}, Args()); }
            });
        }
        for (auto& writer : writers) { writer.join(); }
        const auto json = Parse(trace.Export());
        UNIT_ASSERT_VALUES_EQUAL(json["traceEvents"].GetArraySafe().size(), 150);
        UNIT_ASSERT_VALUES_EQUAL(json["dqTrace"]["dropped_events"].GetUInteger(), 250);
    }

    Y_UNIT_TEST(RegistryRetainsCompletedTraceAndDoesNotEvictActiveQuery) {
        TTaskTraceRegistry registry(1, {.Enabled = true, .MaxQueries = 1});
        const NActors::TActorId first(1, 10), second(1, 11);
        auto trace = registry.GetOrCreate(first);
        trace->Event("event", 1, {}, Args());
        UNIT_ASSERT(!registry.GetOrCreate(second));
        trace->Finish();
        trace.reset();
        const auto saved = Parse(registry.Export(ToString(first)));
        UNIT_ASSERT(saved["dqTrace"]["completed"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(saved["traceEvents"].GetArraySafe().size(), 1);
        UNIT_ASSERT(registry.GetOrCreate(second));
        UNIT_ASSERT_VALUES_EQUAL(Parse(registry.Export())["rejected_admissions"].GetUInteger(), 1);
        UNIT_ASSERT_VALUES_EQUAL(Parse(registry.Export(ToString(first)))["error"].GetString(), "trace_not_found");
    }

    Y_UNIT_TEST(ExpiredCompletedTraceIsRemovedOnAccess) {
        TTaskTraceRegistry registry(1, {.Enabled = true, .Retention = TDuration::Zero()});
        auto trace = registry.GetOrCreate({});
        trace->Finish();
        trace.reset();
        UNIT_ASSERT(Parse(registry.Export())["queries"].GetArraySafe().empty());
    }

    Y_UNIT_TEST(RejectedGraphCannotStartPartialCaptureForLaterTask) {
        TTestRuntime runtime;
        TTaskSchedulerSettings settings;
        settings.ObserveOnly = true;
        settings.Trace = {.Enabled = true, .MaxQueries = 1};
        auto scheduler = CreateDqTaskScheduler(runtime.SingleSys(), 0, MakeIntrusive<NMonitoring::TDynamicCounters>(), settings);
        auto graph = std::make_shared<const TQueryTaskGraph>(TVector<TTaskGraphNode>{{1, 10, 0, 0, 1}},
            TVector<TTaskGraphChannel>{{43, 1, 0, 0, 0}});
        const auto busyQuery = runtime.AllocateEdgeActor();
        const auto rejectedQuery = runtime.AllocateEdgeActor();
        auto busy = scheduler->RegisterGraph(busyQuery, graph, {1});
        auto rejected = scheduler->RegisterGraph(rejectedQuery, graph, {1});
        UNIT_ASSERT(busy->Trace);
        UNIT_ASSERT(!rejected->Trace);
        busy.reset();
        auto task = scheduler->RegisterObserver(runtime.AllocateEdgeActor(), rejectedQuery, 1);
        UNIT_ASSERT(!task->Trace);
        UNIT_ASSERT_VALUES_EQUAL(Parse(scheduler->ExportTrace(ToString(rejectedQuery)))["error"].GetString(), "trace_not_found");
        auto next = scheduler->RegisterGraph(runtime.AllocateEdgeActor(), graph, {1});
        UNIT_ASSERT(next->Trace);
    }

    Y_UNIT_TEST(ObserveModePreservesGraphWaitsAndTraceAfterCleanup) {
        TTestRuntime runtime;
        TTaskSchedulerSettings settings;
        settings.ObserveOnly = true;
        settings.Trace.Enabled = true;
        auto scheduler = CreateDqTaskScheduler(runtime.SingleSys(), 0, MakeIntrusive<NMonitoring::TDynamicCounters>(), settings);
        const auto query = runtime.AllocateEdgeActor();
        auto graph = std::make_shared<const TQueryTaskGraph>(TVector<TTaskGraphNode>{{1, 10, 0, 1, 1}},
            TVector<TTaskGraphChannel>{{43, 1, 0, 0, 0}});
        auto registration = scheduler->RegisterGraph(query, graph, {1});
        auto task = scheduler->RegisterObserver(runtime.AllocateEdgeActor(), query, 1);
        UNIT_ASSERT(task->ObserveOnly);
        task->RequestExecution();
        const auto generation = task->TryDispatch();
        UNIT_ASSERT(generation && task->Start(*generation));
        task->ReportWait({.Reason = ETaskWaitReason::Input, .Origin = ETaskWaitOrigin::Operator,
            .Precision = ETaskWaitPrecision::Exact, .OperatorId = 123, .InputSide = ETaskInputSide::Right,
            .Condition = TTaskWaitCondition::All({{InputDependency(42), 0}})});
        task->ReportWork(ETaskWorkKind::Consume, 123, "HashJoin", "build");
        task->Complete({.RunnerInvoked = true});
        task->Signal(InputDependency(42));
        task->Cancel(true);
        task.reset();
        registration.reset();
        const auto json = Parse(scheduler->ExportTrace(ToString(query)));
        UNIT_ASSERT(json["dqTrace"]["completed"].GetBoolean());
        bool graphSeen = false, waitSeen = false, signalSeen = false;
        for (const auto& event : json["traceEvents"].GetArraySafe()) {
            const auto name = event["name"].GetString();
            graphSeen |= name == "graph";
            signalSeen |= name == "signal";
            if (name == "state" && event["args"]["stop_reason"].GetString() == "waiting") {
                const auto& wait = event["args"]["waits"][0];
                UNIT_ASSERT_VALUES_EQUAL(wait["input_side"].GetString(), "right");
                UNIT_ASSERT_VALUES_EQUAL(wait["precision"].GetString(), "exact");
                UNIT_ASSERT_VALUES_EQUAL(event["args"]["classification"]["coverage"].GetString(), "partial");
                UNIT_ASSERT_VALUES_EQUAL(event["args"]["classification"]["work"].GetString(), "consume");
                UNIT_ASSERT_VALUES_EQUAL(event["args"]["classification"]["phase"].GetString(), "build");
                UNIT_ASSERT(event["args"]["classification"]["legacy_wakeups"].GetBoolean());
                UNIT_ASSERT_VALUES_EQUAL(event["args"]["resume_any_of_all"].GetArraySafe().size(), 3);
                UNIT_ASSERT_VALUES_EQUAL(wait["any_of_all"][0][0]["dependency"].GetString(), ToString(InputDependency(42)));
                waitSeen = true;
            }
        }
        UNIT_ASSERT(graphSeen && waitSeen && signalSeen);
    }
}
