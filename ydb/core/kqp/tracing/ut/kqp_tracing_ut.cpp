#include <ydb/core/kqp/tracing/kqp_execution_rendering.h>
#include <ydb/core/kqp/tracing/kqp_compile_trace.h>
#include <ydb/core/kqp/tracing/kqp_query_rendering.h>
#include <ydb/core/kqp/tracing/kqp_shard_rendering.h>
#include <ydb/core/kqp/tracing/kqp_task_rendering.h>
#include <ydb/core/kqp/tracing/kqp_trace_settings.h>
#include <ydb/core/kqp/tracing/test_util/kqp_trace_test_helpers.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <thread>
#include <stdexcept>

namespace NKikimr {

using namespace NWilson;
using namespace NKqp::NTest;

Y_UNIT_TEST_SUITE(TKqpTrace) {
    TFakeWilsonUploader* RegisterUploader(NActors::TTestActorRuntimeBase& runtime);

    Y_UNIT_TEST(CompilerExecutionFollowsContinuationAcrossThreads) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = RegisterUploader(runtime);
        NWilson::TSpan query(15, NWilson::TTraceId::NewTraceId(15, 4095), "Query",
            NWilson::EFlags::NONE, runtime.GetActorSystem(0));
        NKqp::TCompileTrace trace(query.GetTraceId(), runtime.GetActorSystem(0));
        const auto root = trace.GetCurrentParent();
        trace.Resume();
        trace.Resume(); // Nested run scopes must not duplicate execution slices.
        {
            NKqp::TCompileTraceScope work(&trace, NKqp::TCompileTraceLevels::Detail, "First work", "Test");
        }
        auto continuation = trace.StartContinuation(true);
        const auto continuationId = continuation->GetTraceId();
        trace.Suspend();
        trace.Suspend();
        std::thread worker([&] {
            const auto readyFrom = continuation->Complete();
            NKqp::TCompileTraceRunScope running(&trace, readyFrom);
            NKqp::TCompileTraceScope work(&trace, NKqp::TCompileTraceLevels::Detail, "Resumed work", "Test");
        });
        worker.join();
        trace.Finish(true);
        query.EndOk();
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        TVector<const TTraceSnapshot::TOtelSpan*> executions;
        for (const auto& span : uploader->Spans) {
            if (span.name() == "Compiler execution") {
                executions.push_back(&span);
                UNIT_ASSERT_VALUES_EQUAL(span.parent_span_id(), TString(static_cast<const char*>(root.GetSpanIdPtr()), root.GetSpanIdSize()));
                UNIT_ASSERT(FindAttribute(span, "process.pid"));
                UNIT_ASSERT(FindAttribute(span, "ydb.cpu_us"));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(executions.size(), 2);
        std::ranges::sort(executions, {}, [](const auto* s) { return s->start_time_unix_nano(); });
        UNIT_ASSERT_UNEQUAL(FindAttribute(*executions[0], "thread.id")->value().int_value(),
            FindAttribute(*executions[1], "thread.id")->value().int_value());
        UNIT_ASSERT_VALUES_EQUAL(executions[1]->links_size(), 1);
        const auto* ready = FindSpan(*uploader, "Compiler continuation");
        UNIT_ASSERT(ready);
        UNIT_ASSERT_VALUES_EQUAL(executions[1]->links(0).span_id(), ready->span_id());
        UNIT_ASSERT_VALUES_EQUAL(ready->span_id(),
            TString(static_cast<const char*>(continuationId.GetSpanIdPtr()), continuationId.GetSpanIdSize()));
        UNIT_ASSERT_VALUES_EQUAL(ready->links(0).span_id(), executions[0]->span_id());
        UNIT_ASSERT_LE(ready->end_time_unix_nano(), executions[1]->start_time_unix_nano() + 1000);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*FindSpan(*uploader, "Resumed work"), "ydb.compile.kind")->value().string_value(), "scope");
    }

    Y_UNIT_TEST(DependencyJoinPreservesReadinessAndFailure) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = RegisterUploader(runtime);
        NWilson::TSpan query(15, NWilson::TTraceId::NewTraceId(15, 4095), "Query",
            NWilson::EFlags::NONE, runtime.GetActorSystem(0));
        NKqp::TCompileTrace trace(query.GetTraceId(), runtime.GetActorSystem(0));
        auto first = NThreading::NewPromise<void>();
        auto last = NThreading::NewPromise<void>();
        auto join = trace.StartSpan(14, "Join", "Test", "async");
        const auto joinId = join.GetTraceId();
        trace.AwaitDependency(joinId);
        trace.AwaitDependency(joinId); // Repeated GetAsyncFuture must not duplicate links.
        auto continuation = trace.StartContinuation();
        auto ready = NKqp::TraceCompileDependency(NThreading::WaitAll(first.GetFuture(), last.GetFuture()), std::move(join));
        first.SetValue();
        UNIT_ASSERT(!ready.IsReady());
        std::thread worker([&] { last.SetValue(); });
        worker.join();
        ready.GetValue();
        UNIT_ASSERT(continuation->Complete());
        auto unrelated = trace.StartContinuation();
        UNIT_ASSERT(unrelated->Complete());

        auto failure = NThreading::NewPromise<int>();
        auto failed = NKqp::TraceCompileDependency(failure.GetFuture(), trace.StartSpan(14, "Failed dependency", "Test", "async"));
        failure.SetException(std::make_exception_ptr(std::runtime_error("expected")));
        UNIT_ASSERT_EXCEPTION(failed.GetValue(), std::runtime_error);
        auto value = NKqp::TraceCompileDependency(NThreading::MakeFuture(std::make_unique<int>(42)),
            trace.StartSpan(14, "Move-only dependency", "Test", "async"));
        UNIT_ASSERT_VALUES_EQUAL(*value.ExtractValue(), 42);

        // A failed operation can still return a value: it must not turn an
        // all-or-exception join into a fail-fast join or discard the result.
        auto operation = NKqp::TraceCompileDependency(NThreading::MakeFuture(false),
            trace.StartSpan(14, "Operation error", "Test", "async"), [](bool success) { return success; });
        auto pending = NThreading::NewPromise<void>();
        auto operationJoin = NThreading::WaitExceptionOrAll(operation.Apply([](const NThreading::TFuture<bool>& ready) {
            UNIT_ASSERT(!ready.GetValue());
        }), pending.GetFuture());
        UNIT_ASSERT(!operationJoin.IsReady());
        pending.SetValue();
        operationJoin.GetValue();
        auto voidOperation = NKqp::TraceCompileDependency(NThreading::MakeFuture(),
            trace.StartSpan(14, "Void operation error", "Test", "async"), [] { return false; });
        voidOperation.GetValue();
        query.EndOk();
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        const auto* joined = FindSpan(*uploader, "Join");
        UNIT_ASSERT(joined && joined->events_size() == 1);
        UNIT_ASSERT_VALUES_EQUAL(joined->events(0).name(), "Dependency ready");
        UNIT_ASSERT(FindAttribute(joined->events(0), "thread.id"));
        size_t dependent = 0, unattributed = 0;
        for (const auto& span : uploader->Spans) {
            if (span.name() != "Compiler continuation") {
                continue;
            }
            if (span.links_size()) {
                ++dependent;
                UNIT_ASSERT_VALUES_EQUAL(span.links_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(span.links(0).span_id(), joined->span_id());
                UNIT_ASSERT_LE(joined->events(0).time_unix_nano(), span.events(0).time_unix_nano() + 1000);
            } else {
                ++unattributed;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(dependent, 1);
        UNIT_ASSERT_VALUES_EQUAL(unattributed, 1);
        AssertStatus(*uploader, "Failed dependency", NTraceProto::Status::STATUS_CODE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*FindSpan(*uploader, "Failed dependency"),
            "ydb.compile.future_outcome")->value().string_value(), "exception");
        for (const TStringBuf name : {"Operation error", "Void operation error"}) {
            AssertStatus(*uploader, name, NTraceProto::Status::STATUS_CODE_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*FindSpan(*uploader, name),
                "ydb.compile.future_outcome")->value().string_value(), "value");
        }
    }

    Y_UNIT_TEST(FinishStopsExecutionAndAbandonsPendingContinuation) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = RegisterUploader(runtime);
        NWilson::TSpan query(15, NWilson::TTraceId::NewTraceId(15, 4095), "Query",
            NWilson::EFlags::NONE, runtime.GetActorSystem(0));
        NKqp::TCompileTrace trace(query.GetTraceId(), runtime.GetActorSystem(0));
        {
            NKqp::TCompileTraceRunScope running(&trace);
            auto continuation = trace.StartContinuation(true);
            // Completion/fallback can finish an attempt before its actor
            // handler and its run scope return.
            trace.Finish(false);
            const auto frozenCpu = trace.GetCpuTime();
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            const auto* execution = FindSpan(*uploader, "Compiler execution");
            UNIT_ASSERT(execution);
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*execution, "ydb.cpu_us")->value().int_value(), frozenCpu);

            NWilson::TTraceId lateResume;
            std::thread worker([&] { lateResume = continuation->Complete(); });
            worker.join();
            UNIT_ASSERT(!lateResume);
            UNIT_ASSERT(!continuation->Complete(false));
            trace.Finish(false); // Repeated termination must not emit duplicates.
            UNIT_ASSERT_VALUES_EQUAL(trace.GetCpuTime(), frozenCpu);
        }
        query.EndOk();
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(std::ranges::count_if(uploader->Spans, [](const auto& span) {
            return span.name() == "Compiler execution";
        }), 1);
        UNIT_ASSERT_VALUES_EQUAL(std::ranges::count_if(uploader->Spans, [](const auto& span) {
            return span.name() == "Compiler continuation";
        }), 1);
        const auto* cancelled = FindSpan(*uploader, "Compiler continuation");
        UNIT_ASSERT(cancelled);
        AssertStatus(*uploader, "Compiler continuation", NTraceProto::Status::STATUS_CODE_ERROR);
        UNIT_ASSERT(FindAttribute(*cancelled, "ydb.compile.cancelled")->value().bool_value());
        UNIT_ASSERT(!std::ranges::any_of(cancelled->events(), [](const auto& event) {
            return event.name() == "Result ready";
        }));
    }

    Y_UNIT_TEST(DeferredRuleKeepsParentOnlyWhenNeeded) {
        class TProducer : public NActors::TActorBootstrapped<TProducer> {
        public:
            void Bootstrap(const NActors::TActorContext& ctx) {
                NWilson::TSpan root(15, NWilson::TTraceId::NewTraceId(15, 4095), "Query");
                NKqp::TCompileTrace trace(root.GetTraceId(), ctx.ActorSystem());
                {
                    NKqp::TCompileTraceRunScope running(&trace);
                    NKqp::TCompileTraceDeferredScope skipped(&trace, "Skipped rule", "Test");
                    UNIT_ASSERT(!skipped.Finish(false));
                    NKqp::TCompileTraceDeferredScope parent(&trace, "Rule with child", "Test");
                    {
                        NKqp::TCompileTraceScope child(&trace, 15, "CBO work", "Test");
                    }
                    auto span = parent.Finish(false);
                    UNIT_ASSERT(span);
                    span.End();
                    try {
                        NKqp::TCompileTraceDeferredScope failed(&trace, "Failed rule", "Test");
                        ythrow yexception() << "failure";
                    } catch (const yexception&) {
                    }
                    NKqp::TCompileTraceDeferredScope dropped(&trace, "Rule with skipped nested rule", "Test");
                    {
                        NKqp::TCompileTraceDeferredScope nested(&trace, "Skipped nested rule", "Test");
                        UNIT_ASSERT(!nested.Finish(false));
                    }
                    UNIT_ASSERT(!dropped.Finish(false));
                    NKqp::TCompileTraceDeferredScope kept(&trace, "Rule with applied nested rule", "Test");
                    {
                        NKqp::TCompileTraceDeferredScope nested(&trace, "Applied nested rule", "Test");
                        auto nestedSpan = nested.Finish(true);
                        UNIT_ASSERT(nestedSpan);
                        nestedSpan.End();
                    }
                    auto keptSpan = kept.Finish(false);
                    UNIT_ASSERT(keptSpan);
                    keptSpan.End();
                }
                root.EndOk();
                PassAway();
            }
        };
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = RegisterUploader(runtime);
        runtime.Register(new TProducer());
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(!FindSpan(*uploader, "Skipped rule"));
        AssertDescendant(*uploader, "CBO work", "Rule with child");
        AssertStatus(*uploader, "Failed rule", NTraceProto::Status::STATUS_CODE_ERROR);
        UNIT_ASSERT(!FindSpan(*uploader, "Rule with skipped nested rule"));
        UNIT_ASSERT(!FindSpan(*uploader, "Skipped nested rule"));
        AssertDescendant(*uploader, "Applied nested rule", "Rule with applied nested rule");
        UNIT_ASSERT(uploader->BuildTraceTrees());
    }

    Y_UNIT_TEST(RestartMarksNestedRegions) {
        class TProducer : public NActors::TActorBootstrapped<TProducer> {
        public:
            void Bootstrap(const NActors::TActorContext& ctx) {
                NWilson::TSpan root(15, NWilson::TTraceId::NewTraceId(15, 4095), "Query");
                NKqp::TCompileTrace trace(root.GetTraceId(), ctx.ActorSystem());
                const int pipeline = 0;
                const int nested = 0;
                {
                    NKqp::TCompileTraceRunScope running(&trace);
                    UNIT_ASSERT(trace.BeginRegion(15, "First region", "Test", &pipeline));
                    UNIT_ASSERT(trace.BeginRegion(15, "Host region", "Test", &pipeline));
                    UNIT_ASSERT(trace.BeginRegion(15, "Nested region", "Test", &nested));
                    // The pipeline restarts inside the host region.
                    UNIT_ASSERT(trace.BeginRegion(15, "First region", "Test", &pipeline));
                    trace.EndRegion("First region");
                }
                root.EndOk();
                PassAway();
            }
        };
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = RegisterUploader(runtime);
        runtime.Register(new TProducer());
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        AssertDescendant(*uploader, "Nested region", "Host region");
        for (const TStringBuf name : {"Host region", "Nested region"}) {
            const auto* restarted = FindAttribute(*FindSpan(*uploader, name), "ydb.compile.restarted");
            UNIT_ASSERT_C(restarted && restarted->value().bool_value(), name);
        }
        UNIT_ASSERT(uploader->BuildTraceTrees());
    }

    TFakeWilsonUploader* RegisterUploader(NActors::TTestActorRuntimeBase& runtime) {
        auto* uploader = new TFakeWilsonUploader();
        runtime.RegisterService(MakeWilsonUploaderId(), runtime.Register(uploader));
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        return uploader;
    }

    Y_UNIT_TEST(TaskNamesDescribeOperatorsWithoutReadingLiterals) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = RegisterUploader(runtime);
        NKqpProto::TKqpPhyStage stage;
        stage.AddSources()->MutableReadRangesSource();
        stage.SetProgramAst("(lambda '(arg) (block '((let j (GraceJoinCore arg arg)) (let f (Filter j)) (return (Aggregate f)))))");
        const auto description = NKqp::TTaskTraceDescription::FromStage(stage);
        UNIT_ASSERT_VALUES_EQUAL(description.Name(), "Task: Read + Join + ...");
        NYql::NDqProto::TDqTask task;
        description.Save(task);
        NYql::NDqProto::TDqTask received;
        UNIT_ASSERT(received.ParseFromString(task.SerializeAsString()));
        NWilson::TSpan span(TComponentTracingLevels::TQueryProcessor::Detailed,
            NWilson::TTraceId::NewTraceId(15, 4095), "Task", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
        NKqp::TTaskTraceDescription::Annotate(span, received);
        span.EndOk();
        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto* result = FindSpan(*uploader, "Task: Read + Join + ...");
        UNIT_ASSERT(result);
        const auto& operations = FindAttribute(*result, "ydb.task.operations")->value().array_value();
        UNIT_ASSERT_VALUES_EQUAL(operations.values_size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(operations.values(0).string_value(), "Read");
        UNIT_ASSERT_VALUES_EQUAL(operations.values(1).string_value(), "Join");
        UNIT_ASSERT_VALUES_EQUAL(operations.values(2).string_value(), "Filter");
        UNIT_ASSERT_VALUES_EQUAL(operations.values(3).string_value(), "Aggregate");
        stage.SetProgramAst("(lambda '() (AsList (String 'Join) (String 'Filter) (String 'Aggregate) '(Sort)))");
        UNIT_ASSERT_VALUES_EQUAL(NKqp::TTaskTraceDescription::FromStage(stage).Name(), "Task: Read");
        stage.SetProgramAst("invalid AST (");
        UNIT_ASSERT_VALUES_EQUAL(NKqp::TTaskTraceDescription::FromStage(stage).Name(), "Task: Read");
        stage.Clear();
        stage.AddTableOps()->MutableUpsertRows();
        UNIT_ASSERT_VALUES_EQUAL(NKqp::TTaskTraceDescription::FromStage(stage).Name(), "Task: Write");
    }

    Y_UNIT_TEST(CommitPhaseRespectsLevelsAndRetainsTheLastShard) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = RegisterUploader(runtime);
        for (const ui8 level : {0, 1, 6, 10, 15}) {
            ClearUploader(*uploader);
            NWilson::TSpan parent(1, level ? NWilson::TTraceId::NewTraceId(level, 4095) : NWilson::TTraceId(),
                "Commit", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
            NKqp::TCommitTracePhase phase;
            ui32 counted = 0;
            phase.Start(parent, {"Prepare shards", "CommitPrepareShards", "TKqpBufferWriteActor", nullptr, "DataShard"}, [&] {
                ++counted;
                return NKqp::NQueryTraceSettings::MAX_SHARD_EVENTS + 10;
            });
            const ui64 shards = NKqp::NQueryTraceSettings::MAX_SHARD_EVENTS + 10;
            for (ui64 id = 1; id <= shards; ++id) {
                phase.Acknowledge(id, id == shards);
            }
            phase.End(Ydb::StatusIds::SUCCESS);
            parent.EndOk();
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            const bool detailed = level >= TComponentTracingLevels::TQueryProcessor::Detailed;
            UNIT_ASSERT_VALUES_EQUAL(counted, detailed ? 1 : 0);
            const auto* prepare = FindSpan(*uploader, "Prepare shards");
            UNIT_ASSERT_VALUES_EQUAL(bool(prepare), detailed);
            if (prepare) {
                UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*prepare, "ydb.shards")->value().int_value(), shards);
                const bool diagnostic = level >= TComponentTracingLevels::TQueryProcessor::Diagnostic;
                UNIT_ASSERT_VALUES_EQUAL(prepare->events_size(), diagnostic ? NKqp::NQueryTraceSettings::MAX_SHARD_EVENTS : 0);
                if (diagnostic) {
                    const auto& last = prepare->events(prepare->events_size() - 1);
                    UNIT_ASSERT(FindAttribute(last, "ydb.last_shard")->value().bool_value());
                    UNIT_ASSERT_VALUES_EQUAL(FindAttribute(last, "ydb.shard_id")->value().int_value(), shards);
                    UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*prepare, "ydb.shard_events_dropped")->value().int_value(), 10);
                }
            }
        }
    }

    Y_UNIT_TEST(QueryCpuSeparatesOverheadAndPreservesBatchTotals) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = RegisterUploader(runtime);
        ClearUploader(*uploader);
        NKqp::TBatchExecutionTrace trace;
        for (const ui64 cpu : {100, 200}) {
            NYql::NDqProto::TDqExecutionStats execution;
            NKqpProto::TKqpExecutionExtraStats extra;
            extra.SetCpuTimeUs(cpu);
            extra.SetWaitTimeUs(cpu / 10);
            extra.SetSpilledBytes(cpu * 10);
            extra.SetMaxTaskSkew(cpu / 100.0);
            extra.SetTaskStatsIncomplete(cpu == 100);
            execution.MutableExtra()->PackFrom(extra);
            trace.AddExecution(execution);
        }
        NKqpProto::TKqpStatsQuery stats;
        trace.Export(*stats.AddExecutions());
        stats.AddExecutions()->SetCpuTimeUs(50);
        stats.MutableCompilation()->SetCpuTimeUs(13);
        stats.SetWorkerCpuTimeUs(7);
        NWilson::TSpan query(1, NWilson::TTraceId::NewTraceId(15, 4095), "Query",
            NWilson::EFlags::NONE, runtime.GetActorSystem(0));
        NKikimrKqp::TEvQueryResponse response;
        response.SetConsumedRu(1);
        response.SetYdbStatus(Ydb::StatusIds::SUCCESS);
        NKqp::AddWorkerQueryResultAttributes(query, {"SELECT", "SELECT"}, response, &stats, true);
        query.EndOk();
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        const auto* span = FindSpan(*uploader, "Query");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*span, "ydb.cpu_us")->value().int_value(), 350);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*span, "ydb.wait_us")->value().int_value(), 30);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*span, "ydb.spilled_bytes")->value().int_value(), 3000);
        UNIT_ASSERT(FindAttribute(*span, "ydb.spilled_bytes_available")->value().bool_value());
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*span, "ydb.max_task_skew")->value().double_value(), 2.0);
        UNIT_ASSERT(FindAttribute(*span, "ydb.task_stats_incomplete")->value().bool_value());
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*span, "ydb.compile.cpu_us")->value().int_value(), 13);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*span, "ydb.session.cpu_us")->value().int_value(), 7);
    }

    Y_UNIT_TEST(TaskTraceUsesReportedStatistics) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = RegisterUploader(runtime);
        NYql::NDqProto::TDqComputeActorStats stats;
        stats.SetCpuTimeUs(90);
        auto& task = *stats.AddTasks();
        task.SetCpuTimeUs(70);
        task.SetCreateTimeMs(100);
        task.SetStartTimeMs(120);
        task.SetFinishTimeMs(121);
        task.SetSpillingComputeWriteBytes(30);
        task.SetSpillingChannelWriteBytes(70);
        NKqpProto::TKqpTaskExtraStats extra;
        extra.SetReadRetriesCount(2);
        task.MutableExtra()->PackFrom(extra);
        NWilson::TSpan span(TComponentTracingLevels::TQueryProcessor::Detailed,
            NWilson::TTraceId::NewTraceId(15, 4095), "Task: Compute", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
        NKqp::AddKqpTaskTraceAttributes(span, stats, true);
        span.EndOk();
        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto* compute = FindSpan(*uploader, "Task: ");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*compute, "ydb.cpu_us")->value().int_value(), 70);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*compute, "ydb.read_retries")->value().int_value(), 2);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*compute, "ydb.queue_delay_us")->value().int_value(), 20000);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*compute, "ydb.spilled_bytes")->value().int_value(), 100);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*compute, "ydb.duration_us")->value().int_value(), 1000);

        ClearUploader(*uploader);
        NYql::NDqProto::TDqComputeActorStats sharedStats;
        sharedStats.SetCpuTimeUs(90);
        sharedStats.AddTasks()->SetCpuTimeUs(40);
        sharedStats.AddTasks()->SetCpuTimeUs(50);
        NWilson::TSpan sharedSpan(TComponentTracingLevels::TQueryProcessor::Detailed,
            NWilson::TTraceId::NewTraceId(15, 4095), "Task: Shared", NWilson::EFlags::NONE,
            runtime.GetActorSystem(0));
        NKqp::AddKqpTaskTraceAttributes(sharedSpan, sharedStats, true);
        sharedSpan.EndOk();
        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto* shared = FindSpan(*uploader, "Task: Shared");
        UNIT_ASSERT(shared);
        UNIT_ASSERT(!FindAttribute(*shared, "ydb.cpu_us"));
        UNIT_ASSERT(FindAttribute(*shared, "ydb.cpu_us_shared")->value().bool_value());
    }

    Y_UNIT_TEST(StageSpansCloseWithTasksAndPreserveCompletedStagesOnCancellation) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = RegisterUploader(runtime);
        for (const auto status : {Ydb::StatusIds::SUCCESS, Ydb::StatusIds::CANCELLED}) {
            ClearUploader(*uploader);
            NKqp::TExecutionTrace trace(15, true);
            NKqpProto::TKqpPhyStage physical;
            physical.SetProgramAst("(Aggregate)");
            NWilson::TSpan parent(TComponentTracingLevels::TQueryProcessor::Basic,
                NWilson::TTraceId::NewTraceId(15, 4095), "Run tasks", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
            const auto firstId = trace.StartStage(parent, {0, 7}, physical, 2);
            const auto secondId = trace.StartStage(parent, {1, 7}, physical, 1);
            UNIT_ASSERT(firstId && secondId && firstId != secondId);
            for (ui64 id : {1, 2}) {
                NYql::NDqProto::TDqTask task;
                task.SetId(id);
                trace.AnnotateTask({0, 7}, task);
                NWilson::TSpan child(TComponentTracingLevels::TQueryProcessor::Detailed,
                    NKqp::GetTaskTraceParent(task, parent.GetTraceId()), "Task: Aggregate",
                    NWilson::EFlags::NONE, runtime.GetActorSystem(0));
                runtime.AdvanceCurrentTime(TDuration::Seconds(1));
                child.EndOk();
                NYql::NDqProto::TDqTaskStats stats;
                stats.SetTaskId(id);
                stats.SetStageId(7);
                trace.AddTask(0, 2, stats, 1'000'000, 1, Ydb::StatusIds::SUCCESS);
            }
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(std::ranges::distance(StageSpans(*uploader)), 1);
            const auto completedEnd = StageSpans(*uploader).begin()->end_time_unix_nano();
            runtime.AdvanceCurrentTime(TDuration::Seconds(5));
            NYql::NDqProto::TDqExecutionStats stats;
            trace.Finish(parent, stats, status);
            NKqp::EndQueryTraceSpan(parent, status);
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT(uploader->BuildTraceTrees());
            UNIT_ASSERT_VALUES_EQUAL(std::ranges::distance(StageSpans(*uploader)), 2);
            for (const auto& stage : StageSpans(*uploader)) {
                const bool completed = FindAttribute(stage, "ydb.tx_index")->value().int_value() == 0;
                const auto expected = completed ? NTraceProto::Status::STATUS_CODE_OK
                    : status == Ydb::StatusIds::SUCCESS ? NTraceProto::Status::STATUS_CODE_UNSET
                    : NTraceProto::Status::STATUS_CODE_ERROR;
                UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(stage.status().code()), static_cast<int>(expected));
                if (completed) {
                    UNIT_ASSERT_VALUES_EQUAL(stage.end_time_unix_nano(), completedEnd);
                    UNIT_ASSERT_VALUES_EQUAL(FindAttribute(stage, "ydb.reported_tasks")->value().int_value(), 2);
                    for (const auto& child : uploader->Spans) {
                        if (child.name().StartsWith("Task: ")) {
                            UNIT_ASSERT_VALUES_EQUAL(child.parent_span_id(), stage.span_id());
                        }
                    }
                } else {
                    UNIT_ASSERT(stage.end_time_unix_nano() > completedEnd);
                    UNIT_ASSERT(FindAttribute(stage, "ydb.task_stats_incomplete")->value().bool_value());
                    UNIT_ASSERT(!FindAttribute(stage, "ydb.task_stats_failed")->value().bool_value());
                }
            }
        }
    }

    Y_UNIT_TEST(StageTraceParentRespectsLevelsAndRejectsInvalidContext) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = RegisterUploader(runtime);
        for (const ui8 level : {6, 10}) {
            ClearUploader(*uploader);
            NWilson::TSpan parent(TComponentTracingLevels::TQueryProcessor::Basic,
                NWilson::TTraceId::NewTraceId(level, 4095), "Run tasks", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
            NKqp::TExecutionTrace trace(level, true);
            NKqpProto::TKqpPhyStage stage;
            const auto stageTraceId = trace.StartStage(parent, {0, 1}, stage, 1);
            UNIT_ASSERT_VALUES_EQUAL(bool(stageTraceId), level == 10);
            NYql::NDqProto::TDqTask task;
            trace.AnnotateTask({0, 1}, task);
            auto context = NKqp::GetTaskTraceParent(task, parent.GetTraceId());
            UNIT_ASSERT(context.IsSameTrace(parent.GetTraceId()));
            if (level == 10) {
                UNIT_ASSERT(context == stageTraceId);
            }
            UNIT_ASSERT_VALUES_EQUAL(context.GetVerbosity(), level);
            UNIT_ASSERT_VALUES_EQUAL(context == parent.GetTraceId(), level == 6);
            if (level == 10) {
                UNIT_ASSERT_VALUES_EQUAL(context.GetTimeToLive(), parent.GetTraceId().GetTimeToLive() - 1);
            }
            NKqp::SaveTaskTraceParent(task, {});
            UNIT_ASSERT(NKqp::GetTaskTraceParent(task, parent.GetTraceId()) == parent.GetTraceId());
            UNIT_ASSERT(task.GetTaskParams().empty());
            (*task.MutableTaskParams())["ydb.trace.stage_trace_id"] = "invalid";
            UNIT_ASSERT(NKqp::GetTaskTraceParent(task, parent.GetTraceId()) == parent.GetTraceId());
            NKqp::SaveTaskTraceParent(task, stageTraceId);
            UNIT_ASSERT(!NKqp::GetTaskTraceParent(task, {}));
            auto basic = NWilson::TTraceId::NewTraceId(6, 100);
            UNIT_ASSERT(NKqp::GetTaskTraceParent(task, basic) == basic);
            auto unrelated = NWilson::TTraceId::NewTraceId(10, 100);
            UNIT_ASSERT(NKqp::GetTaskTraceParent(task, unrelated) == unrelated);
            NYql::NDqProto::TDqExecutionStats stats;
            trace.Finish(parent, stats, Ydb::StatusIds::CANCELLED);
            parent.EndError("cancelled");
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT(uploader->BuildTraceTrees());
            UNIT_ASSERT_VALUES_EQUAL(uploader->Traces.size(), 1);
        }
    }

    Y_UNIT_TEST(StageDiagnosticsPreserveAnomaliesAndTotals) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = RegisterUploader(runtime);
        NKqp::TExecutionTrace diagnostics(TComponentTracingLevels::TQueryProcessor::Detailed, true);
        NKqpProto::TKqpPhyStage stage;
        stage.SetProgramAst("(Aggregate)");
        NWilson::TSpan span(TComponentTracingLevels::TQueryProcessor::Basic,
            NWilson::TTraceId::NewTraceId(15, 4095), "Execute plan", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
        diagnostics.StartStage(span, {0, 0}, stage, 10);
        for (ui64 id = 1; id <= 10; ++id) {
            NYql::NDqProto::TDqTaskStats task;
            task.SetTaskId(id);
            task.SetStageId(0);
            task.SetCpuTimeUs(5);
            task.SetWaitInputTimeUs(10);
            task.SetSpillingComputeWriteBytes(id == 2 ? 200 : 0);
            diagnostics.AddTask(0, 10, task, id * 100, id <= 5 ? 1 : 2,
                id == 1 ? Ydb::StatusIds::ABORTED : Ydb::StatusIds::SUCCESS);
        }
        NYql::NDqProto::TDqExecutionStats stats;
        diagnostics.Finish(span, stats, Ydb::StatusIds::ABORTED);
        span.EndError("task failed");
        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto* execution = FindSpan(*uploader, "Execute plan");
        UNIT_ASSERT(execution);
        UNIT_ASSERT_VALUES_EQUAL(std::ranges::distance(StageSpans(*uploader)), 1);
        const auto& event = *StageSpans(*uploader).begin();
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.reported_tasks")->value().int_value(), 10);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.failed_tasks")->value().int_value(), 1);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.task_duration_min_us")->value().int_value(), 100);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.task_duration_avg_us")->value().int_value(), 550);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.task_duration_max_us")->value().int_value(), 1000);
        UNIT_ASSERT_DOUBLES_EQUAL(FindAttribute(event, "ydb.task_skew")->value().double_value(), 1000.0 / 550, 1e-9);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.tasks_by_node")->value().string_value(), "1:5,2:5");
        const auto& tasks = FindAttribute(event, "ydb.ranked_tasks")->value().array_value();
        UNIT_ASSERT_VALUES_EQUAL(tasks.values_size(), NKqp::NQueryTraceSettings::MAX_TASKS_PER_STAGE);
        bool hasFailed = false;
        bool hasSpill = false;
        for (const auto& task : tasks.values()) {
            for (const auto& attr : task.kvlist_value().values()) {
                hasFailed |= attr.key() == "ydb.failed" && attr.value().bool_value();
                hasSpill |= attr.key() == "ydb.spilled_bytes" && attr.value().int_value() == 200;
            }
        }
        UNIT_ASSERT(hasFailed && hasSpill);
        NKqpProto::TKqpExecutionExtraStats extra;
        UNIT_ASSERT(stats.GetExtra().UnpackTo(&extra));
        UNIT_ASSERT(!extra.GetTaskStatsIncomplete());
        UNIT_ASSERT_VALUES_EQUAL(extra.GetWaitTimeUs(), 100);
        UNIT_ASSERT_VALUES_EQUAL(extra.GetSpilledBytes(), 200);

        NKqpProto::TKqpStatsQuery queryStats;
        *queryStats.AddExecutions() = stats;
        NWilson::TSpan query(TComponentTracingLevels::TQueryProcessor::TopLevel,
            NWilson::TTraceId::NewTraceId(15, 4095), "Query", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
        NKikimrKqp::TEvQueryResponse response;
        response.SetConsumedRu(1);
        response.SetYdbStatus(Ydb::StatusIds::ABORTED);
        NKqp::AddWorkerQueryResultAttributes(query, {"SELECT", "SELECT"}, response, &queryStats, true);
        query.EndError("task failed");
        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto* result = FindSpan(*uploader, "Query");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*result, "ydb.spilled_bytes")->value().int_value(), 200);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*result, "ydb.wait_us")->value().int_value(), 100);
    }

    Y_UNIT_TEST(BasicDiagnosticsKeepTotalsWithoutStageSpans) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = RegisterUploader(runtime);
        for (const ui8 level : {6, 10}) {
            ClearUploader(*uploader);
            NKqp::TExecutionTrace diagnostics(level, true);
            NKqpProto::TKqpPhyStage stage;
            NWilson::TSpan span(TComponentTracingLevels::TQueryProcessor::Basic,
                NWilson::TTraceId::NewTraceId(level, 4095), "Execute plan", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
            diagnostics.StartStage(span, {0, 0}, stage, 3);
            for (ui64 id = 0; id < 3; ++id) {
                NYql::NDqProto::TEvComputeActorState state;
                state.SetState(NYql::NDqProto::COMPUTE_STATE_FINISHED);
                auto& task = *state.MutableStats()->AddTasks();
                task.SetTaskId(id + 1);
                task.SetStageId(0);
                task.SetWaitInputTimeUs(1);
                task.SetSpillingComputeWriteBytes(2);
                task.SetStartTimeMs(100);
                task.SetFinishTimeMs(100 + id);
                diagnostics.OnTaskFinished({0, 0}, 3, state, 1);
            }
            NYql::NDqProto::TDqExecutionStats stats;
            diagnostics.Finish(span, stats, Ydb::StatusIds::SUCCESS);
            span.EndOk();
            runtime.SimulateSleep(TDuration::Seconds(1));
            const auto* execution = FindSpan(*uploader, "Execute plan");
            UNIT_ASSERT_VALUES_EQUAL(std::ranges::distance(StageSpans(*uploader)), level == 10 ? 1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*execution, "ydb.wait_us")->value().int_value(), 3);
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*execution, "ydb.spilled_bytes")->value().int_value(), 6);
            UNIT_ASSERT_DOUBLES_EQUAL(FindAttribute(*execution, "ydb.max_task_skew")->value().double_value(), 2, 1e-9);
            UNIT_ASSERT(!FindAttribute(*execution, "ydb.task_stats_incomplete")->value().bool_value());
        }
    }

    Y_UNIT_TEST(StageLimitsKeepTotalsAndTransactionIdentity) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = RegisterUploader(runtime);
        NKqp::TExecutionTrace diagnostics(TComponentTracingLevels::TQueryProcessor::Detailed, true);
        NKqpProto::TKqpPhyStage stage;
        NYql::NDqProto::TDqTaskStats task;
        task.SetStageId(0);
        task.SetWaitOutputTimeUs(1);
        task.SetSpillingChannelWriteBytes(2);
        NWilson::TSpan span(TComponentTracingLevels::TQueryProcessor::Basic,
            NWilson::TTraceId::NewTraceId(15, 4095), "Execute plan", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
        for (ui64 tx = 0; tx < NKqp::NQueryTraceSettings::MAX_STAGES + 3; ++tx) {
            diagnostics.StartStage(span, {tx, 0}, stage, 2);
            task.SetTaskId(tx + 1);
            diagnostics.AddTask(tx, 2, task, std::nullopt, 1, Ydb::StatusIds::ABORTED);
        }
        NYql::NDqProto::TDqExecutionStats stats;
        diagnostics.Finish(span, stats, Ydb::StatusIds::ABORTED);
        span.EndError("incomplete execution");
        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto* execution = FindSpan(*uploader, "Execute plan");
        UNIT_ASSERT_VALUES_EQUAL(std::ranges::distance(StageSpans(*uploader)), NKqp::NQueryTraceSettings::MAX_STAGES);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*execution, "ydb.wait_us")->value().int_value(), NKqp::NQueryTraceSettings::MAX_STAGES + 3);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*execution, "ydb.spilled_bytes")->value().int_value(), 2 * (NKqp::NQueryTraceSettings::MAX_STAGES + 3));
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*execution, "ydb.tasks_without_stage_details")->value().int_value(), 3);
        for (const auto& event : StageSpans(*uploader)) {
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.reported_tasks")->value().int_value(), 1);
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.tasks")->value().int_value(), 2);
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.timed_tasks")->value().int_value(), 0);
        }
    }

    Y_UNIT_TEST(ShardReadsKeepLateFailuresRetriesAndSlowShards) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = RegisterUploader(runtime);
        NKqp::TShardReadTrace reads;
        NWilson::TSpan parent(TComponentTracingLevels::TQueryProcessor::Detailed,
            NWilson::TTraceId::NewTraceId(15, 4095), "Read table", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
        for (ui64 id = 1; id <= 40; ++id) {
            reads.Start(parent, id, id);
            runtime.AdvanceCurrentTime(TDuration::MilliSeconds(1));
            reads.ReadResult(parent, id, 1, id, 1, Ydb::StatusIds::SUCCESS, true);
        }
        reads.Start(parent, 100, 100);
        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(1));
        reads.ReadResult(parent, 100, 1, 100, 0, Ydb::StatusIds::OVERLOADED, false);
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        reads.Retry(parent, 100, 100);
        reads.Start(parent, 100, 101);
        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(1));
        reads.ReadResult(parent, 100, 2, 101, 3, Ydb::StatusIds::SUCCESS, true);
        reads.Start(parent, 200, 200);
        runtime.AdvanceCurrentTime(TDuration::Seconds(2));
        reads.ReadResult(parent, 200, 2, 200, 5, Ydb::StatusIds::SUCCESS, true);
        reads.Start(parent, 300, 300);
        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(1));
        reads.Stop(300);
        reads.Finish(parent);
        parent.EndOk();
        reads.Finish(parent);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(uploader->BuildTraceTrees());
        const auto* result = FindSpan(*uploader, "Read table");
        UNIT_ASSERT_VALUES_EQUAL(result->events_size(), NKqp::NQueryTraceSettings::MAX_INTERESTING_READ_SHARDS);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*result, "ydb.shard_reads")->value().int_value(), 44);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*result, "ydb.shard_summaries_dropped")->value().int_value(), 38);
        bool hasRetry = false, hasSlow = false, hasStopped = false;
        for (const auto& event : result->events()) {
            const auto shardId = FindAttribute(event, "ydb.shard_id")->value().int_value();
            if (shardId == 100) {
                hasRetry = true;
                UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.read_retries")->value().int_value(), 1);
                UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.failed_reads")->value().int_value(), 1);
                UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.reads")->value().int_value(), 2);
                UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.rows")->value().int_value(), 3);
                UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.status_code")->value().string_value(), "SUCCESS");
                UNIT_ASSERT(FindAttribute(event, "ydb.duration_us")->value().int_value() >= 1'000'000);
            }
            hasSlow |= shardId == 200;
            hasStopped |= shardId == 300;
        }
        UNIT_ASSERT(hasRetry && hasSlow && hasStopped);
        size_t attempts = 0;
        for (const auto& span : uploader->Spans) {
            if (span.name() != "Read shard") {
                continue;
            }
            ++attempts;
            const auto id = FindAttribute(span, "ydb.read_id")->value().int_value();
            if (id == 100 || id == 300) {
                UNIT_ASSERT(!FindAttribute(span, "ydb.finished")->value().bool_value());
                UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(span.status().code()), static_cast<int>(id == 100
                    ? NTraceProto::Status::STATUS_CODE_ERROR : NTraceProto::Status::STATUS_CODE_UNSET));
            } else {
                UNIT_ASSERT(FindAttribute(span, "ydb.finished")->value().bool_value());
                UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(span.status().code()),
                    static_cast<int>(NTraceProto::Status::STATUS_CODE_OK));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(attempts, 44);
    }

    Y_UNIT_TEST(ShardReadOverflowKeepsFailureAndCommonContext) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = RegisterUploader(runtime);
        for (const ui8 level : {10, 15}) {
            ClearUploader(*uploader);
            NKqp::TShardReadTrace reads;
            NWilson::TSpan parent(TComponentTracingLevels::TQueryProcessor::Detailed,
                NWilson::TTraceId::NewTraceId(level, 4095), "Read table", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
            for (ui64 id = 1; id <= NKqp::NQueryTraceSettings::MAX_ACTIVE_SHARD_READS; ++id) {
                reads.Start(parent, id, id);
            }
            auto traceId = reads.Start(parent, 999, 999);
            UNIT_ASSERT(traceId == parent.GetTraceId());
            NWilson::TSpan native(TComponentTracingLevels::TQueryProcessor::Detailed,
                std::move(traceId), "Datashard.Read", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
            native.EndError("read failed");
            reads.ReadResult(parent, 999, 2, 999, 0, Ydb::StatusIds::UNAVAILABLE, false);
            reads.Finish(parent);
            parent.EndError("read failed");
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT(uploader->BuildTraceTrees());
            UNIT_ASSERT_VALUES_EQUAL(uploader->Traces.size(), 1);
            AssertDescendant(*uploader, "Datashard.Read", "Read table");
            const auto* result = FindSpan(*uploader, "Read table");
            if (level == 10) {
                UNIT_ASSERT(!FindSpan(*uploader, "Read shard"));
                UNIT_ASSERT_VALUES_EQUAL(result->events_size(), 0);
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*result, "ydb.shard_reads_untraced")->value().int_value(), 1);
            UNIT_ASSERT(FindAttribute(*result, "ydb.shard_stats_incomplete")->value().bool_value());
            bool found = false;
            for (const auto& event : result->events()) {
                if (FindAttribute(event, "ydb.shard_id")->value().int_value() == 999) {
                    found = true;
                    UNIT_ASSERT(!FindAttribute(event, "ydb.duration.measured")->value().bool_value());
                    UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.status_code")->value().string_value(), "UNAVAILABLE");
                }
            }
            UNIT_ASSERT(found);
        }
    }

} // Y_UNIT_TEST_SUITE(TKqpTrace)

} // namespace NKikimr
