#include <ydb/library/yql/dq/task_scheduler/worker_pool.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <util/stream/output.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/system/datetime.h>
#include <util/system/rusage.h>

#include <array>
#include <deque>
#include <mutex>

using namespace NActors;
using namespace NYql::NDq;

namespace {

enum class EMode { Direct, ObserveOnly, Scheduled, WorkerPool };
enum EStage : ui32 { Producer, Aggregate, Consumer, Stages };

const char* Name(EMode mode) {
    switch (mode) {
        case EMode::Direct: return "direct_model";
        case EMode::ObserveOnly: return "observe_only";
        case EMode::Scheduled: return "scheduled";
        case EMode::WorkerPool: return "worker_pool";
    }
    Y_ABORT();
}

struct TConfig {
    ui32 Rows = 32768;
    ui32 Quantum = 64;
    ui32 ConsumerQuantum = 1;
    ui32 Noise = 1024;
    static constexpr ui32 InputCapacity = 256;
    static constexpr ui32 OutputCapacity = 8;
    static constexpr ui32 AggregateRows = 32;
};

struct TResult {
    ui64 Checksum = 0;
    ui64 ConsumedRows = 0;
    ui64 ConsumedGroups = 0;
    ui64 InputRows = 0;
    ui64 OutputRows = 0;
    ui64 Runs = 0;
    ui64 NoProgressRuns = 0;
    ui64 NoiseRuns = 0;
    ui64 RunCpuUs = 0;
    ui64 NoProgressCpuUs = 0;
    ui64 ProcessCpuUs = 0;
    ui64 ElapsedUs = 0;
    ui64 Notifications = 0;
    ui64 RunEvents = 0;
    ui64 PeakPayloadBytes = 0;
};

enum {
    EvWork = EventSpaceBegin(TEvents::ES_PRIVATE) + 1240,
    EvReady,
    EvParked,
    EvDone,
    EvBarrier,
    EvBarrierAck,
};

struct TEvWork : TEventLocal<TEvWork, EvWork> {};
struct TEvReady : TEventLocal<TEvReady, EvReady> {};
struct TEvParked : TEventLocal<TEvParked, EvParked> {};
struct TEvDone : TEventLocal<TEvDone, EvDone> {
    explicit TEvDone(std::exception_ptr error = {}) : Error(std::move(error)) {}
    std::exception_ptr Error;
};
struct TEvBarrier : TEventLocal<TEvBarrier, EvBarrier> {
    explicit TEvBarrier(bool stop) : Stop(stop) {}
    const bool Stop;
};
struct TEvBarrierAck : TEventLocal<TEvBarrierAck, EvBarrierAck> {};

// Payload queues and work are identical in every mode. The mutex lets the
// runtime use real worker threads without races in the synthetic operators.
struct TGraph {
    TActorSystem* System;
    const TActorId Edge;
    const EMode Mode;
    const TConfig Config;
    const std::shared_ptr<IDqTaskScheduler> Scheduler;
    std::shared_ptr<const TQueryGraphRegistration> GraphRegistration;
    std::mutex Mutex;
    std::array<TActorId, Stages> Actors;
    std::array<std::shared_ptr<TTaskSchedulingHandle>, Stages> Tasks;
    std::array<THashSet<TTaskDependencyId>, Stages> Subscriptions;
    std::array<bool, Stages> Done = {};
    std::deque<ui64> Input;
    std::deque<ui64> Output;
    ui64 Produced = 0;
    ui64 PartialSum = 0;
    ui32 PartialRows = 0;
    TResult Result;

    TGraph(TActorSystem* system, TActorId edge, EMode mode, TConfig config,
           std::shared_ptr<IDqTaskScheduler> scheduler)
        : System(system), Edge(edge), Mode(mode), Config(config), Scheduler(std::move(scheduler)) {}

    // Called with Mutex held, including by the benchmark driver.
    void Notify(EStage stage, TTaskDependencyId dependency, bool force = false) {
        // Preserve channel-level notification coalescing in all three modes.
        if (!force && !Subscriptions[stage].erase(dependency)) {
            return;
        }
        ++Result.Notifications;
        if (Tasks[stage]) {
            Tasks[stage]->Signal(dependency);
        }
        if (Mode != EMode::Scheduled && Mode != EMode::WorkerPool) {
            System->Send(Actors[stage], new TEvWork());
        }
    }

    void Request(EStage stage) {
        if (Mode == EMode::Scheduled || Mode == EMode::WorkerPool) {
            Tasks[stage]->RequestExecution();
        } else {
            System->Send(Actors[stage], new TEvWork());
        }
    }

    void UpdatePeak() {
        Result.PeakPayloadBytes = Max<ui64>(Result.PeakPayloadBytes, (Input.size() + Output.size()) * sizeof(ui64));
    }
};

class TStageOperation : public IDqTaskExecution {
public:
    TStageOperation(std::shared_ptr<TGraph> graph, EStage stage)
        : Graph(std::move(graph)), Stage(stage) {}
    void SetTask(std::shared_ptr<TTaskSchedulingHandle> task) { Task = std::move(task); }
    std::shared_ptr<const std::atomic<bool>> GetAvailability() const override { return Available; }
    bool IsAvailable() const override { return Available->load(); }
    bool TryAcquire() override {
        bool expected = true;
        return Available->compare_exchange_strong(expected, false);
    }
    void Release() override {
        Available->store(true);
        Graph->Scheduler->NotifyExecutionAvailable();
    }
    TTaskSliceResult RunSlice(const TTaskSliceBudget&) override {
        return {.Progress = Execute({}, true)};
    }
    void OnSliceFinished(TTaskSliceResult result) override {
        if (result.Error) {
            Task->Cancel();
            Graph->System->Send(Graph->Edge, new TEvDone(std::move(result.Error)));
        }
    }

private:
    void Await(TTaskDependencyId dependency) {
        Graph->Subscriptions[Stage].insert(dependency);
        if (Task) {
            Task->Await(dependency, Task->Observe(dependency));
        }
    }

    bool Produce(TTaskRunProgress& progress) {
        for (ui32 i = 0; i < Graph->Config.Quantum && Graph->Produced < Graph->Config.Rows
                         && Graph->Input.size() < TConfig::InputCapacity; ++i) {
            Graph->Input.push_back(++Graph->Produced);
            ++progress.OutputRows;
        }
        Graph->UpdatePeak();
        if (Graph->Produced == Graph->Config.Rows) {
            Graph->Done[Producer] = true;
            progress.Finished = true;
        }
        if (progress.OutputRows || progress.Finished) {
            Graph->Notify(Aggregate, InputDependency(1));
        }
        if (Graph->Done[Producer]) {
            return false;
        }
        if (Graph->Input.size() == TConfig::InputCapacity) {
            Await(OutputDependency(1));
            return false;
        }
        return true;
    }

    bool AggregateRows(TTaskRunProgress& progress) {
        Y_DEFER {
            if (progress.InputRows) {
                Graph->Notify(Producer, OutputDependency(1));
            }
        };
        for (ui32 i = 0; i < Graph->Config.Quantum; ++i) {
            const bool finalGroup = Graph->Done[Producer] && Graph->Input.empty() && Graph->PartialRows;
            if (Graph->PartialRows == TConfig::AggregateRows || finalGroup) {
                if (Graph->Output.size() == TConfig::OutputCapacity) {
                    Await(OutputDependency(2));
                    return false;
                }
                Graph->Output.push_back(Graph->PartialSum);
                Graph->PartialSum = 0;
                Graph->PartialRows = 0;
                ++progress.OutputRows;
                Graph->UpdatePeak();
                Graph->Notify(Consumer, InputDependency(2));
            } else if (!Graph->Input.empty()) {
                Graph->PartialSum += Graph->Input.front();
                Graph->Input.pop_front();
                ++Graph->PartialRows;
                ++progress.InputRows;
                ++Graph->Result.ConsumedRows;
            } else if (Graph->Done[Producer]) {
                Graph->Done[Aggregate] = true;
                progress.Finished = true;
                Graph->Notify(Consumer, InputDependency(2));
                break;
            } else {
                Await(InputDependency(1));
                break;
            }
        }
        if (Graph->Done[Aggregate]) {
            return false;
        }
        if (Graph->PartialRows == TConfig::AggregateRows && Graph->Output.size() == TConfig::OutputCapacity) {
            Await(OutputDependency(2));
            return false;
        }
        if (Graph->Input.empty() && !Graph->Done[Producer] && Graph->PartialRows < TConfig::AggregateRows) {
            Await(InputDependency(1));
            return false;
        }
        return true;
    }

    bool Consume(TTaskRunProgress& progress) {
        for (ui32 i = 0; i < Graph->Config.ConsumerQuantum && !Graph->Output.empty(); ++i) {
            Graph->Result.Checksum += Graph->Output.front();
            Graph->Output.pop_front();
            ++Graph->Result.ConsumedGroups;
            ++progress.InputRows;
        }
        if (progress.InputRows) {
            Graph->Notify(Aggregate, OutputDependency(2));
        }
        if (!Graph->Output.empty()) {
            return true;
        }
        if (Graph->Done[Aggregate]) {
            Graph->Done[Consumer] = true;
            progress.Finished = true;
        } else {
            Await(InputDependency(2));
        }
        return false;
    }

public:
    TTaskRunProgress Execute(std::optional<ui64> generation, bool startedByWorker = false) {
        std::lock_guard lock(Graph->Mutex);
        ++Graph->Result.RunEvents;
        if (Graph->Done[Stage]) {
            return {};
        }
        if (Task) {
            if (Graph->Mode == EMode::ObserveOnly) {
                Task->RequestExecution();
                generation = Task->TryDispatch();
            }
            if (!startedByWorker) {
                Y_ABORT_UNLESS(generation);
                if (!Task->Start(*generation)) {
                    return {};
                }
            }
            Task->BeginProbe();
        }
        const ui64 cpuStart = ThreadCPUTime();
        const auto start = TMonotonic::Now();
        TTaskRunProgress progress;
        progress.RunnerInvoked = true;
        bool runnable = false;
        switch (Stage) {
            case Producer: runnable = Produce(progress); break;
            case Aggregate: runnable = AggregateRows(progress); break;
            case Consumer: runnable = Consume(progress); break;
            case Stages: Y_ABORT();
        }
        progress.CpuTimeUs = ThreadCPUTime() - cpuStart;
        progress.ElapsedTimeUs = (TMonotonic::Now() - start).MicroSeconds();
        progress.BudgetExhausted = runnable;
        ++Graph->Result.Runs;
        Graph->Result.InputRows += progress.InputRows;
        Graph->Result.OutputRows += progress.OutputRows;
        Graph->Result.RunCpuUs += progress.CpuTimeUs;
        if (!progress.HasObservedProgress()) {
            ++Graph->Result.NoProgressRuns;
            Graph->Result.NoProgressCpuUs += progress.CpuTimeUs;
        }
        if (Task) {
            Task->Complete(progress);
            if (Graph->Done[Stage]) {
                Task->Cancel(true);
            }
        }
        if (runnable && Graph->Mode != EMode::Scheduled && Graph->Mode != EMode::WorkerPool) {
            Graph->Request(Stage);
        }
        if (!AnnouncedPark && Stage != Producer) {
            AnnouncedPark = true;
            Graph->System->Send(Graph->Edge, new TEvParked());
        }
        if (Graph->Done[Consumer] && Stage == Consumer) {
            Graph->System->Send(Graph->Edge, new TEvDone());
        }
        return progress;
    }

    const std::shared_ptr<TGraph> Graph;
    const EStage Stage;
    std::shared_ptr<TTaskSchedulingHandle> Task;
    bool AnnouncedPark = false;
    const std::shared_ptr<std::atomic<bool>> Available = std::make_shared<std::atomic<bool>>(true);
};

class TStageActor : public TActorBootstrapped<TStageActor> {
public:
    TStageActor(std::shared_ptr<TGraph> graph, EStage stage)
        : Graph(std::move(graph)), Stage(stage), Operation(std::make_shared<TStageOperation>(Graph, Stage)) {}
    void Bootstrap() {
        {
            std::lock_guard lock(Graph->Mutex);
            Graph->Actors[Stage] = SelfId();
            if (Graph->Scheduler) {
                Task = Graph->Scheduler->Register(SelfId(), Graph->Edge, Stage + 1);
                Graph->Tasks[Stage] = Task;
                Operation->SetTask(Task);
                if (Graph->Mode == EMode::WorkerPool) {
                    Graph->Scheduler->BindExecution(Graph->Edge, SelfId(), Operation);
                }
            }
        }
        Become(&TStageActor::StateWork);
        Send(Graph->Edge, new TEvReady());
    }
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvWork::EventType, Direct);
            hFunc(TEvTaskRunPermit, Permit);
            hFunc(TEvBarrier, Barrier);
        }
    }
private:
    void Direct() { Operation->Execute({}); }
    void Permit(TEvTaskRunPermit::TPtr& ev) {
        if (ev->Get()->Task == Task) {
            Operation->Execute(ev->Get()->Generation);
        }
    }
    void Barrier(TEvBarrier::TPtr& ev) {
        if (ev->Get()->Stop && Task) {
            Task->Cancel();
        }
        Send(Graph->Edge, new TEvBarrierAck());
        if (ev->Get()->Stop) {
            PassAway();
        }
    }
    const std::shared_ptr<TGraph> Graph;
    const EStage Stage;
    const std::shared_ptr<TStageOperation> Operation;
    std::shared_ptr<TTaskSchedulingHandle> Task;
};

class TComparisonRuntime : public TTestActorRuntimeBase {
public:
    TComparisonRuntime() : TTestActorRuntimeBase(1, true) { InitNodes(); }
    ~TComparisonRuntime() override { CleanupNodes(); }
};

ui64 ProcessCpuUs() {
    const auto usage = TRusage::Get();
    return (usage.Utime + usage.Stime).MicroSeconds();
}

TResult Run(TComparisonRuntime& runtime, EMode mode, const TConfig& config,
            const std::shared_ptr<IDqTaskScheduler>& scheduler) {
    const auto edge = runtime.AllocateEdgeActor();
    auto graph = std::make_shared<TGraph>(runtime.SingleSys(), edge, mode, config, scheduler);
    if (scheduler) {
        auto topology = std::make_shared<const TQueryTaskGraph>(
            TVector<TTaskGraphNode>{{1, 1, edge.NodeId(), 0, 1, true}, {2, 2, edge.NodeId(), 1, 1},
                                   {3, 3, edge.NodeId(), 1, 0, false, true}},
            TVector<TTaskGraphChannel>{{1, 1, 2, 0, 0}, {2, 2, 3, 0, 0}});
        graph->GraphRegistration = scheduler->RegisterGraph(edge, std::move(topology), {1, 2, 3});
    }
    for (ui32 stage = 0; stage < Stages; ++stage) {
        runtime.Register(new TStageActor(graph, static_cast<EStage>(stage)));
    }
    for (ui32 i = 0; i < Stages; ++i) {
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvReady>(edge, TDuration::Seconds(20)));
    }
    const auto start = TMonotonic::Now();
    const auto cpuStart = ProcessCpuUs();
    {
        std::lock_guard lock(graph->Mutex);
        graph->Request(Aggregate);
        graph->Request(Consumer);
    }
    for (ui32 i = 0; i < 2; ++i) {
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvParked>(edge, TDuration::Seconds(20)));
    }
    ui64 beforeNoise;
    {
        std::lock_guard lock(graph->Mutex);
        beforeNoise = graph->Result.NoProgressRuns;
        for (ui32 i = 0; i < config.Noise; ++i) {
            // Independent stale callbacks / previously probed inputs, not a
            // burst of pushes bypassing one channel's notification flag.
            graph->Notify(Aggregate, InputDependency(1000 + i), true);
            graph->Notify(Consumer, InputDependency(1000 + i), true);
        }
        for (auto stage : {Aggregate, Consumer}) {
            runtime.SingleSys()->Send(graph->Actors[stage], new TEvBarrier(false));
        }
    }
    for (ui32 i = 0; i < 2; ++i) {
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvBarrierAck>(edge, TDuration::Seconds(20)));
    }
    {
        std::lock_guard lock(graph->Mutex);
        graph->Result.NoiseRuns = graph->Result.NoProgressRuns - beforeNoise;
        graph->Request(Producer);
    }
    auto done = runtime.GrabEdgeEvent<TEvDone>(edge, TDuration::Seconds(30));
    UNIT_ASSERT(done);
    if (done->Get()->Error) { std::rethrow_exception(done->Get()->Error); }
    for (auto actor : graph->Actors) {
        runtime.SingleSys()->Send(actor, new TEvBarrier(true));
    }
    for (ui32 i = 0; i < Stages; ++i) {
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvBarrierAck>(edge, TDuration::Seconds(20)));
    }
    std::lock_guard lock(graph->Mutex);
    graph->Result.ProcessCpuUs = ProcessCpuUs() - cpuStart;
    graph->Result.ElapsedUs = (TMonotonic::Now() - start).MicroSeconds();
    UNIT_ASSERT_VALUES_EQUAL(graph->Result.Checksum, ui64(config.Rows) * (ui64(config.Rows) + 1) / 2);
    UNIT_ASSERT_VALUES_EQUAL(graph->Result.ConsumedRows, config.Rows);
    UNIT_ASSERT_VALUES_EQUAL(graph->Result.ConsumedGroups,
                             (ui64(config.Rows) + TConfig::AggregateRows - 1) / TConfig::AggregateRows);
    UNIT_ASSERT(graph->Result.PeakPayloadBytes <= (TConfig::InputCapacity + TConfig::OutputCapacity) * sizeof(ui64));
    return graph->Result;
}

struct TComparison {
    TComparisonRuntime Runtime;
    std::array<std::shared_ptr<IDqTaskScheduler>, 4> Schedulers;

    TComparison() {
        for (auto mode : {EMode::ObserveOnly, EMode::Scheduled, EMode::WorkerPool}) {
            Schedulers[static_cast<ui32>(mode)] = CreateDqTaskScheduler(Runtime.SingleSys(), 0,
                MakeIntrusive<NMonitoring::TDynamicCounters>(),
                {.ObserveOnly = mode == EMode::ObserveOnly, .Shards = 2, .MaxInFlightPerShard = 1,
                 .UseWorkerPool = mode == EMode::WorkerPool});
        }
    }

    TResult RunMode(EMode mode, const TConfig& config) {
        return Run(Runtime, mode, config, Schedulers[static_cast<ui32>(mode)]);
    }
};

void Print(IOutputStream& out, const TConfig& config, EMode mode, ui32 iteration, const TResult& result) {
    out << Name(mode) << ',' << iteration << ',' << config.Rows << ',' << config.Quantum << ','
         << config.ConsumerQuantum << ',' << config.Noise << ',' << result.Runs << ',' << result.NoProgressRuns << ','
         << result.NoiseRuns << ',' << result.InputRows << ',' << result.OutputRows << ','
         << double(result.InputRows) / result.Runs << ',' << double(result.OutputRows) / result.Runs << ','
         << result.RunEvents << ',' << result.Notifications << ',' << result.RunCpuUs << ','
         << result.NoProgressCpuUs << ',' << result.ProcessCpuUs << ',' << result.ElapsedUs << ','
         << result.PeakPayloadBytes << ',' << result.Checksum << Endl;
}

} // namespace

Y_UNIT_TEST_SUITE(DqTaskSchedulingComparison) {
    Y_UNIT_TEST(IrrelevantNotificationsDoNotRunBlockedTasks) {
        TComparison comparison;
        TConfig config;
        config.Rows = 4096;
        config.Noise = 256;
        const auto direct = comparison.RunMode(EMode::Direct, config);
        const auto observed = comparison.RunMode(EMode::ObserveOnly, config);
        const auto scheduled = comparison.RunMode(EMode::Scheduled, config);
        UNIT_ASSERT_VALUES_EQUAL(direct.NoiseRuns, 2 * config.Noise);
        UNIT_ASSERT_VALUES_EQUAL(observed.NoiseRuns, direct.NoiseRuns);
        UNIT_ASSERT_VALUES_EQUAL(scheduled.NoiseRuns, 0);
        const auto workers = comparison.RunMode(EMode::WorkerPool, config);
        UNIT_ASSERT_VALUES_EQUAL(workers.NoiseRuns, 0);
    }

    Y_UNIT_TEST(ReportComparison) {
        TConfig config;
        config.Rows = FromString<ui32>(GetTestParam("rows", "32768"));
        config.Quantum = FromString<ui32>(GetTestParam("quantum", "64"));
        config.ConsumerQuantum = FromString<ui32>(GetTestParam("consumer_quantum", "1"));
        config.Noise = FromString<ui32>(GetTestParam("noise", "1024"));
        const auto repeats = FromString<ui32>(GetTestParam("repeats", "3"));
        UNIT_ASSERT(config.Quantum && config.ConsumerQuantum && repeats);
        TComparison comparison;
        TConfig warmup = config;
        warmup.Rows = 1024;
        warmup.Noise = 0;
        for (auto mode : {EMode::Direct, EMode::ObserveOnly, EMode::Scheduled, EMode::WorkerPool}) {
            comparison.RunMode(mode, warmup);
        }
        const auto output = GetOutputPath() / "comparison.csv";
        output.Parent().MkDirs();
        TOFStream report(output.GetPath());
        const TStringBuf header = "mode,iteration,rows,quantum,consumer_quantum,noise,runs,no_progress_runs,noise_runs,input_rows,output_rows,"
                "input_rows_per_run,output_rows_per_run,run_events,notifications,"
                "run_cpu_us,no_progress_cpu_us,process_cpu_us,elapsed_us,peak_payload_bytes,checksum";
        Cerr << header << Endl;
        report << header << Endl;
        const auto noisy = config.Noise;
        TVector<ui32> scenarios = {0};
        if (noisy) {
            scenarios.push_back(noisy);
        }
        for (ui32 noise : scenarios) {
            config.Noise = noise;
            for (ui32 iteration = 0; iteration < repeats; ++iteration) {
                for (ui32 offset = 0; offset < 4; ++offset) {
                    const auto mode = static_cast<EMode>((iteration + offset) % 4);
                    const auto result = comparison.RunMode(mode, config);
                    Print(Cerr, config, mode, iteration, result);
                    Print(report, config, mode, iteration, result);
                }
            }
        }
    }
}
