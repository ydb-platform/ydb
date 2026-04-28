#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log_settings.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/tracer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>
#include <util/system/env.h>
#include <util/system/event.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

using namespace NActors;
using namespace NActors::NTracing;

namespace {

enum ETraceBenchmarkEvents {
    EvTraceBenchPing = EventSpaceBegin(TEvents::ES_PRIVATE) + 128,
    EvTraceBenchPong,
};

struct TEvTraceBenchPing : TEventLocal<TEvTraceBenchPing, EvTraceBenchPing> {};
struct TEvTraceBenchPong : TEventLocal<TEvTraceBenchPong, EvTraceBenchPong> {};

ui64 GetEnvUi64(const char* name, ui64 defaultValue) {
    ui64 value = 0;
    const TString raw = GetEnv(name);
    if (raw && TryFromString<ui64>(raw, value)) {
        return value;
    }
    return defaultValue;
}

double Seconds(std::chrono::steady_clock::duration duration) {
    return std::chrono::duration<double>(duration).count();
}

struct TDirectBenchResult {
    TString Mode;
    ui32 Threads = 0;
    ui64 Events = 0;
    double DurationSeconds = 0;
};

TDirectBenchResult RunDirectTracerBenchmark(const TString& mode, ui32 threads, ui64 eventsPerThread, bool recording) {
    TSettings settings;
    settings.MaxThreads = threads + 8;
    settings.MaxBufferSizePerThread = 64 * 1024;

    auto tracer = CreateActorTracer(settings);
    if (recording) {
        UNIT_ASSERT(tracer->Start());
    }

    std::atomic<ui32> ready = 0;
    std::atomic<bool> start = false;
    std::vector<std::thread> workers;
    workers.reserve(threads);

    for (ui32 i = 0; i < threads; ++i) {
        workers.emplace_back([&, i] {
            const TActorId sender(1, 1000 + i);
            const TActorId recipient(1, 2000 + i);
            IEventHandle event(recipient, sender, new TEvTraceBenchPing());

            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            for (ui64 j = 0; j < eventsPerThread; ++j) {
                tracer->HandleSend(event);
            }
        });
    }

    while (ready.load(std::memory_order_acquire) < threads) {
        std::this_thread::yield();
    }

    const auto startedAt = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }
    const auto elapsed = std::chrono::steady_clock::now() - startedAt;

    if (recording) {
        UNIT_ASSERT(tracer->Stop());
    }

    return {
        .Mode = mode,
        .Threads = threads,
        .Events = eventsPerThread * threads,
        .DurationSeconds = Seconds(elapsed),
    };
}

struct TActorBenchCompletion {
    explicit TActorBenchCompletion(ui32 actors)
        : RemainingActors(actors)
    {}

    std::atomic<ui32> RemainingActors;
    TManualEvent Done;
};

class TEchoActor : public TActor<TEchoActor> {
public:
    TEchoActor()
        : TActor(&TThis::StateWork)
    {}

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTraceBenchPing, Handle);
        }
    }

    void Handle(TEvTraceBenchPing::TPtr& ev, const TActorContext& ctx) {
        ctx.Send(ev->Sender, new TEvTraceBenchPong());
    }
};

class TLoadActor : public TActorBootstrapped<TLoadActor> {
public:
    TLoadActor(TActorId target, ui64 requests, ui32 maxInFlight, TActorBenchCompletion& completion)
        : Target(target)
        , TotalRequests(requests)
        , MaxInFlight(maxInFlight)
        , Completion(completion)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        SendMore();
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTraceBenchPong, Handle);
        }
    }

    void Handle(TEvTraceBenchPong::TPtr&, const TActorContext&) {
        --InFlight;
        ++Received;
        if (Received == TotalRequests) {
            if (Completion.RemainingActors.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                Completion.Done.Signal();
            }
            return;
        }
        SendMore();
    }

    void SendMore() {
        while (Sent < TotalRequests && InFlight < MaxInFlight) {
            Send(Target, new TEvTraceBenchPing());
            ++Sent;
            ++InFlight;
        }
    }

private:
    TActorId Target;
    ui64 TotalRequests = 0;
    ui32 MaxInFlight = 0;
    TActorBenchCompletion& Completion;
    ui64 Sent = 0;
    ui64 Received = 0;
    ui32 InFlight = 0;
};

struct TActorBenchResult {
    TString Mode;
    ui32 Threads = 0;
    ui32 LoadActors = 0;
    ui64 Requests = 0;
    ui64 ActorMessages = 0;
    ui64 ExpectedTraceEvents = 0;
    double DurationSeconds = 0;
};

std::unique_ptr<TActorSystem> CreateBenchActorSystem(ui32 threads, bool recording) {
    auto setup = MakeHolder<TActorSystemSetup>();
    setup->NodeId = 1;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
    setup->Executors[0].Reset(new TBasicExecutorPool(0, threads, 100));
    setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(512, 0)));

    auto logSettings = MakeIntrusive<NLog::TSettings>(
        TActorId(1, "logger"),
        0,
        NLog::PRI_WARN);
    logSettings->TracerSettings.AutoStart = recording;
    logSettings->TracerSettings.MaxThreads = threads + 16;
    logSettings->TracerSettings.MaxBufferSizePerThread = 64 * 1024;

    return std::make_unique<TActorSystem>(setup, nullptr, logSettings);
}

TActorBenchResult RunActorSystemBenchmark(
    const TString& mode,
    ui32 threads,
    ui32 loadActors,
    ui64 requestsPerLoadActor,
    ui32 maxInFlight,
    bool recording)
{
    auto actorSystem = CreateBenchActorSystem(threads, recording);
    actorSystem->Start();

    std::vector<TActorId> echoes;
    echoes.reserve(loadActors);
    for (ui32 i = 0; i < loadActors; ++i) {
        echoes.push_back(actorSystem->Register(new TEchoActor()));
    }

    TActorBenchCompletion completion(loadActors);

    const auto startedAt = std::chrono::steady_clock::now();
    for (ui32 i = 0; i < loadActors; ++i) {
        actorSystem->Register(new TLoadActor(echoes[i], requestsPerLoadActor, maxInFlight, completion));
    }

    UNIT_ASSERT_C(completion.Done.WaitT(TDuration::Seconds(60)), "actor tracing benchmark timed out");
    const auto elapsed = std::chrono::steady_clock::now() - startedAt;

    auto* tracer = actorSystem->GetActorTracer();
    UNIT_ASSERT(tracer != nullptr);
    if (recording) {
        UNIT_ASSERT(tracer->Stop());
    }

    actorSystem->Stop();
    actorSystem->Cleanup();

    const ui64 requests = requestsPerLoadActor * loadActors;
    const ui64 actorMessages = requests * 2; // ping + pong
    const ui64 expectedTraceEvents = actorMessages * 2; // SendLocal + ReceiveLocal per actor message

    return {
        .Mode = mode,
        .Threads = threads,
        .LoadActors = loadActors,
        .Requests = requests,
        .ActorMessages = actorMessages,
        .ExpectedTraceEvents = expectedTraceEvents,
        .DurationSeconds = Seconds(elapsed),
    };
}

void PrintDirectBenchHeader() {
    Cerr << "direct_mode,threads,events,duration_s,events_per_s" << Endl;
}

void PrintDirectBenchResult(const TDirectBenchResult& result) {
    const double eventsPerSecond = static_cast<double>(result.Events) / result.DurationSeconds;
    Cerr << result.Mode << ','
              << result.Threads << ','
              << result.Events << ','
              << result.DurationSeconds << ','
              << eventsPerSecond
              << Endl;
}

void PrintActorBenchHeader() {
    Cerr << "actor_mode,threads,load_actors,requests,actor_messages,duration_s,messages_per_s,"
                 "expected_trace_events,trace_events_per_s,degradation_pct"
              << Endl;
}

void PrintActorBenchResult(const TActorBenchResult& result, double baselineSeconds) {
    const double messagesPerSecond = static_cast<double>(result.ActorMessages) / result.DurationSeconds;
    const double traceEventsPerSecond = static_cast<double>(result.ExpectedTraceEvents) / result.DurationSeconds;
    const double degradation = baselineSeconds > 0
        ? (result.DurationSeconds / baselineSeconds - 1.0) * 100.0
        : 0.0;

    Cerr << result.Mode << ','
              << result.Threads << ','
              << result.LoadActors << ','
              << result.Requests << ','
              << result.ActorMessages << ','
              << result.DurationSeconds << ','
              << messagesPerSecond << ','
              << result.ExpectedTraceEvents << ','
              << traceEventsPerSecond << ','
              << degradation
              << Endl;
}

void RunActorSystemOverheadCase(ui32 threads) {
    const ui64 requestsPerLoadActor = GetEnvUi64("YDB_TRACE_BENCH_REQUESTS_PER_ACTOR", 50'000);
    const ui32 maxInFlight = static_cast<ui32>(GetEnvUi64("YDB_TRACE_BENCH_INFLIGHT", 64));
    const ui32 loadActors = threads * 2;

    Cerr << "sep=," << Endl;
    PrintActorBenchHeader();

    const auto idle = RunActorSystemBenchmark(
        "idle", threads, loadActors, requestsPerLoadActor, maxInFlight, false);
    const auto recording = RunActorSystemBenchmark(
        "recording", threads, loadActors, requestsPerLoadActor, maxInFlight, true);

    PrintActorBenchResult(idle, idle.DurationSeconds);
    PrintActorBenchResult(recording, idle.DurationSeconds);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TracerBenchmark) {

    Y_UNIT_TEST(DirectWriteThroughput) {
        const ui64 eventsPerThread = GetEnvUi64("YDB_TRACE_BENCH_DIRECT_EVENTS", 1'000'000);
        const std::vector<ui32> threadCounts = {1, 4, 8, 16};

        Cerr << "sep=," << Endl;
        PrintDirectBenchHeader();

        for (ui32 threads : threadCounts) {
            PrintDirectBenchResult(RunDirectTracerBenchmark("idle", threads, eventsPerThread, false));
            PrintDirectBenchResult(RunDirectTracerBenchmark("recording", threads, eventsPerThread, true));
        }
    }

    Y_UNIT_TEST(ActorSystemOverheadSingleThread) {
        RunActorSystemOverheadCase(1);
    }

    Y_UNIT_TEST(ActorSystemOverheadMultiThread) {
        RunActorSystemOverheadCase(4);
    }

    Y_UNIT_TEST(ActorSystemOverhead8Threads) {
        RunActorSystemOverheadCase(8);
    }

    Y_UNIT_TEST(ActorSystemOverhead16Threads) {
        RunActorSystemOverheadCase(16);
    }
}
