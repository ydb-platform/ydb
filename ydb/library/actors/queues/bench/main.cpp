#include "queue.h"
#include "worker.h"
#include "bench_cases.h"
#include "queue_tracer.h"
#include "probes.h"

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/monlib/service/monservice.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/system/datetime.h>


using TMonSrvc = NMonitoring::TMonService2;
using namespace NActors;
using namespace NActors::NQueueBench;

void InitMonService(THolder<TMonSrvc>& monSrvc, int monPort)
{
    monSrvc.Reset(new TMonSrvc(monPort));
    NLwTraceMonPage::RegisterPages(monSrvc->GetRoot());
    NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(BENCH_TRACING_PROVIDER));
}

using TTracer = NTracing::TMPMCRingQueueBadPathTracer;
template <ui32 SIZE_BITS>
using TCasesWithTracer = TBenchCasesWithDurationAndThreads<TMPMCRingQueue<SIZE_BITS, TTracer>, TAdaptiveQueue<SIZE_BITS, TTracer>>;
using ICaseWithCollector = IBenchCaseWithDurationAndThreads<NTracing::TStatsCollector>;



using TDegradator = NTracing::TMPMCRingQueueDegradatorAndTracer<1024, 1, 60'000'000>;

template <>
thread_local ui64 TDegradator::TDegradator::SkipSteps = 0;
template <>
std::atomic_uint64_t TDegradator::TDegradator::InFlight = 0;
    
template <ui32 SIZE_BITS>
using TCasesWithDegradator = TBenchCasesWithDurationAndThreads<TMPMCRingQueue<SIZE_BITS, TTracer>, TAdaptiveQueue<SIZE_BITS, TTracer>>;


THashMap<TString,ICaseWithCollector*> Tests {
    {"Basic", static_cast<ICaseWithCollector*>(new TCasesWithTracer<20>::TBasicPushPop<NTracing::TStatsCollector, false>)},
    {"Producer1Consumer1", static_cast<ICaseWithCollector*>(new TCasesWithTracer<20>::TBasicProducingConsuming<NTracing::TStatsCollector, 1, 1, false>)},
    {"Producer1Consumer2", static_cast<ICaseWithCollector*>(new TCasesWithTracer<20>::TBasicProducingConsuming<NTracing::TStatsCollector, 1, 2, false>)},
    {"Producer2Consumer1", static_cast<ICaseWithCollector*>(new TCasesWithTracer<20>::TBasicProducingConsuming<NTracing::TStatsCollector, 2, 1, false>)},
    {"SingleProducer", static_cast<ICaseWithCollector*>(new TCasesWithTracer<20>::TSingleProducer<NTracing::TStatsCollector, false>)},
    {"SingleConsumer", static_cast<ICaseWithCollector*>(new TCasesWithTracer<20>::TSingleConsumer<NTracing::TStatsCollector, false>)},
};

THashMap<TString,ICaseWithCollector*> TestsWithSleep1Us {
    {"Basic", static_cast<ICaseWithCollector*>(new TCasesWithTracer<20>::TBasicPushPop<NTracing::TStatsCollector, true>)},
    {"Producer1Consumer1", static_cast<ICaseWithCollector*>(new TCasesWithTracer<20>::TBasicProducingConsuming<NTracing::TStatsCollector, 1, 1, true>)},
    {"Producer1Consumer2", static_cast<ICaseWithCollector*>(new TCasesWithTracer<20>::TBasicProducingConsuming<NTracing::TStatsCollector, 1, 2, true>)},
    {"Producer2Consumer1", static_cast<ICaseWithCollector*>(new TCasesWithTracer<20>::TBasicProducingConsuming<NTracing::TStatsCollector, 2, 1, true>)},
    {"SingleProducer", static_cast<ICaseWithCollector*>(new TCasesWithTracer<20>::TSingleProducer<NTracing::TStatsCollector, true>)},
    {"SingleConsumer", static_cast<ICaseWithCollector*>(new TCasesWithTracer<20>::TSingleConsumer<NTracing::TStatsCollector, true>)},
};

THashMap<TString,ICaseWithCollector*> TestsWithBlockedThread {
    {"Basic", static_cast<ICaseWithCollector*>(new TCasesWithDegradator<20>::TBasicPushPop<NTracing::TStatsCollector, false>)},
    {"Producer1Consumer1", static_cast<ICaseWithCollector*>(new TCasesWithDegradator<20>::TBasicProducingConsuming<NTracing::TStatsCollector, 1, 1, false>)},
    {"Producer1Consumer2", static_cast<ICaseWithCollector*>(new TCasesWithDegradator<20>::TBasicProducingConsuming<NTracing::TStatsCollector, 1, 2, false>)},
    {"Producer2Consumer1", static_cast<ICaseWithCollector*>(new TCasesWithDegradator<20>::TBasicProducingConsuming<NTracing::TStatsCollector, 2, 1, false>)},
    {"SingleProducer", static_cast<ICaseWithCollector*>(new TCasesWithDegradator<20>::TSingleProducer<NTracing::TStatsCollector, false>)},
    {"SingleConsumer", static_cast<ICaseWithCollector*>(new TCasesWithDegradator<20>::TSingleConsumer<NTracing::TStatsCollector, false>)},
};

THashMap<TString,ICaseWithCollector*> TestsWithSleep1UsAndBlockedThread {
    {"Basic", static_cast<ICaseWithCollector*>(new TCasesWithDegradator<20>::TBasicPushPop<NTracing::TStatsCollector, true>)},
    {"Producer1Consumer1", static_cast<ICaseWithCollector*>(new TCasesWithDegradator<20>::TBasicProducingConsuming<NTracing::TStatsCollector, 1, 1, true>)},
    {"Producer1Consumer2", static_cast<ICaseWithCollector*>(new TCasesWithDegradator<20>::TBasicProducingConsuming<NTracing::TStatsCollector, 1, 2, true>)},
    {"Producer2Consumer1", static_cast<ICaseWithCollector*>(new TCasesWithDegradator<20>::TBasicProducingConsuming<NTracing::TStatsCollector, 2, 1, true>)},
    {"SingleProducer", static_cast<ICaseWithCollector*>(new TCasesWithDegradator<20>::TSingleProducer<NTracing::TStatsCollector, true>)},
    {"SingleConsumer", static_cast<ICaseWithCollector*>(new TCasesWithDegradator<20>::TSingleConsumer<NTracing::TStatsCollector, true>)},
};


int main(int argc, char* argv[]) {
    //NLWTrace::StartLwtraceFromEnv();
    //signal(SIGPIPE, SIG_IGN);
    TString testName;
    int testDurationS = 600;
    int monPort = 7777;
    int lwtraceThreadLogSize = 1'000'000;
    int threadCount = 2;
    bool shortOutput = false;
    bool sleep1us = false;
    bool blockThread = false;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption(0, "mon-port", "port of monitoring service")
        .RequiredArgument("port")
        .StoreResult(&monPort, monPort);
    opts.AddLongOption('n', "name", "test name")
        .Required()
        .RequiredArgument("testname")
        .StoreResult(&testName, testName);
    opts.AddLongOption('d', "duration", "test duration")
        .RequiredArgument("seconds")
        .StoreResult(&testDurationS, testDurationS);
    opts.AddLongOption('t', "threads", "threads in the test")
        .RequiredArgument("thread-count")
        .StoreResult(&threadCount, threadCount);
    opts.AddLongOption("lwtrace-thread-log-size", "thread log size")
        .RequiredArgument("size")
        .StoreResult(&lwtraceThreadLogSize, lwtraceThreadLogSize);
    opts.AddLongOption("short-output", "reduce output")
        .NoArgument()
        .SetFlag(&shortOutput);
    opts.AddLongOption("sleep1us", "sleep 1us instead of spin-lock-pause")
        .NoArgument()
        .SetFlag(&sleep1us);
    opts.AddLongOption("block-thread", "every time one thread will sleep 1 minute")
        .NoArgument()
        .SetFlag(&blockThread);
    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    THolder<TMonSrvc> monSrvc;
    InitMonService(monSrvc, monPort);
    monSrvc->Start();
    NLWTrace::TManager* traceMngr = &NLwTraceMonPage::TraceManager();

    // init query lwtrace
    auto query = NLWTrace::TQuery();
    query.SetPerThreadLogSize(lwtraceThreadLogSize); // s -> ms
    auto& block = *query.AddBlocks();
    auto& probeDesc = *block.MutableProbeDesc();
    probeDesc.SetGroup("BenchTracing");
    auto action = block.AddAction();
    action->MutableLogAction();

    // init query threadpools stats
    auto queueStats = NLWTrace::TQuery();
    {
        queueStats.SetPerThreadLogSize(lwtraceThreadLogSize);
        auto& block = *queueStats.AddBlocks();
        auto& probeDesc = *block.MutableProbeDesc();
        probeDesc.SetGroup("ThreadPoolStats");
        auto action = block.AddAction();
        action->MutableLogAction();
    }

    auto *tests = &Tests;
    if (blockThread && sleep1us) {
        tests = &TestsWithSleep1UsAndBlockedThread;
    } else if (blockThread) {
        tests = &TestsWithBlockedThread;
    } else if (sleep1us) {
        tests = &TestsWithSleep1Us;
    }

    auto it = tests->find(testName);
    if (it == tests->end()) {
        Cerr << "Unknown test\n";
        return 1;
    }
    TString error = it->second->Validate(threadCount);
    if (error) {
        Cerr << "Error: " << error << Endl;
        return 1;
    }

    traceMngr->New(testName, query);
    auto stats = it->second->Run(TDuration::Seconds(testDurationS), threadCount);
    traceMngr->Stop(testName);
    auto threads = it->second->GetThreads(threadCount);
    std::visit([&](auto threads) {
        if constexpr (std::is_same_v<ui64, std::decay_t<decltype(threads)>>) {
            stats.Print(testDurationS, threads, shortOutput);
        } else {
            stats.Print(testDurationS, threads.ProducerThreads, threads.ConsumerThreads, shortOutput);
        }
    }, threads);
    return 0;
}