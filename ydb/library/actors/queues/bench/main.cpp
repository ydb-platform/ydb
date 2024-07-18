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
template <ui32 SIZE_BITS>
using TNotReallyCasesWithTracer = TBenchCasesWithDurationAndThreads<TMPMCRingQueue<SIZE_BITS, TTracer>, TNotReallyAdaptiveQueue<SIZE_BITS, TTracer>>;
template <ui32 SIZE_BITS>
using TCasesWithTracerV1 = TBenchCasesWithDurationAndThreads<TMPMCRingQueueV1<SIZE_BITS, TTracer>, TIdAdaptor<TMPMCRingQueueV1<SIZE_BITS, TTracer>>>;
template <ui32 SIZE_BITS>
using TCasesWithTracerV2 = TBenchCasesWithDurationAndThreads<TMPMCRingQueueV2<SIZE_BITS, TTracer>, TIdAdaptor<TMPMCRingQueueV2<SIZE_BITS, TTracer>>>;
template <ui32 SIZE_BITS>
using TCasesWithTracerV3 = TBenchCasesWithDurationAndThreads<TMPMCRingQueueV3<SIZE_BITS, TTracer>, TIdAdaptor<TMPMCRingQueueV3<SIZE_BITS, TTracer>>>;

using ICaseWithCollector = IBenchCaseWithDurationAndThreads<NTracing::TStatsCollector>;


using TDegradator = NTracing::TMPMCRingQueueDegradatorAndTracer<1024, 1, 60'000'000>;

template <>
thread_local ui64 TDegradator::TDegradator::SkipSteps = 0;
template <>
std::atomic_uint64_t TDegradator::TDegradator::InFlight = 0;
    
template <ui32 SIZE_BITS>
using TCasesWithDegradator = TBenchCasesWithDurationAndThreads<TMPMCRingQueue<SIZE_BITS, TDegradator>, TAdaptiveQueue<SIZE_BITS, TDegradator>>;
template <ui32 SIZE_BITS>
using TNotReallyCasesWithDegradator = TBenchCasesWithDurationAndThreads<TMPMCRingQueue<SIZE_BITS, TDegradator>, TNotReallyAdaptiveQueue<SIZE_BITS, TDegradator>>;
template <ui32 SIZE_BITS>
using TCasesWithDegradatorV1 = TBenchCasesWithDurationAndThreads<TMPMCRingQueueV1<SIZE_BITS, TDegradator>, TIdAdaptor<TMPMCRingQueueV1<SIZE_BITS, TDegradator>>>;
template <ui32 SIZE_BITS>
using TCasesWithDegradatorV2 = TBenchCasesWithDurationAndThreads<TMPMCRingQueueV2<SIZE_BITS, TDegradator>, TIdAdaptor<TMPMCRingQueueV2<SIZE_BITS, TDegradator>>>;
template <ui32 SIZE_BITS>
using TCasesWithDegradatorV3 = TBenchCasesWithDurationAndThreads<TMPMCRingQueueV3<SIZE_BITS, TDegradator>, TIdAdaptor<TMPMCRingQueueV3<SIZE_BITS, TDegradator>>>;

template <typename TCases, bool Sleep>
THashMap<TString,ICaseWithCollector*> MakeTests() {
    return {
        {"Basic", static_cast<ICaseWithCollector*>(new TCases::template TBasicPushPop<NTracing::TStatsCollector, Sleep>)},
        {"Producer1Consumer1", static_cast<ICaseWithCollector*>(new TCases::template TBasicProducingConsuming<NTracing::TStatsCollector, 1, 1, Sleep>)},
        {"Producer1Consumer2", static_cast<ICaseWithCollector*>(new TCases::template TBasicProducingConsuming<NTracing::TStatsCollector, 1, 2, Sleep>)},
        {"Producer2Consumer1", static_cast<ICaseWithCollector*>(new TCases::template TBasicProducingConsuming<NTracing::TStatsCollector, 2, 1, Sleep>)},
        {"SingleProducer", static_cast<ICaseWithCollector*>(new TCases::template TSingleProducer<NTracing::TStatsCollector, Sleep>)},
        {"SingleConsumer", static_cast<ICaseWithCollector*>(new TCases::template TSingleConsumer<NTracing::TStatsCollector, Sleep>)},
    };
}

struct TTests {
    THashMap<TString, THashMap<TString,ICaseWithCollector*>> Tests;

    template <typename TCases, typename TDefradatorCases>
    static TTests MakeTestSuite() {
        return TTests {
            {
                {"", MakeTests<TCases, false>()}, 
                {"sleep1us", MakeTests<TCases, true>()}, 
                {"blocked-thread", MakeTests<TDefradatorCases, false>()}, 
                {"blocked-thread,sleep1us", MakeTests<TDefradatorCases, true>()}, 
            }
        };
    }
};

THashMap<TString, TTests> Tests {
    {"v0", TTests::MakeTestSuite<TCasesWithTracer<20>, TCasesWithDegradator<20>>()},
    {"v0-not-really", TTests::MakeTestSuite<TNotReallyCasesWithTracer<20>, TNotReallyCasesWithDegradator<20>>()},
    {"v1", TTests::MakeTestSuite<TCasesWithTracerV1<20>, TCasesWithDegradatorV1<20>>()},
    {"v2", TTests::MakeTestSuite<TCasesWithTracerV2<20>, TCasesWithDegradatorV2<20>>()},
    {"v3", TTests::MakeTestSuite<TCasesWithTracerV3<20>, TCasesWithDegradatorV3<20>>()},
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
    TString queueType = "v0";

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
    opts.AddLongOption("queue-type", "use second version of mpmc-ring-queue")
        .RequiredArgument("type")
        .StoreResult(&queueType);
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

    auto testSuiteIt = Tests.find(queueType);
    if (testSuiteIt == Tests.end()) {
        Cerr << "Unknown queue type\n";
        return 1; 
    }
    auto testSuite = testSuiteIt->second;
    TString attrs = "";
    if (blockThread) {
        attrs += "blocked-thread";
    }
    if (sleep1us) {
        if (attrs) {
            attrs += ",";
        }
        attrs += "sleep1us";
    }

    auto *tests = &testSuite.Tests[attrs];
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