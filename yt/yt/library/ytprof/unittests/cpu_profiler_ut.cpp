#include <dlfcn.h>

#include <gtest/gtest.h>

#include <library/cpp/testing/common/env.h>

#include <library/cpp/yt/memory/new.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/library/ytprof/cpu_profiler.h>
#include <yt/yt/library/ytprof/symbolize.h>
#include <yt/yt/library/ytprof/profile.h>
#include <yt/yt/library/ytprof/external_pprof.h>

#include <util/string/cast.h>
#include <util/stream/file.h>
#include <util/datetime/base.h>
#include <util/system/shellcommand.h>

#include <yt/yt/core/concurrency/thread_pool.h>

namespace NYT::NYTProf {
namespace {

using namespace NConcurrency;
using namespace NTracing;

using TSampleFilter = TCpuProfilerOptions::TSampleFilter;

////////////////////////////////////////////////////////////////////////////////

template <size_t Index>
Y_NO_INLINE void BurnCpu()
{
    THash<TString> hasher;
    ui64 value = 0;
    for (int i = 0; i < 10000000; i++) {
        value += hasher(ToString(i));
    }
    EXPECT_NE(Index, value);
}

static std::atomic<int> Counter{0};

struct TNoTailCall
{
    ~TNoTailCall()
    {
        Counter++;
    }
};

static Y_NO_INLINE void StaticFunction()
{
    TNoTailCall noTail;
    BurnCpu<0>();
}

void RunUnderProfiler(
    const TString& name,
    std::function<void()> work,
    bool checkSamples = true,
    const std::vector<TSampleFilter>& filters = {},
    bool expectEmpty = false)
{
    TCpuProfilerOptions options;
    options.SampleFilters = filters;
    options.SamplingFrequency = 100000;
    options.RecordActionRunTime = true;

#ifdef YTPROF_DEBUG_BUILD
    options.SamplingFrequency = 100;
#endif

    TCpuProfiler profiler(options);

    profiler.Start();

    work();

    profiler.Stop();

    auto profile = profiler.ReadProfile();
    if (checkSamples) {
        ASSERT_EQ(expectEmpty, profile.sampleSize() == 0);
    }

    Symbolize(&profile, true);
    AddBuildInfo(&profile, TBuildInfo::GetDefault());
    SymbolizeByExternalPProf(&profile, TSymbolizationOptions{
        .TmpDir = GetOutputPath(),
        .KeepTmpDir = true,
        .RunTool = [] (const std::vector<TString>& args) {
            TShellCommand command{args[0], TList<TString>{args.begin()+1, args.end()}};
            command.Run();

            EXPECT_TRUE(command.GetExitCode() == 0)
                << command.GetError();
        },
    });

    TFileOutput output(GetOutputPath() / name);
    WriteProfile(&output, profile);
    output.Finish();
}

class TCpuProfilerTest
    : public ::testing::Test
{
    void SetUp() override
    {
        if (!IsProfileBuild()) {
            GTEST_SKIP() << "rebuild with --build=profile";
        }
    }
};

TEST_F(TCpuProfilerTest, SingleThreadRun)
{
    RunUnderProfiler("single_thread.pb.gz", [] {
        BurnCpu<0>();
    });
}

TEST_F(TCpuProfilerTest, MultipleThreads)
{
    RunUnderProfiler("multiple_threads.pb.gz", [] {
        std::thread t1([] {
            BurnCpu<1>();
        });

        std::thread t2([] {
            BurnCpu<2>();
        });

        t1.join();
        t2.join();
    });
}

TEST_F(TCpuProfilerTest, StaticFunction)
{
    RunUnderProfiler("static_function.pb.gz", [] {
        StaticFunction();
    });
}

Y_NO_INLINE void RecursiveFunction(int n)
{
    TNoTailCall noTail;
    if (n == 0) {
        BurnCpu<0>();
    } else {
        RecursiveFunction(n-1);
    }
}

TEST_F(TCpuProfilerTest, DeepRecursion)
{
    RunUnderProfiler("recursive_function.pb.gz", [] {
        RecursiveFunction(1024);
    });
}

TEST_F(TCpuProfilerTest, DlOpen)
{
    RunUnderProfiler("dlopen.pb.gz", [] {
        auto libraryPath = BinaryPath("yt/yt/library/ytprof/unittests/testso/libtestso.so");

        auto dl = dlopen(libraryPath.c_str(), RTLD_LAZY);
        ASSERT_TRUE(dl);

        auto sym = dlsym(dl, "CallNext");
        ASSERT_TRUE(sym);

        auto callNext = reinterpret_cast<void(*)(void(*)())>(sym);
        callNext(BurnCpu<0>);
    });
}

TEST_F(TCpuProfilerTest, DlClose)
{
    RunUnderProfiler("dlclose.pb.gz", [] {
        auto libraryPath = BinaryPath("yt/yt/library/ytprof/unittests/testso1/libtestso1.so");

        auto dl = dlopen(libraryPath.c_str(), RTLD_LAZY);
        ASSERT_TRUE(dl);

        auto sym = dlsym(dl, "CallOtherNext");
        ASSERT_TRUE(sym);

        auto callNext = reinterpret_cast<void(*)(void(*)())>(sym);
        callNext(BurnCpu<0>);

        ASSERT_EQ(dlclose(dl), 0);
    });
}

void ReadUrandom()
{
    TIFStream input("/dev/urandom");

    std::array<char, 1 << 20> buffer;

    for (int i = 0; i < 100; i++) {
        input.Read(buffer.data(), buffer.size());
    }
}

TEST_F(TCpuProfilerTest, Syscalls)
{
    RunUnderProfiler("syscalls.pb.gz", [] {
        ReadUrandom();
    });
}

TEST_F(TCpuProfilerTest, VDSO)
{
    RunUnderProfiler("vdso.pb.gz", [] {
        auto now = TInstant::Now();
        while (TInstant::Now() < now + TDuration::MilliSeconds(100))
        { }
    }, false);
}

TEST_F(TCpuProfilerTest, ProfilerTags)
{
    auto userTag = New<TProfilerTag>("user", "prime");
    auto intTag = New<TProfilerTag>("block_size", 1024);

    RunUnderProfiler("tags.pb.gz", [&] {
        {
            TCpuProfilerTagGuard guard(userTag);
            BurnCpu<0>();
        }
        {
            TCpuProfilerTagGuard guard(intTag);
            BurnCpu<1>();
        }
        {
            TCpuProfilerTagGuard guard(userTag);
            TCpuProfilerTagGuard secondGuard(intTag);
            BurnCpu<2>();
        }
    });
}

TEST_F(TCpuProfilerTest, MultipleProfilers)
{
    TCpuProfiler profiler, secondProfiler;

    profiler.Start();
    EXPECT_THROW(secondProfiler.Start(), std::exception);
}

TEST_F(TCpuProfilerTest, TraceContext)
{
    RunUnderProfiler("trace_context.pb.gz", [] {
        auto actionQueue = New<TActionQueue>("CpuProfileTest");

        BIND([] {
            auto rootTraceContext = TTraceContext::NewRoot("");
            rootTraceContext->AddProfilingTag("user", "prime");
            TCurrentTraceContextGuard guard(rootTraceContext);

            auto asyncSubrequest = BIND([&] {
                TChildTraceContextGuard guard("");
                AnnotateTraceContext([] (const auto traceContext) {
                    traceContext->AddProfilingTag("table", "//foo");
                });

                BurnCpu<0>();
            })
                .AsyncVia(GetCurrentInvoker())
                .Run();

            BurnCpu<1>();
            WaitFor(asyncSubrequest)
                .ThrowOnError();
        })
            .AsyncVia(actionQueue->GetInvoker())
            .Run()
            .Get();

        actionQueue->Shutdown();
    });
}

TEST_F(TCpuProfilerTest, SlowActions)
{
    static const TString WorkerThreadName = "Heavy";
    static const auto TraceThreshold = TDuration::MilliSeconds(50);

    const std::vector<TSampleFilter> filters = {
        GetActionMinExecTimeFilter(TraceThreshold),
    };

    auto busyWait = [] (TDuration duration) {
        auto now = TInstant::Now();
        while (TInstant::Now() < now + duration)
        { }
    };

    auto traceContext = NTracing::TTraceContext::NewRoot("Test");

    const bool ExpectEmptyTraces = true;

    auto threadPool = CreateThreadPool(2, WorkerThreadName);

    // No slow actions.
    RunUnderProfiler("slow_actions_empty.pb.gz", [&] {
        NTracing::TTraceContextGuard guard(traceContext);
        auto future = BIND(busyWait, TraceThreshold / 2)
            .AsyncVia(threadPool->GetInvoker())
            .Run();
        future.Get();
    },
    true,
    filters,
    ExpectEmptyTraces);

    // Slow actions = non empty traces.
    RunUnderProfiler("slow_actions.pb.gz", [&] {
        NTracing::TTraceContextGuard guard(traceContext);
        auto future = BIND(busyWait, TraceThreshold * 3)
            .AsyncVia(threadPool->GetInvoker())
            .Run();
        future.Get();
    },
    true,
    filters);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTProf
