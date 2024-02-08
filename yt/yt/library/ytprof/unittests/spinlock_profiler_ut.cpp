#include <gtest/gtest.h>

#include <library/cpp/testing/common/env.h>

#include <yt/yt/library/ytprof/spinlock_profiler.h>
#include <yt/yt/library/ytprof/symbolize.h>
#include <yt/yt/library/ytprof/profile.h>
#include <yt/yt/library/ytprof/external_pprof.h>

#include <tcmalloc/malloc_extension.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/string/cast.h>
#include <util/stream/file.h>
#include <util/datetime/base.h>
#include <util/system/compiler.h>
#include <util/system/shellcommand.h>
#include <util/thread/lfstack.h>
#include <util/generic/size_literals.h>

namespace NYT::NYTProf {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <class TProfiler>
void RunUnderProfiler(const TString& name, std::function<void()> work, bool checkSamples = true)
{
    TSpinlockProfilerOptions options;
    options.ProfileFraction = 10;

    TProfiler profiler(options);

    profiler.Start();

    work();

    profiler.Stop();

    auto profile = profiler.ReadProfile();
    if (checkSamples) {
        ASSERT_NE(0, profile.sample_size());
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

class TSpinlockProfilerTest
    : public ::testing::Test
{
    void SetUp() override
    {
        if (!IsProfileBuild()) {
            GTEST_SKIP() << "rebuild with --build=profile";
        }
    }
};

TEST_F(TSpinlockProfilerTest, PageHeapLock)
{
    RunUnderProfiler<TSpinlockProfiler>("pageheap_lock.pb.gz", [] {
        std::atomic<bool> Stop = false;

        std::thread release([&] {
            while (!Stop) {
                tcmalloc::MallocExtension::ReleaseMemoryToSystem(1_GB);
            }
        });

        std::vector<std::thread> allocators;
        for (int i = 0; i < 16; i++) {
            allocators.emplace_back([&] {
                while (!Stop) {
                    auto ptr = malloc(4_MB);
                    DoNotOptimizeAway(ptr);
                    free(ptr);
                }
            });
        }

        Sleep(TDuration::Seconds(5));
        Stop = true;

        release.join();
        for (auto& t : allocators) {
            t.join();
        }
    });
}


TEST_F(TSpinlockProfilerTest, TransferCacheLock)
{
    RunUnderProfiler<TSpinlockProfiler>("transfer_cache_lock.pb.gz", [] {
        std::atomic<bool> Stop = false;

        TLockFreeStack<int> stack;
        std::thread producer([&] {
            while (!Stop) {
                stack.Enqueue(1);
            }
        });

        std::thread consumer([&] {
            while (!Stop) {
                int value;
                stack.Dequeue(&value);
            }
        });

        Sleep(TDuration::Seconds(5));

        Stop = true;
        producer.join();
        consumer.join();
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSpinlockProfilerTest, YTLocks)
{
    RunUnderProfiler<TBlockingProfiler>("ytlock.pb.gz", [] {
        std::atomic<bool> Stop = false;

        NThreading::TSpinLock lock;
        std::thread slow([&] {
            while (!Stop) {
                lock.Acquire();
                Sleep(TDuration::MilliSeconds(10));
                lock.Release();
                Sleep(TDuration::MilliSeconds(10));
            }
        });

        std::thread fast([&] {
            while (!Stop) {
                lock.Acquire();
                lock.Release();
            }
        });

        Sleep(TDuration::Seconds(5));

        Stop = true;
        slow.join();
        fast.join();
    });
}

////////////////////////////////////////////////////////////////////////////////


} // namespace
} // namespace NYT::NYTProf
