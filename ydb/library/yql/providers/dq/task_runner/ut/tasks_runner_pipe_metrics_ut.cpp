#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_pipe.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>

#include <util/generic/scope.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/strip.h>
#include <util/system/fs.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <future>
#include <iterator>
#include <stdexcept>

#include <sys/wait.h>

namespace NYql::NTaskRunnerProxy {
namespace {

void WaitUntil(const std::function<bool()>& ready, const char* error = "Timed out waiting for runner metrics")
{
    const auto deadline = TInstant::Now() + TDuration::Seconds(10);
    while (!ready()) {
        if (TInstant::Now() >= deadline) {
            throw std::runtime_error(error);
        }
        Sleep(TDuration::MilliSeconds(10));
    }
}

class TEmptyFileCache: public IFileCache {
public:
    std::atomic<bool> FailGetDir = false;

    TEmptyFileCache(TString directory, std::function<void()> beforeAcquire, std::function<void()> beforeGetDir = {})
        : Directory_(std::move(directory))
        , BeforeAcquire_(std::move(beforeAcquire))
        , BeforeGetDir_(std::move(beforeGetDir))
    { }

    void AddFile(const TString&, const TString&) override { }
    TMaybe<TString> FindFile(const TString&) override { return Nothing(); }
    TMaybe<TString> AcquireFile(const TString&) override
    {
        BeforeAcquire_();
        return Nothing();
    }
    void ReleaseFile(const TString&) override { }
    bool Contains(const TString&) override { return false; }
    void Walk(const std::function<void(const TString&)>&) override { }
    i64 FreeDiskSize() override { return 0; }
    ui64 UsedDiskSize() override { return 0; }
    TString GetDir() override
    {
        if (BeforeGetDir_) {
            BeforeGetDir_();
        }
        if (FailGetDir.load()) {
            throw std::runtime_error("Cannot get executor directory");
        }
        return Directory_;
    }

private:
    const TString Directory_;
    const std::function<void()> BeforeAcquire_;
    const std::function<void()> BeforeGetDir_;
};

class TPipeMetricsFixture {
public:
    const NMonitoring::TDynamicCounterPtr Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();

    explicit TPipeMetricsFixture(int poolSize, int destroyExitCode = 0, bool blockDestroy = false)
    {
        (Directory_.Path() / "pids").MkDirs();
        const auto porto = Directory_.Path() / "porto";
        {
            TFileOutput script(porto.GetPath());
            script << "#!/bin/sh\n"
                << "case \"$1\" in\n"
                << "exec) printf '%s\\n' \"$$\" > '" << Directory_.Name() << "/pids/'\"${4##*/}\"; exec /bin/sleep 60;;\n"
                << "destroy)\n"
                << "  : > '" << Directory_.Name() << "/destroy-entered'\n"
                << "  n=0\n"
                << "  while [ ! -e '" << Directory_.Name() << "/release' ] && [ \"$n\" -lt 300 ]; do\n"
                << "    /bin/sleep 0.1; n=$((n+1))\n"
                << "  done\n"
                << "  exit " << destroyExitCode << ";;\n"
                << "*) exit 1;;\nesac\n";
        }
        UNIT_ASSERT_VALUES_EQUAL(Chmod(porto.c_str(), MODE0755), 0);
        if (!blockDestroy) {
            ReleaseDestroy();
        }
        FileCache_ = MakeIntrusive<TEmptyFileCache>(Directory_.Name(), [this] { WaitForProcessPids(); });
        TPipeFactoryOptions options;
        options.ExecPath = "/bin/sleep";
        options.FileCache = FileCache_;
        options.EnablePorto = true;
        options.EnablePortoAnonLimitRaiseBeforeDestroy = false;
        options.PortoCtlPath = porto.GetPath();
        options.MaxProcesses = poolSize;
        options.Counters = Counters;
        Factory_ = CreatePipeFactory(options);
        WaitForProcessPids();
    }

    ~TPipeMetricsFixture()
    {
        ReleaseDestroy();
        Factory_ = nullptr;
        // Unused warm owners and failed assertions still need fixture cleanup.
        const auto deadline = TInstant::Now() + TDuration::Seconds(10);
        TVector<TFsPath> files;
        TVector<int> pids;
        do {
            files.clear();
            pids.clear();
            (Directory_.Path() / "pids").List(files);
            for (const auto& file : files) {
                int pid = 0;
                if (TryFromString(Strip(TFileInput(file).ReadAll()), pid) && pid > 0) {
                    pids.push_back(pid);
                }
            }
            if (std::ssize(pids) >= Counters->GetCounter("PortoContainersStarted", /*derivative=*/true)->Val()) {
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        } while (TInstant::Now() < deadline);
        for (int pid : pids) {
            int status = 0;
            int result;
            do {
                result = waitpid(pid, &status, WNOHANG);
            } while (result == -1 && errno == EINTR);
            if (result == 0) {
                kill(pid, SIGKILL);
                while (waitpid(pid, &status, 0) == -1 && errno == EINTR) { }
            }
        }
    }

    void ReleaseDestroy()
    {
        TFileOutput(Directory_.Path() / "release").Finish();
    }

    void ResetFactory()
    {
        Factory_ = nullptr;
    }

    void FailNextStart()
    {
        FileCache_->FailGetDir.store(true);
    }

    void WaitForDestroy()
    {
        WaitUntil([&] { return (Directory_.Path() / "destroy-entered").Exists(); });
    }

    void AssertReaped(int slot)
    {
        const int pid = FromString<int>(Strip(TFileInput(
            Directory_.Path() / "pids" / ("Slot-" + ToString(slot))).ReadAll()));
        int status = 0;
        int result;
        do {
            result = waitpid(pid, &status, WNOHANG);
        } while (result == -1 && errno == EINTR);
        const int error = errno;
        UNIT_ASSERT_VALUES_EQUAL_C(result, -1, "Executor was not reaped by cleanup");
        UNIT_ASSERT_VALUES_EQUAL(error, ECHILD);
    }

    void PrepareTaskWithMissingFile(bool changeKey = false, bool useNewApi = false)
    {
        Yql::DqsProto::TTaskMeta meta;
        auto* setting = meta.AddSettings();
        setting->SetName("_EnablePorto");
        setting->SetValue("true");
        if (changeKey) {
            setting = meta.AddSettings();
            setting->SetName("_PortoMemoryLimit");
            setting->SetValue("1024");
        }
        auto* file = meta.AddFiles();
        file->SetObjectType(Yql::DqsProto::TFile::EUSER_FILE);
        file->SetObjectId("missing");
        NDqProto::TDqTask task;
        task.MutableMeta()->PackFrom(meta);
        if (useNewApi) {
            Factory_->Get(/*alloc*/ nullptr, NDq::TDqTaskSettings(&task), NDqProto::DQ_STATS_MODE_NONE);
        } else {
            Factory_->GetOld(nullptr, NDq::TDqTaskSettings(&task));
        }
    }

    void FailTaskPreparation(bool changeKey = false, bool useNewApi = false)
    {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            PrepareTaskWithMissingFile(changeKey, useNewApi),
            std::runtime_error,
            FileCache_->FailGetDir.load() ? "Cannot get executor directory" : "Cannot find object `missing` in cache");
    }

    i64 Count(const TString& name) const
    {
        const auto counter = Counters->FindCounter(name);
        if (!counter) {
            throw std::runtime_error(("Missing runner counter: " + name).c_str());
        }
        return counter->Val();
    }

private:
    TTempDir Directory_;
    TIntrusivePtr<TEmptyFileCache> FileCache_;
    IProxyFactory::TPtr Factory_;

    void WaitForProcessPids()
    {
        const auto expected = Count("PortoContainersStarted");
        WaitUntil([&] {
            for (i64 slot = 1; slot <= expected; ++slot) {
                const auto file = Directory_.Path() / "pids" / ("Slot-" + ToString(slot));
                int pid = 0;
                if (!file.Exists() || !TryFromString(Strip(TFileInput(file).ReadAll()), pid) || pid <= 0) {
                    return false;
                }
            }
            return true;
        }, "Timed out waiting for executor PIDs");
    }
};

void CheckEofExecutorReaped(bool failColdStart, bool useNewApi, bool blockCleanup = false)
{
    TTempDir directory;
    const auto pidDirectory = directory.Path() / "pids";
    pidDirectory.MkDirs();
    const auto executable = directory.Path() / "executor";
    {
        TFileOutput script(executable);
        script << "#!/bin/sh\n"
            << "printf '%s\\n' \"$$\" > \"$PID_DIRECTORY/${PWD##*/}\"\n"
            << "exec /bin/cat > /dev/null\n";
    }
    UNIT_ASSERT_VALUES_EQUAL(Chmod(executable.c_str(), MODE0755), 0);

    const int processCount = failColdStart ? 2 : 1;
    TVector<int> pids(processCount, 0);
    auto readPids = [&] {
        bool ready = true;
        for (int slot = 1; slot <= processCount; ++slot) {
            const auto file = pidDirectory / ("Slot-" + ToString(slot));
            if (!file.Exists() || !TryFromString(Strip(TFileInput(file).ReadAll()), pids[slot - 1]) || pids[slot - 1] <= 0) {
                ready = false;
            }
        }
        return ready;
    };
    std::atomic<bool> holdRefill = false;
    std::atomic<bool> refillEntered = false;
    auto fileCache = MakeIntrusive<TEmptyFileCache>(directory.Name(), [&] {
        WaitUntil(readPids, "EOF executor did not publish its PID");
        if (blockCleanup) {
            WaitUntil([&] { return refillEntered.load(); }, "Refill did not block the scheduler");
        }
    }, [&] {
        if (holdRefill.load()) {
            refillEntered.store(true);
            WaitUntil([&] { return !holdRefill.load(); }, "Refill was not released");
        }
    });
    IProxyFactory::TPtr factory;
    Y_DEFER {
        holdRefill.store(false);
        if (refillEntered.load()) {
            const auto deadline = TInstant::Now() + TDuration::Seconds(5);
            while (TInstant::Now() < deadline) {
                try {
                    const auto file = pidDirectory / "Slot-2";
                    int pid = 0;
                    if (file.Exists() && TryFromString(Strip(TFileInput(file).ReadAll()), pid) && pid > 0) {
                        break;
                    }
                } catch (...) { }
                Sleep(TDuration::MilliSeconds(10));
            }
        }
        factory = nullptr;
        try {
            TVector<TFsPath> files;
            pidDirectory.List(files);
            for (const auto& file : files) {
                int pid = 0;
                if (!TryFromString(Strip(TFileInput(file).ReadAll()), pid) || pid <= 0) {
                    continue;
                }
                int status = 0;
                int result;
                do {
                    result = waitpid(pid, &status, WNOHANG);
                } while (result == -1 && errno == EINTR);
                if (result == 0) {
                    kill(pid, SIGKILL);
                    while (waitpid(pid, &status, 0) == -1 && errno == EINTR) { }
                }
            }
        } catch (...) { }
    };

    TPipeFactoryOptions options;
    options.ExecPath = executable.GetPath();
    options.FileCache = fileCache;
    options.Env["PID_DIRECTORY"] = pidDirectory.GetPath();
    options.MaxProcesses = failColdStart || blockCleanup ? processCount : 0;
    factory = CreatePipeFactory(options);
    if (failColdStart || blockCleanup) {
        WaitUntil(readPids, "EOF executors did not publish their PIDs");
        fileCache->FailGetDir.store(failColdStart);
        holdRefill.store(blockCleanup);
    }

    Yql::DqsProto::TTaskMeta meta;
    if (failColdStart) {
        auto* setting = meta.AddSettings();
        setting->SetName("_PortoMemoryLimit");
        setting->SetValue("1024");
    }
    auto* file = meta.AddFiles();
    file->SetObjectType(Yql::DqsProto::TFile::EUSER_FILE);
    file->SetObjectId("missing");
    NDqProto::TDqTask task;
    task.MutableMeta()->PackFrom(meta);
    auto prepareTask = [&] {
        if (useNewApi) {
            factory->Get(/*alloc*/ nullptr, NDq::TDqTaskSettings(&task), NDqProto::DQ_STATS_MODE_NONE);
        } else {
            factory->GetOld(/*alloc*/ nullptr, NDq::TDqTaskSettings(&task));
        }
    };
    UNIT_ASSERT_EXCEPTION_CONTAINS(prepareTask(), std::runtime_error,
        failColdStart ? "Cannot get executor directory" : "Cannot find object `missing` in cache");

    if (blockCleanup) {
        siginfo_t info{};
        const auto deadline = TInstant::Now() + TDuration::Seconds(3);
        while (true) {
            info = {};
            const int result = waitid(P_PID, pids.front(), &info, WEXITED | WNOHANG | WNOWAIT);
            UNIT_ASSERT_VALUES_EQUAL(result, 0);
            if (info.si_pid == pids.front()) {
                break;
            }
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Executor did not exit on EOF while cleanup was queued");
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_VALUES_EQUAL(info.si_code, static_cast<int>(CLD_EXITED));
        UNIT_ASSERT_VALUES_EQUAL(info.si_status, 0);
        holdRefill.store(false);
    }

    for (int pid : pids) {
        siginfo_t info{};
        const auto deadline = TInstant::Now() + TDuration::Seconds(5);
        while (true) {
            info = {};
            // Observe exit without reaping the child on behalf of the factory.
            const int result = waitid(P_PID, pid, &info, WEXITED | WNOHANG | WNOWAIT);
            if (result == -1 && errno == ECHILD) {
                break;
            }
            UNIT_ASSERT_C(result == 0 || errno == EINTR, "Cannot inspect executor " << pid);
            UNIT_ASSERT_C(TInstant::Now() < deadline,
                "Executor was not reaped (Pid: " << pid << ", ExitPid: " << info.si_pid
                << ", ExitCode: " << info.si_code << ", ExitStatus: " << info.si_status << ")");
            Sleep(TDuration::MilliSeconds(10));
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TPipeMetricsTest) {

    Y_UNIT_TEST(FailedPreparationReapsEofExecutor)
    {
        CheckEofExecutorReaped(/*failColdStart*/ false, /*useNewApi*/ false);
    }

    Y_UNIT_TEST(NewRunnerApiFailureReapsEofExecutor)
    {
        CheckEofExecutorReaped(/*failColdStart*/ false, /*useNewApi*/ true);
    }

    Y_UNIT_TEST(FailedColdStartReapsEvictedEofExecutors)
    {
        CheckEofExecutorReaped(/*failColdStart*/ true, /*useNewApi*/ false);
    }

    Y_UNIT_TEST(FailedPreparationClosesPipesBeforeQueuedCleanup)
    {
        CheckEofExecutorReaped(/*failColdStart*/ false, /*useNewApi*/ false, /*blockCleanup*/ true);
    }

    Y_UNIT_TEST(MetricBudgetIsSixteenScalars)
    {
        struct TCounterVisitor: public NMonitoring::ICountableConsumer {
            int Scalars = 0;
            int Histograms = 0;

            void OnCounter(const TString&, const TString&, const NMonitoring::TCounterForPtr*) override
            {
                ++Scalars;
            }

            void OnHistogram(const TString&, const TString&, NMonitoring::IHistogramSnapshotPtr, bool) override
            {
                ++Histograms;
            }

            void OnGroupBegin(const TString&, const TString&, const NMonitoring::TDynamicCounters*) override { }
            void OnGroupEnd(const TString&, const TString&, const NMonitoring::TDynamicCounters*) override { }
        } visitor;

        TPipeMetricsFixture fixture(/*poolSize*/ 0);
        fixture.Counters->Accept({}, {}, visitor);
        UNIT_ASSERT_VALUES_EQUAL(visitor.Histograms, 0);
        UNIT_ASSERT_VALUES_EQUAL(visitor.Scalars, 16);
        for (const auto* name : {"PoolStopDelayCount", "PoolStopDelayTotalUs", "PortoCleanupCount", "PortoCleanupTotalUs"}) {
            const auto counter = fixture.Counters->FindCounter(name);
            UNIT_ASSERT_C(counter, name);
            UNIT_ASSERT_C(counter->ForDerivative(), name);
            UNIT_ASSERT_VALUES_EQUAL(counter->Val(), 0);
        }
    }

    Y_UNIT_TEST(FailedPreparationCleansColdExecutor)
    {
        TPipeMetricsFixture fixture(/*poolSize*/ 0);
        fixture.FailTaskPreparation();
        WaitUntil([&] { return fixture.Count("PortoCleanupCount") == 1 && fixture.Count("ProcessesPendingStop") == 0; });
        fixture.AssertReaped(/*slot*/ 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolAcquireMisses"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolAcquireHits"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolEvictedOnKeyMismatch"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoOwnerReleasedWithoutDestroyAttempt"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoOwnerReleasedAfterDestroyFailure"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolStopDelayCount"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoContainersDestroyed"), 1);
    }

    Y_UNIT_TEST(NewRunnerApiFailureCleansColdExecutor)
    {
        TPipeMetricsFixture fixture(/*poolSize*/ 0);
        fixture.FailTaskPreparation(/*changeKey*/ false, /*useNewApi*/ true);
        WaitUntil([&] { return fixture.Count("PortoCleanupCount") == 1 && fixture.Count("ProcessesPendingStop") == 0; });
        fixture.AssertReaped(/*slot*/ 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoOwnerReleasedWithoutDestroyAttempt"), 0);
    }

    Y_UNIT_TEST(UnusedPoolOwnerReleasedWithoutDestroyAttempt)
    {
        TPipeMetricsFixture fixture(/*poolSize*/ 2);
        fixture.ResetFactory();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoOwnerReleasedWithoutDestroyAttempt"), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoOwnerReleasedAfterDestroyFailure"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoCleanupCount"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolStopDelayCount"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolStopDelayTotalUs"), 0);
    }

    Y_UNIT_TEST(FailedPreparationCleansWarmExecutor)
    {
        TPipeMetricsFixture fixture(/*poolSize*/ 2);
        fixture.FailTaskPreparation();
        WaitUntil([&] { return fixture.Count("PortoCleanupCount") == 1 && fixture.Count("ProcessesPendingStop") == 0; });
        fixture.AssertReaped(/*slot*/ 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolAcquireHits"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolAcquireMisses"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolEvictedOnKeyMismatch"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoOwnerReleasedWithoutDestroyAttempt"), 0);
    }

    Y_UNIT_TEST(FailedColdStartCleansEvictedExecutors)
    {
        TPipeMetricsFixture fixture(/*poolSize*/ 2);
        fixture.FailNextStart();
        fixture.FailTaskPreparation(/*changeKey*/ true);
        WaitUntil([&] { return fixture.Count("PortoCleanupCount") == 2 && fixture.Count("ProcessesPendingStop") == 0; });
        fixture.AssertReaped(/*slot*/ 1);
        fixture.AssertReaped(/*slot*/ 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoContainersStarted"), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolEvictedOnKeyMismatch"), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoOwnerReleasedWithoutDestroyAttempt"), 0);
    }

    Y_UNIT_TEST(FailedPreparationReturnsBeforeCleanupFinishes)
    {
        TPipeMetricsFixture fixture(/*poolSize*/ 0, /*destroyExitCode*/ 0, /*blockDestroy*/ true);
        auto failedTask = std::async(std::launch::async, [&] { fixture.PrepareTaskWithMissingFile(); });
        const bool returned = failedTask.wait_for(std::chrono::seconds(10)) == std::future_status::ready;
        if (!returned) {
            fixture.ReleaseDestroy();
        }
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            failedTask.get(),
            std::runtime_error,
            "Cannot find object `missing` in cache");
        UNIT_ASSERT_C(returned, "Task preparation waited for cleanup");
        fixture.WaitForDestroy();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("ProcessesPendingStop"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoCleanupCallsInFlight"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoContainersAlive"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoCleanupCount"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoOwnerReleasedWithoutDestroyAttempt"), 0);
        fixture.ReleaseDestroy();
        WaitUntil([&] { return fixture.Count("ProcessesPendingStop") == 0; });
        fixture.AssertReaped(/*slot*/ 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoContainersDestroyed"), 1);
    }

    Y_UNIT_TEST(BlockedCleanupIsVisibleBeforeCompletion)
    {
        TPipeMetricsFixture fixture(/*poolSize*/ 2, /*destroyExitCode*/ 0, /*blockDestroy*/ true);
        fixture.FailTaskPreparation(/*changeKey*/ true);
        fixture.WaitForDestroy();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolEvictedOnKeyMismatch"), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("ProcessesPendingStop"), 3);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolStopDelayCount"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoCleanupCount"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoCleanupTotalUs"), 0);
        fixture.ReleaseDestroy();
        WaitUntil([&] { return fixture.Count("ProcessesPendingStop") == 0; });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoCleanupCallsInFlight"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoContainersDestroyed"), 3);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoOwnerReleasedAfterDestroyFailure"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolStopDelayCount"), 3);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoCleanupCount"), 3);
        UNIT_ASSERT(fixture.Count("PoolStopDelayTotalUs") > 0);
        UNIT_ASSERT(fixture.Count("PortoCleanupTotalUs") > 0);
    }

    Y_UNIT_TEST(FailedDestroyIsCountedAtOwnerRelease)
    {
        for (const int destroyExitCode : {1, 4}) {
            TPipeMetricsFixture fixture(/*poolSize*/ 2, /*destroyExitCode*/ destroyExitCode);
            fixture.FailTaskPreparation(/*changeKey*/ true);
            WaitUntil([&] { return fixture.Count("PortoOwnerReleasedAfterDestroyFailure") == 3; });
            UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoContainerDestroyErrors"), 3);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoContainersDestroyed"), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoCleanupCallsInFlight"), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoCleanupCount"), 3);
            UNIT_ASSERT(fixture.Count("PortoCleanupTotalUs") > 0);
        }
    }
}

} // namespace NYql::NTaskRunnerProxy
