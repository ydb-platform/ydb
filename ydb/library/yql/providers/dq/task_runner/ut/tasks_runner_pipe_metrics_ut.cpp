#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_pipe.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/strip.h>
#include <util/system/fs.h>

#include <cerrno>
#include <csignal>
#include <iterator>

#include <sys/wait.h>

namespace NYql::NTaskRunnerProxy {
namespace {

void WaitUntil(const std::function<bool()>& ready)
{
    const auto deadline = TInstant::Now() + TDuration::Seconds(10);
    while (!ready()) {
        UNIT_ASSERT_C(TInstant::Now() < deadline, "Timed out waiting for runner metrics");
        Sleep(TDuration::MilliSeconds(10));
    }
}

class TEmptyFileCache: public IFileCache {
public:
    explicit TEmptyFileCache(TString directory)
        : Directory_(std::move(directory))
    { }

    void AddFile(const TString&, const TString&) override { }
    TMaybe<TString> FindFile(const TString&) override { return Nothing(); }
    TMaybe<TString> AcquireFile(const TString&) override { return Nothing(); }
    void ReleaseFile(const TString&) override { }
    bool Contains(const TString&) override { return false; }
    void Walk(const std::function<void(const TString&)>&) override { }
    i64 FreeDiskSize() override { return 0; }
    ui64 UsedDiskSize() override { return 0; }
    TString GetDir() override { return Directory_; }

private:
    const TString Directory_;
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
        TPipeFactoryOptions options;
        options.ExecPath = "/bin/sleep";
        options.FileCache = MakeIntrusive<TEmptyFileCache>(Directory_.Name());
        options.EnablePorto = true;
        options.EnablePortoAnonLimitRaiseBeforeDestroy = false;
        options.PortoCtlPath = porto.GetPath();
        options.MaxProcesses = poolSize;
        options.Counters = Counters;
        Factory_ = CreatePipeFactory(options);
    }

    ~TPipeMetricsFixture()
    {
        ReleaseDestroy();
        Factory_ = nullptr;
        // A failed task preparation releases its executor without killing it.
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

    void FailTaskPreparation(bool changeKey = false)
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
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            Factory_->GetOld(nullptr, NDq::TDqTaskSettings(&task)),
            std::runtime_error,
            "Cannot find object");
    }

    i64 Count(const TString& name) const
    {
        const auto counter = Counters->FindCounter(name);
        UNIT_ASSERT_C(counter, name);
        return counter->Val();
    }

private:
    TTempDir Directory_;
    IProxyFactory::TPtr Factory_;
};

} // namespace

Y_UNIT_TEST_SUITE(TPipeMetricsTest) {
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

    Y_UNIT_TEST(MissAndOwnerReleasedWithoutDestroyAttempt)
    {
        TPipeMetricsFixture fixture(/*poolSize*/ 0);
        fixture.FailTaskPreparation();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolAcquireMisses"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolAcquireHits"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolEvictedOnKeyMismatch"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoOwnerReleasedWithoutDestroyAttempt"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoOwnerReleasedAfterDestroyFailure"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolStopDelayCount"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolStopDelayTotalUs"), 0);
    }

    Y_UNIT_TEST(HitDoesNotEvictMatchingProcesses)
    {
        TPipeMetricsFixture fixture(/*poolSize*/ 2);
        fixture.FailTaskPreparation();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolAcquireHits"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolAcquireMisses"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolEvictedOnKeyMismatch"), 0);
    }

    Y_UNIT_TEST(BlockedCleanupIsVisibleBeforeCompletion)
    {
        TPipeMetricsFixture fixture(/*poolSize*/ 2, /*destroyExitCode*/ 0, /*blockDestroy*/ true);
        fixture.FailTaskPreparation(/*changeKey*/ true);
        WaitUntil([&] { return fixture.Count("PortoCleanupCallsInFlight") == 1; });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolEvictedOnKeyMismatch"), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("ProcessesPendingStop"), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolStopDelayCount"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoCleanupCount"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoCleanupTotalUs"), 0);
        Sleep(TDuration::MilliSeconds(100));
        fixture.ReleaseDestroy();
        WaitUntil([&] { return fixture.Count("ProcessesPendingStop") == 0; });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoCleanupCallsInFlight"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoContainersDestroyed"), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoOwnerReleasedAfterDestroyFailure"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PoolStopDelayCount"), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoCleanupCount"), 2);
        UNIT_ASSERT(fixture.Count("PoolStopDelayTotalUs") >= 100'000);
        UNIT_ASSERT(fixture.Count("PortoCleanupTotalUs") >= 100'000);
    }

    Y_UNIT_TEST(FailedDestroyIsCountedAtOwnerRelease)
    {
        for (const int destroyExitCode : {1, 4}) {
            TPipeMetricsFixture fixture(/*poolSize*/ 2, /*destroyExitCode*/ destroyExitCode);
            fixture.FailTaskPreparation(/*changeKey*/ true);
            WaitUntil([&] { return fixture.Count("PortoOwnerReleasedAfterDestroyFailure") == 2; });
            UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoContainerDestroyErrors"), 2);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoContainersDestroyed"), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoCleanupCallsInFlight"), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Count("PortoCleanupCount"), 2);
            UNIT_ASSERT(fixture.Count("PortoCleanupTotalUs") > 0);
        }
    }
}

} // namespace NYql::NTaskRunnerProxy
