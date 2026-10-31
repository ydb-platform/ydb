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
#include <csignal>
#include <future>
#include <stdexcept>

#include <sys/wait.h>

namespace NYql::NTaskRunnerProxy {
namespace {

void WaitUntil(const std::function<bool()>& predicate)
{
    const auto deadline = TInstant::Now() + TDuration::Seconds(10);
    while (!predicate()) {
        if (TInstant::Now() >= deadline) {
            throw std::runtime_error("Timed out waiting for runner cleanup");
        }
        Sleep(TDuration::MilliSeconds(10));
    }
}

class TBlockingFileCache: public IFileCache {
public:
    const TString Directory;
    std::atomic<bool> Acquiring = false;
    std::atomic<bool> Release = false;

    explicit TBlockingFileCache(TString directory)
        : Directory(std::move(directory))
    {}

    void AddFile(const TString&, const TString&) override {}
    TMaybe<TString> FindFile(const TString&) override { return Nothing(); }
    TMaybe<TString> AcquireFile(const TString&) override {
        Acquiring = true;
        WaitUntil([&] { return Release.load(); });
        return Nothing();
    }
    void ReleaseFile(const TString&) override {}
    bool Contains(const TString&) override { return false; }
    void Walk(const std::function<void(const TString&)>&) override {}
    i64 FreeDiskSize() override { return 0; }
    ui64 UsedDiskSize() override { return 0; }
    TString GetDir() override { return Directory; }
};

class TFixture {
public:
    TTempDir Directory;
    const NMonitoring::TDynamicCounterPtr Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    const TIntrusivePtr<TBlockingFileCache> Cache = MakeIntrusive<TBlockingFileCache>(Directory.Name());
    IProxyFactory::TPtr Factory;

    TFixture()
    {
        (Directory.Path() / "pids").MkDirs();
        const auto porto = Directory.Path() / "porto";
        {
            TFileOutput script(porto);
            script << "#!/bin/sh\n"
                << "case \"$1\" in\n"
                << "exec) printf '%s\\n' \"$$\" > '" << Directory.Name()
                << "/pids/'\"${4##*/}\"; exec /bin/sleep 60;;\n"
                << "destroy)\n"
                << "  : > '" << Directory.Name() << "/destroy-'\"${2##*/}\"\n"
                << "  if [ \"${2##*/}\" = Slot-1 ]; then\n"
                << "    n=0\n"
                << "    while [ ! -e '" << Directory.Name() << "/release-destroy' ] && [ \"$n\" -lt 200 ]; do\n"
                << "      /bin/sleep 0.05; n=$((n+1))\n"
                << "    done\n"
                << "  fi;;\n"
                << "*) exit 1;;\nesac\n";
        }
        UNIT_ASSERT_VALUES_EQUAL(Chmod(porto.c_str(), MODE0755), 0);
        TPipeFactoryOptions options;
        options.ExecPath = "/bin/sleep";
        options.FileCache = Cache;
        options.EnablePorto = true;
        options.EnablePortoAnonLimitRaiseBeforeDestroy = false;
        options.PortoCtlPath = porto.GetPath();
        options.MaxProcesses = 1;
        options.Counters = Counters;
        Factory = CreatePipeFactory(options);
    }

    ~TFixture()
    {
        ReleaseDestroy();
        Cache->Release = true;
        Factory = nullptr;
        // Fake Porto has no real container: reap warm executors after shutdown.
        const auto started = Started();
        for (i64 slot = 1; slot <= started; ++slot) {
            const auto pidFile = Directory.Path() / "pids" / ("Slot-" + ToString(slot));
            int pid = 0;
            if (!pidFile.Exists() || !TryFromString(Strip(TFileInput(pidFile).ReadAll()), pid) || pid <= 0) {
                continue;
            }
            int status = 0;
            int result;
            do {
                result = waitpid(pid, &status, WNOHANG);
            } while (result == -1 && errno == EINTR);
            if (result == 0) {
                kill(pid, SIGKILL);
                while (waitpid(pid, &status, 0) == -1 && errno == EINTR) {}
            }
        }
    }

    i64 Started() const
    {
        return Counters->GetCounter("PortoContainersStarted", true)->Val();
    }

    int WaitForPid(int slot)
    {
        int pid = 0;
        const auto file = Directory.Path() / "pids" / ("Slot-" + ToString(slot));
        WaitUntil([&] { return file.Exists() && TryFromString(Strip(TFileInput(file).ReadAll()), pid) && pid > 0; });
        return pid;
    }

    void ReleaseDestroy()
    {
        TFileOutput(Directory.Path() / "release-destroy").Finish();
    }

    void RequestDifferentKey()
    {
        Yql::DqsProto::TTaskMeta meta;
        auto* setting = meta.AddSettings();
        setting->SetName("_EnablePorto");
        setting->SetValue("true");
        setting = meta.AddSettings();
        setting->SetName("_PortoMemoryLimit");
        setting->SetValue("1024");
        auto* file = meta.AddFiles();
        file->SetObjectType(Yql::DqsProto::TFile::EUSER_FILE);
        file->SetObjectId("missing");
        NDqProto::TDqTask task;
        task.MutableMeta()->PackFrom(meta);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            Factory->GetOld(nullptr, NDq::TDqTaskSettings(&task)),
            std::runtime_error,
            "Cannot find object `missing` in cache");
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TPipeFactoryCleanup) {
    Y_UNIT_TEST(StopsIncompatibleReserveBeforeRefill) {
        TFixture fixture;
        fixture.WaitForPid(1);
        auto request = std::async(std::launch::async, [&] { fixture.RequestDifferentKey(); });
        Y_DEFER {
            fixture.ReleaseDestroy();
            fixture.Cache->Release = true;
        };
        WaitUntil([&] { return fixture.Cache->Acquiring.load(); });
        WaitUntil([&] { return (fixture.Directory.Path() / "destroy-Slot-1").Exists(); });

        // A stale idle executor and an acquired executor exist. Refilling
        // before this blocked destroy would already have launched a third.
        UNIT_ASSERT_VALUES_EQUAL(fixture.Started(), 2);
        const int acquiredPid = fixture.WaitForPid(2);
        UNIT_ASSERT_VALUES_EQUAL(kill(acquiredPid, 0), 0);
        UNIT_ASSERT(!(fixture.Directory.Path() / "destroy-Slot-2").Exists());

        fixture.ReleaseDestroy();
        WaitUntil([&] { return fixture.Started() == 3; });
        fixture.WaitForPid(3);
        UNIT_ASSERT_VALUES_EQUAL(kill(acquiredPid, 0), 0);
        UNIT_ASSERT(!(fixture.Directory.Path() / "destroy-Slot-2").Exists());
        fixture.Cache->Release = true;
        request.get();
    }
}

} // namespace NYql::NTaskRunnerProxy
