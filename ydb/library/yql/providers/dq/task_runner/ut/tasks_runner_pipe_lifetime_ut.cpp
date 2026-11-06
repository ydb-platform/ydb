#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_pipe.h>

#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>

#include <yql/essentials/utils/failure_injector/failure_injector.h>

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
#include <stdexcept>

#include <sys/wait.h>

namespace NYql::NTaskRunnerProxy {
namespace {

void WaitUntil(const std::function<bool()>& predicate)
{
    const auto deadline = TInstant::Now() + TDuration::Seconds(10);
    while (!predicate()) {
        if (TInstant::Now() >= deadline) {
            throw std::runtime_error("Timed out waiting for pipe factory");
        }
        Sleep(TDuration::MilliSeconds(10));
    }
}

class TBlockingFileCache: public IFileCache {
public:
    std::atomic<bool> Block = false;
    std::atomic<bool> Entered = false;
    std::atomic<bool> Release = false;

    explicit TBlockingFileCache(TString directory)
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

    TString GetDir() override
    {
        if (Block.load()) {
            Entered.store(true);
            WaitUntil([&] { return Release.load(); });
        }
        return Directory_;
    }

private:
    const TString Directory_;
};

NDqProto::TDqTask MakeTask(bool enablePorto, bool missingFile)
{
    Yql::DqsProto::TTaskMeta meta;
    auto* setting = meta.AddSettings();
    setting->SetName("_EnablePorto");
    setting->SetValue(enablePorto ? "true" : "false");
    if (missingFile) {
        auto* file = meta.AddFiles();
        file->SetObjectType(Yql::DqsProto::TFile::EUSER_FILE);
        file->SetObjectId("missing");
    }
    NDqProto::TDqTask task;
    task.MutableMeta()->PackFrom(meta);
    return task;
}

TString SerializeProgramWithoutInputs()
{
    using namespace NKikimr::NMiniKQL;

    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    TStructLiteralBuilder builder(env);
    builder.Add("Program", TRuntimeNode(env.GetVoidLazy(), /*isImmediate*/ true));
    return SerializeRuntimeNode(
        TRuntimeNode(builder.Build(), /*isImmediate*/ true),
        env.GetNodeStack());
}

int ReadPid(const TFsPath& path)
{
    int pid = 0;
    if (path.Exists()) {
        TryFromString(Strip(TFileInput(path).ReadAll()), pid);
    }
    return pid;
}

void CleanupChildren(const TFsPath& directory, i64 expected)
{
    try {
        WaitUntil([&] {
            for (i64 slot = 1; slot <= expected; ++slot) {
                if (ReadPid(directory / ("Slot-" + ToString(slot))) <= 0) {
                    return false;
                }
            }
            return true;
        });
    } catch (...) { }
    TVector<TFsPath> files;
    directory.List(files);
    for (const auto& file : files) {
        const int pid = ReadPid(file);
        if (pid <= 0) {
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
}

#ifdef _linux_
int CountOpenDescriptors()
{
    TVector<TFsPath> files;
    TFsPath("/proc/self/fd").List(files);
    return static_cast<int>(files.size());
}
#endif

} // namespace

Y_UNIT_TEST_SUITE(TPipeFactoryLifetimeTest) {
#ifdef _linux_
    Y_UNIT_TEST(FailedLaunchClosesDescriptorsBeforeFork)
    {
        TTempDir directory;
        const auto pids = directory.Path() / "pids";
        pids.MkDirs();
        const auto executable = directory.Path() / "executor";
        {
            TFileOutput script(executable);
            script << "#!/bin/sh\n"
                << "printf '%s\\n' \"$$\" > \"$PID_DIRECTORY/${PWD##*/}\"\n"
                << "exec /bin/sleep 60\n";
        }
        UNIT_ASSERT_VALUES_EQUAL(Chmod(executable.c_str(), MODE0755), 0);
        TPipeFactoryOptions options;
        options.ExecPath = executable.GetPath();
        options.FileCache = MakeIntrusive<TBlockingFileCache>(directory.Name());
        options.Env["PID_DIRECTORY"] = pids.GetPath();
        options.MaxProcesses = 0;
        auto factory = CreatePipeFactory(options);
        Y_DEFER {
            factory = nullptr;
            CleanupChildren(pids, /*expected*/ 0);
        };

        auto task = MakeTask(/*enablePorto*/ false, /*missingFile*/ true);
        TFailureInjector::Activate();
        Y_DEFER {
            TFailureInjector::Reach("DqPipeBeforeFork", [] { });
        };
        const int descriptors = CountOpenDescriptors();
        for (int attempt = 0; attempt < 8; ++attempt) {
            TFailureInjector::Set("DqPipeBeforeFork", /*skip*/ 0, /*countOfFails*/ 1);
            auto launch = [&] {
                if (attempt % 2 == 0) {
                    factory->GetOld(/*alloc*/ nullptr, NDq::TDqTaskSettings(&task));
                } else {
                    factory->Get(/*alloc*/ nullptr, NDq::TDqTaskSettings(&task), NDqProto::DQ_STATS_MODE_NONE);
                }
            };
            UNIT_ASSERT_EXCEPTION_CONTAINS(launch(), yexception, "Injected task runner launch failure");
            UNIT_ASSERT_VALUES_EQUAL(CountOpenDescriptors(), descriptors);
        }
        TVector<TFsPath> files;
        pids.List(files);
        UNIT_ASSERT(files.empty());
        siginfo_t info{};
        UNIT_ASSERT_VALUES_EQUAL(waitid(P_ALL, 0, &info, WEXITED | WNOHANG | WNOWAIT), -1);
        UNIT_ASSERT_VALUES_EQUAL(errno, ECHILD);
    }
#endif

    Y_UNIT_TEST(FactoryDestructionWaitsForRefill)
    {
        TTempDir directory;
        const auto pids = directory.Path() / "pids";
        pids.MkDirs();
        {
            const auto porto = directory.Path() / "portoctl";
            TFileOutput script(porto);
            script << "#!/bin/sh\ncase \"$1\" in\n"
                << "exec) printf '%s\\n' \"$$\" > '" << pids.GetPath()
                << "/'\"${4##*/}\"; exec /bin/sleep 60;;\n"
                << "destroy) exit 0;;\n*) exit 1;;\nesac\n";
            script.Finish();
            UNIT_ASSERT_VALUES_EQUAL(Chmod(porto.c_str(), MODE0755), 0);
        }
        const auto cache = MakeIntrusive<TBlockingFileCache>(directory.Name());
        const auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TPipeFactoryOptions options;
        options.ExecPath = "/bin/sleep";
        options.FileCache = cache;
        options.EnablePorto = true;
        options.EnablePortoAnonLimitRaiseBeforeDestroy = false;
        // Make the factory's path allocation unshared after the acquired owner is released.
        options.PortoCtlPath = directory.Name() + "/./portoctl";
        options.MaxProcesses = 1;
        options.Counters = counters;
        auto factory = CreatePipeFactory(options);
        options.PortoCtlPath.clear();
        Y_DEFER {
            cache->Release.store(true);
            factory = nullptr;
            CleanupChildren(pids, counters->GetCounter("PortoContainersStarted", /*derivative*/ true)->Val());
        };
        WaitUntil([&] { return ReadPid(pids / "Slot-1") > 0; });

        cache->Block.store(true);
        auto task = MakeTask(/*enablePorto*/ true, /*missingFile*/ false);
        task.MutableProgram()->SetRaw(SerializeProgramWithoutInputs());
        auto alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            factory->GetOld(alloc, NDq::TDqTaskSettings(&task)),
            yexception,
            "programInputsIdx");
        WaitUntil([&] { return cache->Entered.load(); });
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("PortoContainersAlive")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("ProcessesPendingStop")->Val(), 0);

        std::atomic<bool> destroying = false;
        std::future<void> destroyed;
        Y_DEFER {
            cache->Release.store(true);
        };
        destroyed = std::async(std::launch::async, [&] {
            destroying.store(true);
            factory = nullptr;
        });
        WaitUntil([&] { return destroying.load(); });
        UNIT_ASSERT_C(
            destroyed.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout,
            "Factory destruction did not wait for the blocked refill");
        cache->Release.store(true);
        destroyed.get();
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("PortoContainersStarted", /*derivative*/ true)->Val(), 2);
        WaitUntil([&] { return ReadPid(pids / "Slot-2") > 0; });
    }
}

} // namespace NYql::NTaskRunnerProxy
