#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_pipe.h>

#include <ydb/library/yql/providers/dq/api/protos/service.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>

#include <util/datetime/base.h>

#include <util/folder/tempdir.h>

#include <util/generic/scope.h>
#include <util/generic/yexception.h>

#include <util/stream/file.h>

#include <util/string/cast.h>
#include <util/string/strip.h>

#include <util/system/sysstat.h>

#include <cerrno>
#include <csignal>
#include <stdexcept>

#include <sys/wait.h>
#include <unistd.h>

namespace NYql::NTaskRunnerProxy {
namespace {

int WaitForExecutorPid(const TFsPath& pidFile)
{
    const auto deadline = TInstant::Now() + TDuration::Seconds(5);
    while (TInstant::Now() < deadline) {
        int pid = 0;
        if (pidFile.Exists()) {
            const auto pidFileContent = TFileInput(pidFile.GetPath()).ReadAll();
            if (TryFromString<int>(Strip(pidFileContent), pid) && pid > 0) {
                return pid;
            }
        }
        Sleep(TDuration::MilliSeconds(10));
    }
    return 0;
}

class TStartedExecutorFileCache
    : public IFileCache
{
public:
    TStartedExecutorFileCache(TString directory, TFsPath pidFile)
        : Directory_(std::move(directory))
        , PidFile_(std::move(pidFile))
    { }

    void AddFile(const TString& /*path*/, const TString& /*objectId*/) override
    { }

    TMaybe<TString> FindFile(const TString& /*objectId*/) override
    {
        return Nothing();
    }

    TMaybe<TString> AcquireFile(const TString& /*objectId*/) override
    {
        if (WaitForExecutorPid(PidFile_) <= 0) {
            ythrow yexception() << "Executor did not publish its PID";
        }
        return Directory_ + "/executor";
    }

    void ReleaseFile(const TString& /*objectId*/) override
    { }

    bool Contains(const TString& /*objectId*/) override
    {
        return false;
    }

    void Walk(const std::function<void(const TString&)>& /*callback*/) override
    { }

    i64 FreeDiskSize() override
    {
        return 0;
    }

    ui64 UsedDiskSize() override
    {
        return 0;
    }

    TString GetDir() override
    {
        return Directory_;
    }

private:
    const TString Directory_;
    const TFsPath PidFile_;
};

TString SerializeProgram(bool includeInputs)
{
    using namespace NKikimr::NMiniKQL;

    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    TStructLiteralBuilder builder(env);
    builder.Add("Program", TRuntimeNode(env.GetVoidLazy(), /*isImmediate*/ true));
    if (includeInputs) {
        builder.Add("Inputs", TRuntimeNode(env.GetEmptyTupleLazy(), /*isImmediate*/ true));
    }
    return SerializeRuntimeNode(
        TRuntimeNode(builder.Build(), /*isImmediate*/ true),
        env.GetNodeStack());
}

template <class TException>
void CheckConstructorCleanup(
    NDqProto::TDqTask task,
    TStringBuf errorSubstring,
    bool expectFailure = true)
{
    TTempDir directory;
    const auto executable = directory.Path() / "executor";
    const auto pidFile = directory.Path() / "pid";
    {
        TFileOutput script(executable.GetPath());
        script << "#!/bin/sh\nprintf '%s\\n' \"$$\" > \"$PID_FILE\"\nexec /bin/sleep 10\n";
    }
    UNIT_ASSERT_VALUES_EQUAL(Chmod(executable.c_str(), MODE0755), 0);

    Yql::DqsProto::TTaskMeta taskMeta;
    auto* file = taskMeta.AddFiles();
    file->SetObjectType(Yql::DqsProto::TFile::EUSER_FILE);
    file->SetObjectId("dummy");
    file->SetName("dummy");
    task.MutableMeta()->PackFrom(taskMeta);

    TPipeFactoryOptions options;
    options.ExecPath = executable.GetPath();
    options.FileCache = MakeIntrusive<TStartedExecutorFileCache>(directory.Name(), pidFile);
    options.Env["PID_FILE"] = pidFile.GetPath();
    options.MaxProcesses = 0;
    auto factory = CreatePipeFactory(options);
    auto alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__);

    bool reapedByRunner = false;
    bool reaped = false;
    auto cleanup = [&] () {
        if (reaped) {
            return;
        }
        try {
            const int pid = WaitForExecutorPid(pidFile);
            if (pid <= 0) {
                return;
            }

            int status = 0;
            int result;
            do {
                result = waitpid(pid, &status, WNOHANG);
            } while (result == -1 && errno == EINTR);
            reapedByRunner = result == -1 && errno == ECHILD;
            if (result == 0) {
                if (kill(pid, SIGKILL) == -1 && errno != ESRCH) {
                    return;
                }
                do {
                    result = waitpid(pid, &status, 0);
                } while (result == -1 && errno == EINTR);
            }
            reaped = result == pid || reapedByRunner;
        } catch (...) { }
    };
    Y_DEFER {
        cleanup();
    };

    if (expectFailure) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            factory->GetOld(alloc, NDq::TDqTaskSettings(&task)),
            TException,
            errorSubstring);
    } else {
        UNIT_ASSERT_NO_EXCEPTION(factory->GetOld(alloc, NDq::TDqTaskSettings(&task)));
    }
    cleanup();
    factory = nullptr;
    UNIT_ASSERT_C(reaped, "Executor PID was not published or its process could not be reaped");
    UNIT_ASSERT_C(reapedByRunner, "Runner did not reap the executor before releasing ownership");
}

} // namespace

Y_UNIT_TEST_SUITE(TPipeTaskRunnerConstructorTest) {
    Y_UNIT_TEST(TaskMetaFailureKillsAndReapsExecutor)
    {
        NDqProto::TDqTask task;
        task.SetId(1);
        task.MutableProgram()->SetRaw(SerializeProgram(/*includeInputs*/ false));
        CheckConstructorCleanup<yexception>(task, "programInputsIdx");
    }

    Y_UNIT_TEST(ChannelFailureKillsAndReapsExecutor)
    {
        NDqProto::TDqTask task;
        task.SetId(1);
        task.MutableProgram()->SetRaw(SerializeProgram(/*includeInputs*/ true));
        task.AddInputs()->MutableSource();
        CheckConstructorCleanup<std::out_of_range>(task, "");
    }

    Y_UNIT_TEST(SuccessfulRunnerDestructorReapsExecutor)
    {
        NDqProto::TDqTask task;
        task.SetId(1);
        task.MutableProgram()->SetRaw(SerializeProgram(/*includeInputs*/ true));
        CheckConstructorCleanup<yexception>(task, "", /*expectFailure*/ false);
    }
}

} // namespace NYql::NTaskRunnerProxy
