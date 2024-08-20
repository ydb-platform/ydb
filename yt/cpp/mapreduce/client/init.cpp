#include "init.h"

#include "abortable_registry.h"
#include "job_profiler.h"

#include <yt/cpp/mapreduce/http/requests.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/init.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/cpp/mapreduce/io/job_reader.h>

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>

#include <library/cpp/sighandler/async_signals_handler.h>

#include <util/folder/dirut.h>

#include <util/generic/singleton.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/type.h>

#include <util/system/env.h>
#include <util/system/thread.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

void WriteVersionToLog()
{
    YT_LOG_INFO("Wrapper version: %v",
        TProcessState::Get()->ClientVersion);
}

static TNode SecureVaultContents; // safe

void InitializeSecureVault()
{
    SecureVaultContents = NodeFromYsonString(
        GetEnv("YT_SECURE_VAULT", "{}"));
}

}

////////////////////////////////////////////////////////////////////////////////

const TNode& GetJobSecureVault()
{
    return SecureVaultContents;
}

////////////////////////////////////////////////////////////////////////////////

class TAbnormalTerminator
{
public:
    TAbnormalTerminator() = default;

    static void SetErrorTerminationHandler()
    {
        if (Instance().OldHandler_ != nullptr) {
            return;
        }

        Instance().OldHandler_ = std::set_terminate(&TerminateHandler);

        SetAsyncSignalFunction(SIGINT, SignalHandler);
        SetAsyncSignalFunction(SIGTERM, SignalHandler);
    }

private:
    static TAbnormalTerminator& Instance()
    {
        return *Singleton<TAbnormalTerminator>();
    }

    static void* Invoke(void* opaque)
    {
        (*reinterpret_cast<std::function<void()>*>(opaque))();
        return nullptr;
    }

    static void TerminateWithTimeout(
        const TDuration& timeout,
        const std::function<void(void)>& exitFunction,
        const TString& logMessage)
    {
        std::function<void()> threadFun = [=] {
            YT_LOG_INFO("%v",
                logMessage);
            NDetail::TAbortableRegistry::Get()->AbortAllAndBlockForever();
        };
        TThread thread(TThread::TParams(Invoke, &threadFun).SetName("aborter"));
        thread.Start();
        thread.Detach();

        Sleep(timeout);
        exitFunction();
    }

    static void SignalHandler(int signalNumber)
    {
        TerminateWithTimeout(
            TDuration::Seconds(5),
            std::bind(_exit, -signalNumber),
            ::TStringBuilder() << "Signal " << signalNumber << " received, aborting transactions. Waiting 5 seconds...");
    }

    static void TerminateHandler()
    {
        TerminateWithTimeout(
            TDuration::Seconds(5),
            [&] {
                if (Instance().OldHandler_) {
                    Instance().OldHandler_();
                } else {
                    abort();
                }
            },
            ::TStringBuilder() << "Terminate called, aborting transactions. Waiting 5 seconds...");
    }

private:
    std::terminate_handler OldHandler_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

EInitStatus& GetInitStatus()
{
    static EInitStatus initStatus = EInitStatus::NotInitialized;
    return initStatus;
}

static void ElevateInitStatus(const EInitStatus newStatus) {
    NDetail::GetInitStatus() = Max(NDetail::GetInitStatus(), newStatus);
}

void CommonInitialize(int argc, const char** argv)
{
    auto logLevelStr = to_lower(TConfig::Get()->LogLevel);
    ILogger::ELevel logLevel;

    if (!TryFromString(logLevelStr, logLevel)) {
        Cerr << "Invalid log level: " << TConfig::Get()->LogLevel << Endl;
        exit(1);
    }

    auto logPath = TConfig::Get()->LogPath;
    auto logger = logPath.Empty() ? CreateStdErrLogger(logLevel) : CreateFileLogger(logLevel, logPath);
    SetLogger(logger);

    TProcessState::Get()->SetCommandLine(argc, argv);
}

void NonJobInitialize(const TInitializeOptions& options)
{
    if (FromString<bool>(GetEnv("YT_CLEANUP_ON_TERMINATION", "0")) || options.CleanupOnTermination_) {
        TAbnormalTerminator::SetErrorTerminationHandler();
    }
    if (options.WaitProxy_) {
        NDetail::TWaitProxy::Get()->SetProxy(options.WaitProxy_);
    }
    WriteVersionToLog();
}

void ExecJob(int argc, const char** argv, const TInitializeOptions& options)
{
    // Now we are definitely in job.
    // We take this setting from environment variable to be consistent with client code.
    TConfig::Get()->UseClientProtobuf = IsTrue(GetEnv("YT_USE_CLIENT_PROTOBUF", ""));

    auto execJobImpl = [&options](TString jobName, i64 outputTableCount, bool hasState) {
        auto jobProfiler = CreateJobProfiler();
        jobProfiler->Start();

        InitializeSecureVault();

        NDetail::OutputTableCount = static_cast<i64>(outputTableCount);

        THolder<IInputStream> jobStateStream;
        if (hasState) {
            jobStateStream = MakeHolder<TIFStream>("jobstate");
        } else {
            jobStateStream = MakeHolder<TBufferStream>(0);
        }

        int ret = 1;
        try {
            ret = TJobFactory::Get()->GetJobFunction(jobName.data())(outputTableCount, *jobStateStream);
        } catch (const TSystemError& ex) {
            if (ex.Status() == EPIPE) {
                // 32 == EPIPE, write number here so it's easier to grep this exit code in source files
                exit(32);
            }
            throw;
        }

        jobProfiler->Stop();

        if (options.JobOnExitFunction_) {
            (*options.JobOnExitFunction_)();
        }
        exit(ret);
    };

    auto jobArguments = NodeFromYsonString(GetEnv("YT_JOB_ARGUMENTS", "#"));
    if (jobArguments.HasValue()) {
        execJobImpl(
            jobArguments["job_name"].AsString(),
            jobArguments["output_table_count"].AsInt64(),
            jobArguments["has_state"].AsBool());
        Y_UNREACHABLE();
    }

    TString jobType = argc >= 2 ? argv[1] : TString();
    if (argc != 5 || jobType != "--yt-map" && jobType != "--yt-reduce") {
        // We are inside job but probably using old API
        // (i.e. both NYT::Initialize and NMR::Initialize are called).
        WriteVersionToLog();
        return;
    }

    TString jobName(argv[2]);
    i64 outputTableCount = FromString<i64>(argv[3]);
    int hasState = FromString<int>(argv[4]);
    execJobImpl(jobName, outputTableCount, hasState);
    Y_UNREACHABLE();
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

void JoblessInitialize(const TInitializeOptions& options)
{
    static const char* fakeArgv[] = {"unknown..."};
    NDetail::CommonInitialize(1, fakeArgv);
    NDetail::NonJobInitialize(options);
    NDetail::ElevateInitStatus(NDetail::EInitStatus::JoblessInitialization);
}

void Initialize(int argc, const char* argv[], const TInitializeOptions& options)
{
    NDetail::CommonInitialize(argc, argv);

    NDetail::ElevateInitStatus(NDetail::EInitStatus::FullInitialization);

    const bool isInsideJob = !GetEnv("YT_JOB_ID").empty();
    if (isInsideJob) {
        NDetail::ExecJob(argc, argv, options);
    } else {
        NDetail::NonJobInitialize(options);
    }
}

void Initialize(int argc, char* argv[], const TInitializeOptions& options)
{
    return Initialize(argc, const_cast<const char**>(argv), options);
}

void Initialize(const TInitializeOptions& options)
{
    static const char* fakeArgv[] = {"unknown..."};
    Initialize(1, fakeArgv, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
