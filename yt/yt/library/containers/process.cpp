#ifdef __linux__

#include "process.h"

#include <yt/yt/library/containers/instance.h>

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/fs.h>

namespace NYT::NContainers {

using namespace NPipes;
using namespace NNet;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger Logger("Process");

static constexpr pid_t InvalidProcessId = -1;

////////////////////////////////////////////////////////////////////////////////

TPortoProcess::TPortoProcess(
    const TString& path,
    IInstanceLauncherPtr containerLauncher,
    bool copyEnv)
    : TProcessBase(path)
    , ContainerLauncher_(std::move(containerLauncher))
{
    AddArgument(NFS::GetFileName(path));
    if (copyEnv) {
        for (char** envIt = environ; *envIt; ++envIt) {
            Env_.push_back(Capture(*envIt));
        }
    }
}

void TPortoProcess::Kill(int signal)
{
    if (auto instance = GetInstance()) {
        instance->Kill(signal);
    }
}

void TPortoProcess::DoSpawn()
{
    YT_VERIFY(ProcessId_ == InvalidProcessId && !Finished_);
    YT_VERIFY(!GetInstance());
    YT_VERIFY(!Started_);
    YT_VERIFY(!Args_.empty());

    if (!WorkingDirectory_.empty()) {
        ContainerLauncher_->SetCwd(WorkingDirectory_);
    }

    Started_ = true;

    try {
        // TPortoProcess doesn't support running processes inside rootFS.
        YT_VERIFY(!ContainerLauncher_->HasRoot());
        std::vector<TString> args(Args_.begin() + 1, Args_.end());
        auto instance = WaitFor(ContainerLauncher_->Launch(ResolvedPath_, args, DecomposeEnv()))
            .ValueOrThrow();
        ContainerInstance_.Store(instance);
        FinishedPromise_.SetFrom(instance->Wait());

        try {
            ProcessId_ = instance->GetPid();
        } catch (const std::exception& ex) {
            // This could happen if Porto container has already died or pid namespace of
            // parent container is not a parent of pid namespace of child container.
            // It's not a problem, since for Porto process pid is used for logging purposes only.
            YT_LOG_DEBUG(ex, "Failed to get pid of root process (Container: %v)",
                instance->GetName());
        }

        YT_LOG_DEBUG("Process inside Porto spawned successfully (Path: %v, ExternalPid: %v, Container: %v)",
            ResolvedPath_,
            ProcessId_,
            instance->GetName());

        FinishedPromise_.ToFuture().Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& exitStatus) {
            Finished_ = true;
            if (exitStatus.IsOK()) {
                YT_LOG_DEBUG("Process inside Porto exited gracefully (ExternalPid: %v, Container: %v)",
                    ProcessId_,
                    instance->GetName());
            } else {
                YT_LOG_DEBUG(exitStatus, "Process inside Porto exited with an error (ExternalPid: %v, Container: %v)",
                    ProcessId_,
                    instance->GetName());
            }
        }));
    } catch (const std::exception& ex) {
        Finished_ = true;
        THROW_ERROR_EXCEPTION("Failed to start child process inside Porto")
            << TErrorAttribute("path", ResolvedPath_)
            << TErrorAttribute("container", ContainerLauncher_->GetName())
            << ex;
    }
}

IInstancePtr TPortoProcess::GetInstance()
{
    return ContainerInstance_.Acquire();
}

THashMap<TString, TString> TPortoProcess::DecomposeEnv() const
{
    THashMap<TString, TString> result;
    for (const auto& env : Env_) {
        TStringBuf name, value;
        TStringBuf(env).TrySplit('=', name, value);
        result[name] = value;
    }
    return result;
}

static TString CreateStdIONamedPipePath()
{
    const TString name = ToString(TGuid::Create());
    return NFS::GetRealPath(NFS::CombinePaths("/tmp", name));
}

IConnectionWriterPtr TPortoProcess::GetStdInWriter()
{
    auto pipe = TNamedPipe::Create(CreateStdIONamedPipePath());
    ContainerLauncher_->SetStdIn(pipe->GetPath());
    NamedPipes_.push_back(pipe);
    return pipe->CreateAsyncWriter();
}

IConnectionReaderPtr TPortoProcess::GetStdOutReader()
{
    auto pipe = TNamedPipe::Create(CreateStdIONamedPipePath());
    ContainerLauncher_->SetStdOut(pipe->GetPath());
    NamedPipes_.push_back(pipe);
    return pipe->CreateAsyncReader();
}

IConnectionReaderPtr TPortoProcess::GetStdErrReader()
{
    auto pipe = TNamedPipe::Create(CreateStdIONamedPipePath());
    ContainerLauncher_->SetStdErr(pipe->GetPath());
    NamedPipes_.push_back(pipe);
    return pipe->CreateAsyncReader();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers

#endif
