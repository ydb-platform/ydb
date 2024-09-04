#include "process.h"
#include "pipe.h"

#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <library/cpp/yt/system/handle_eintr.h>
#include <library/cpp/yt/system/exit.h>

#include <util/folder/dirut.h>

#include <util/generic/guid.h>

#include <util/string/ascii.h>
#include <util/string/util.h>

#include <util/system/env.h>
#include <util/system/execpath.h>
#include <util/system/maxlen.h>
#include <util/system/shellcommand.h>

#ifdef _unix_
  #include <unistd.h>
  #include <errno.h>
  #include <sys/wait.h>
  #include <sys/resource.h>
  #include <spawn.h>
#endif

#ifdef _darwin_
  #include <crt_externs.h>
  #define environ (*_NSGetEnviron())
#endif

#if defined(__APPLE__)
    #define YT_USE_POSIX_SPAWN_API
#endif

#if defined(YT_USE_POSIX_SPAWN_API)
    #include <spawn.h>
#endif

namespace NYT {

using namespace NPipes;
using namespace NNet;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Process");

static constexpr pid_t InvalidProcessId = -1;

static constexpr int ResolveRetryCount = 5;
static constexpr auto ResolveRetryTimeout = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

#if defined(YT_USE_POSIX_SPAWN_API)

class TPosixSpawnFileActions
{
public:
    TPosixSpawnFileActions()
    {
        THROW_ERROR_EXCEPTION_IF(
            ::posix_spawn_file_actions_init(&Actions_) != 0,
            TError("Failed to initialize spawn file actions object")
                << TError::FromSystem());
    }

    ~TPosixSpawnFileActions()
    {
        auto res = ::posix_spawn_file_actions_destroy(&Actions_);
        YT_LOG_FATAL_UNLESS(
            res != 0,
            "Failed to destroy spawn file actions object");
    }

    void AddDup2FileAction(int oldFD, int newFD)
    {
        THROW_ERROR_EXCEPTION_IF(
            ::posix_spawn_file_actions_adddup2(&Actions_, oldFD, newFD) != 0,
            TError("Failed to add dup2 file action (OldFD: %v, NewFD: %v)", oldFD, newFD)
                << TError::FromSystem());
    }

    void AddChdirFileAction(TString path)
    {
        THROW_ERROR_EXCEPTION_IF(
            ::posix_spawn_file_actions_addchdir_np(&Actions_, path.c_str()) != 0,
            TError("Failed to add chdir file actions (Path: %v)", path)
                << TError::FromSystem());
    }

    posix_spawn_file_actions_t* Get()
    {
        return &Actions_;
    }

private:
    posix_spawn_file_actions_t Actions_;
};

////////////////////////////////////////////////////////////////////////////////

class TPosixSpawnAttrs
{
public:
    TPosixSpawnAttrs()
    {
        THROW_ERROR_EXCEPTION_IF(
            ::posix_spawnattr_init(&Actions_) != 0,
            TError("Failed to initialize spawnattrs object")
                << TError::FromSystem());
    }

    ~TPosixSpawnAttrs()
    {
        auto res = ::posix_spawnattr_destroy(&Actions_);

        YT_LOG_FATAL_UNLESS(
            res != 0,
            "Failed to destroy spawnattrs object");
    }

    void AddResetAllSignals()
    {
        sigset_t allBlocked;
        sigfillset(&allBlocked);

        THROW_ERROR_EXCEPTION_IF(
            ::posix_spawnattr_setsigdefault(&Actions_, &allBlocked) != 0,
            TError("Failed to set default signal handlers for spawnattrs")
                << TError::FromSystem());

        UpdateFlags(POSIX_SPAWN_SETSIGDEF);
    }

    void AddSetSigMask(sigset_t* sigset)
    {
        THROW_ERROR_EXCEPTION_IF(
            ::posix_spawnattr_setsigmask(&Actions_, sigset) != 0,
            TError("Failed to set signal mask for spawnattrs")
                << TError::FromSystem());

        UpdateFlags(POSIX_SPAWN_SETSIGMASK);
    }

    void AddCreateProcessGroup()
    {
        THROW_ERROR_EXCEPTION_IF(
            ::posix_spawnattr_setpgroup(&Actions_, 0) != 0,
            TError("Failed to set pgroup value to 0 of spawnattrs")
                << TError::FromSystem());

        UpdateFlags(POSIX_SPAWN_SETPGROUP);
    }

    posix_spawnattr_t* Get()
    {
        return &Actions_;
    }

private:
    posix_spawnattr_t Actions_;

    void UpdateFlags(int mask)
    {
        auto currentFlags = GetFlags();

        THROW_ERROR_EXCEPTION_IF(
            ::posix_spawnattr_setflags(&Actions_, currentFlags | mask) != 0,
            TError("Failed to set flags to spawnattrs (OldFlags: %v, Mask: %v)", currentFlags, mask)
                << TError::FromSystem());
    }

    short GetFlags() const
    {
        short flags;
        THROW_ERROR_EXCEPTION_IF(
            ::posix_spawnattr_getflags(&Actions_, &flags) != 0,
            TError("Failed to flags from spawnattrs")
                << TError::FromSystem());
        return flags;
    }
};

#endif

////////////////////////////////////////////////////////////////////////////////

TErrorOr<TString> ResolveBinaryPath(const TString& binary)
{
    auto Logger = NYT::Logger()
        .WithTag("Binary: %v", binary);

    YT_LOG_DEBUG("Resolving binary path");

    std::vector<TError> accumulatedErrors;

    auto test = [&] (const char* path) {
        YT_LOG_DEBUG("Probing path (Path: %v)", path);
        if (access(path, R_OK | X_OK) == 0) {
            return true;
        } else {
            auto error = TError("Cannot run %Qlv", path) << TError::FromSystem();
            accumulatedErrors.push_back(std::move(error));
            return false;
        }
    };

    auto failure = [&] {
        auto error = TError(
            EProcessErrorCode::CannotResolveBinary,
            "Cannot resolve binary %Qlv",
            binary);
        error.MutableInnerErrors()->swap(accumulatedErrors);
        YT_LOG_DEBUG(error, "Error resolving binary path");
        return error;
    };

    auto success = [&] (const TString& path) {
        YT_LOG_DEBUG("Binary resolved (Path: %v)", path);
        return path;
    };

    if (test(binary.c_str())) {
        return success(binary);
    }

    // If this is an absolute path, stop here.
    if (binary.empty() || binary[0] == '/') {
        return failure();
    }

    // XXX(sandello): Sometimes we drop PATH from environment when spawning isolated processes.
    // In this case, try to locate somewhere nearby.
    {
        auto execPathDirName = GetDirName(GetExecPath());
        YT_LOG_DEBUG("Looking in our exec path directory (ExecPathDir: %v)", execPathDirName);
        auto probe = TString::Join(execPathDirName, "/", binary);
        if (test(probe.c_str())) {
            return success(probe);
        }
    }

    std::array<char, MAX_PATH> buffer;

    auto envPathStr = GetEnv("PATH");
    TStringBuf envPath(envPathStr);
    TStringBuf envPathItem;

    YT_LOG_DEBUG("Looking for binary in PATH (Path: %v)", envPathStr);

    while (envPath.NextTok(':', envPathItem)) {
        if (buffer.size() < 2 + envPathItem.size() + binary.size()) {
            continue;
        }

        size_t index = 0;
        std::copy(envPathItem.begin(), envPathItem.end(), buffer.begin() + index);
        index += envPathItem.size();
        buffer[index] = '/';
        index += 1;
        std::copy(binary.begin(), binary.end(), buffer.begin() + index);
        index += binary.size();
        buffer[index] = 0;

        if (test(buffer.data())) {
            return success(TString(buffer.data(), index));
        }
    }

    return failure();
}

std::vector<TString> GetEnviron()
{
    std::vector<TString> env;
    size_t size = 0;
    for (char** envIt = environ; *envIt; ++envIt) {
        ++size;
    }
    env.reserve(size);
    for (char** envIt = environ; *envIt; ++envIt) {
        env.emplace_back(*envIt);
    }
    return env;
}

bool TryKillProcessByPid(int pid, int signal)
{
#ifdef _unix_
    YT_VERIFY(pid != -1);
    int result = ::kill(pid, signal);
    // Ignore ESRCH because process may have died just before TryKillProcessByPid.
    if (result < 0 && errno != ESRCH) {
        return false;
    }
    return true;
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

////////////////////////////////////////////////////////////////////////////////

namespace {

#ifdef _unix_

bool TryWaitid(idtype_t idtype, id_t id, siginfo_t *infop, int options)
{
    if (infop != nullptr) {
        // See comment below.
        infop->si_pid = 0;
    }

    siginfo_t info;
    ::memset(&info, 0, sizeof(info));
    auto res = HandleEintr(::waitid, idtype, id, infop != nullptr ? infop : &info, options);

    if (res == 0) {
        // According to man wait.
        // If WNOHANG was specified in options and there were
        // no children in a waitable state, then waitid() returns 0 immediately.
        // To distinguish this case from that where a child
        // was in a waitable state, zero out the si_pid field
        // before the call and check for a nonzero value in this field after
        // the call returns.
        if (infop && infop->si_pid == 0) {
            return false;
        }
        return true;
    }

    return false;
}

void Wait4OrDie(pid_t id, int* status, int options, rusage* rusage)
{
    auto res = HandleEintr(::wait4, id, status, options, rusage);
    if (res == -1) {
        YT_LOG_FATAL(TError::FromSystem(), "Wait4 failed");
    }
}

bool TrySetSignalMask(const sigset_t* sigmask, sigset_t* oldSigmask)
{
    int error = pthread_sigmask(SIG_SETMASK, sigmask, oldSigmask);
    if (error != 0) {
        return false;
    }
    return true;
}

#if !defined(YT_USE_POSIX_SPAWN_API)

void Cleanup(int pid)
{
    YT_VERIFY(pid > 0);

    YT_VERIFY(TryKillProcessByPid(pid, 9));
    YT_VERIFY(TryWaitid(P_PID, pid, nullptr, WEXITED));
}

bool TryResetSignals()
{
    for (int sig = 1; sig < NSIG; ++sig) {
        // Ignore invalid signal errors.
        ::signal(sig, SIG_DFL);
    }
    return true;
}

#endif

#endif

} // namespace

////////////////////////////////////////////////////////////////////////////////

TProcessBase::TProcessBase(const TString& path)
    : Path_(path)
    , ProcessId_(InvalidProcessId)
{ }

void TProcessBase::AddArgument(TStringBuf arg)
{
    YT_VERIFY(ProcessId_ == InvalidProcessId && !Finished_);

    Args_.push_back(Capture(arg));
}

void TProcessBase::AddEnvVar(TStringBuf var)
{
    YT_VERIFY(ProcessId_ == InvalidProcessId && !Finished_);

    Env_.push_back(Capture(var));
}

void TProcessBase::AddArguments(std::initializer_list<TStringBuf> args)
{
    for (auto arg : args) {
        AddArgument(arg);
    }
}

void TProcessBase::AddArguments(const std::vector<TString>& args)
{
    for (const auto& arg : args) {
        AddArgument(arg);
    }
}

void TProcessBase::SetWorkingDirectory(const TString& path)
{
    WorkingDirectory_ = path;
}

void TProcessBase::CreateProcessGroup()
{
    CreateProcessGroup_ = true;
}

////////////////////////////////////////////////////////////////////////////////

class TSimpleProcess::TProcessSpawnState
{
public:
    TProcessSpawnState() = default;

#if !defined(YT_USE_POSIX_SPAWN_API)
    void SetErrorPipe(TPipe* errorPipe)
    {
        Pipe_ = errorPipe;
    }

    TError ValidateSpawnResult()
    {
    #ifdef _unix_
        int data[2];
        ssize_t res;
        YT_VERIFY(Pipe_);
        res = HandleEintr(::read, Pipe_->GetReadFD(), &data, sizeof(data));
        Pipe_->CloseReadFD();

        if (res == 0) {
            return {};
        }

        YT_VERIFY(res == sizeof(data));

        int actionIndex = data[0];
        int errorCode = data[1];

        YT_VERIFY(0 <= actionIndex && actionIndex < std::ssize(SpawnActions_));
        const auto& action = SpawnActions_[actionIndex];

        return TError("%v", action.ErrorMessage)
            << TError::FromSystem(errorCode);
    #else
        THROW_ERROR_EXPECTION("Unsupported platform");
    #endif
    }
#endif

    void AddDup2FileAction(int oldFD, int newFD)
    {
    #if defined(YT_USE_POSIX_SPAWN_API)
        SpawnFileActions_.AddDup2FileAction(oldFD, newFD);
    #else
        TSpawnAction action{
            std::bind(TryDup2, oldFD, newFD),
            Format("Error duplicating %v file descriptor to %v in child process", oldFD, newFD)
        };
        SpawnActions_.push_back(action);
    #endif
    }

    void AddSignalSafetySpawnActions(sigset_t* oldSignals)
    {
    #if defined(YT_USE_POSIX_SPAWN_API)
        SpawnAttrs_.AddResetAllSignals();
        SpawnAttrs_.AddSetSigMask(oldSignals);
    #else
        SpawnActions_.push_back(TSpawnAction{
            TryResetSignals,
            "Error resetting signals to default disposition in child process: signal failed"
        });

        SpawnActions_.push_back(TSpawnAction{
            std::bind(TrySetSignalMask, oldSignals, nullptr),
            "Error unblocking signals in child process: pthread_sigmask failed"
        });
    #endif
    }

    void AddChdirSpawnAction(const TString& workingDirectory)
    {
    #if defined(YT_USE_POSIX_SPAWN_API)
        SpawnFileActions_.AddChdirFileAction(workingDirectory);
    #else
        SpawnActions_.push_back(TSpawnAction{
            [&] {
                NFs::SetCurrentWorkingDirectory(workingDirectory);
                return true;
            },
            "Error changing working directory"
        });
    #endif
    }

    void AddCreateProcessGroupAction()
    {
    #if defined(YT_USE_POSIX_SPAWN_API)
        SpawnAttrs_.AddCreateProcessGroup();
    #else
        SpawnActions_.push_back(TSpawnAction{
            [&] {
                setpgrp();
                return true;
            },
            "Error creating process group"
        });
    #endif
    }

    void AddExecvSpawnAction(
        const char* resolvedPath,
        char* const* argv,
        char* const* env)
    {
        Path_ = resolvedPath;
        Argv_ = argv;
        Env_ = env;
    #if defined(YT_USE_POSIX_SPAWN_API)
        // This step is automatic for posix_spawn.
    #else
        SpawnActions_.push_back(TSpawnAction{
            [=] {
                // Execve may fail, if called binary is being updated, e.g. during yandex-yt package update
                // with errno ETXTBSY - executable file is open for writing. For example see YT-6352.
                // Initiator could retry after getting error EProcessErrorCode::CannotStartProcess.
                return TryExecve(resolvedPath, argv, env);
            },
            "Error starting child process: execve failed"
        });
    #endif
    }

    pid_t SpawnChild()
    {
    #if defined(YT_USE_POSIX_SPAWN_API)
        return DoSpawnChildPosixSpawn();
    #else
        return DoSpawnChildVFork();
    #endif
    }

private:
    const char* Path_;
    char* const* Argv_;
    char* const* Env_;

#if defined(YT_USE_POSIX_SPAWN_API)
    TPosixSpawnFileActions SpawnFileActions_;
    TPosixSpawnAttrs SpawnAttrs_;
#else
    struct TSpawnAction
    {
        std::function<bool()> Callback;
        TString ErrorMessage;
    };

    std::vector<TSpawnAction> SpawnActions_;

    TPipe* Pipe_;
#endif

#if defined(YT_USE_POSIX_SPAWN_API)
    pid_t DoSpawnChildPosixSpawn()
    {
        pid_t pid;

        auto result = ::posix_spawn(
            &pid,
            Path_,
            SpawnFileActions_.Get(),
            SpawnAttrs_.Get(),
            Argv_,
            Env_);

        THROW_ERROR_EXCEPTION_IF(
            result != 0,
            TError("Failed to spawn process with posix_spawn")
                << TError::FromSystem());

        return pid;
    }

#else
    pid_t DoSpawnChildVFork()
    {
        // NB: fork() copy-on-write cause undefined behaviour when run concurrently with
        // Disk IO on O_DIRECT file descriptor. vfork don't suffer from the same issue.
        // NB: vfork() blocks parent until child executes new program or exits.
        int pid = vfork();

        if (pid < 0) {
            THROW_ERROR_EXCEPTION("Error starting child process: vfork failed")
                << TErrorAttribute("path", Path_)
                << TError::FromSystem();
        }

        if (pid == 0) {
            try {
                // Successful call to Execve leaves
                // the current context and starts execution
                // of a specified binary. Child never returns
                // from this call.
                Child();
            } catch (...) {
                YT_ABORT();
            }
        }

        // Parent returns.
        return pid;
    }

    [[noreturn]] void Child()
    {
        for (int actionIndex = 0; actionIndex < std::ssize(SpawnActions_); ++actionIndex) {
            auto& action = SpawnActions_[actionIndex];
            if (!action.Callback()) {
                // Report error through the pipe.
                int data[] = {
                    actionIndex,
                    errno
                };

                // According to pipe(7) write of small buffer is atomic.
                YT_VERIFY(Pipe_);
                ssize_t size = HandleEintr(::write, Pipe_->GetWriteFD(), &data, sizeof(data));
                YT_VERIFY(size == sizeof(data));
                AbortProcess(ToUnderlying(EProcessExitCode::GenericError));
            }
        }
        YT_ABORT();
    }

#endif
};

////////////////////////////////////////////////////////////////////////////////

TSimpleProcess::TSimpleProcess(const TString& path, bool copyEnv, TDuration pollPeriod)
    // TString is guaranteed to be zero-terminated.
    // https://wiki.yandex-team.ru/Development/Poisk/arcadia/util/TStringAndTStringBuf#sobstvennosimvoly
    : TProcessBase(path)
    , PollPeriod_(pollPeriod)
    , PipeFactory_(3)
    , SpawnState_(std::make_unique<TProcessSpawnState>())
{
    AddArgument(path);

    if (copyEnv) {
        for (char** envIt = environ; *envIt; ++envIt) {
            Env_.push_back(Capture(*envIt));
        }
    }
}

TSimpleProcess::~TSimpleProcess()
{ }

void TSimpleProcess::AddDup2FileAction(int oldFD, int newFD)
{
    SpawnState_->AddDup2FileAction(oldFD, newFD);
    MaxSpawnActionFD_ = std::max(MaxSpawnActionFD_, newFD);
}

IConnectionReaderPtr TSimpleProcess::GetStdOutReader()
{
    auto& pipe = StdPipes_[STDOUT_FILENO];
    pipe = PipeFactory_.Create();
    AddDup2FileAction(pipe.GetWriteFD(), STDOUT_FILENO);
    return pipe.CreateAsyncReader();
}

IConnectionReaderPtr TSimpleProcess::GetStdErrReader()
{
    auto& pipe = StdPipes_[STDERR_FILENO];
    pipe = PipeFactory_.Create();
    AddDup2FileAction(pipe.GetWriteFD(), STDERR_FILENO);
    return pipe.CreateAsyncReader();
}

IConnectionWriterPtr TSimpleProcess::GetStdInWriter()
{
    auto& pipe = StdPipes_[STDIN_FILENO];
    pipe = PipeFactory_.Create();
    AddDup2FileAction(pipe.GetReadFD(), STDIN_FILENO);
    return pipe.CreateAsyncWriter();
}

TFuture<void> TProcessBase::Spawn()
{
    try {
        // Resolve binary path.
        std::vector<TError> innerErrors;
        for (int retryIndex = ResolveRetryCount; retryIndex >= 0; --retryIndex) {
            auto errorOrPath = ResolveBinaryPath(Path_);
            if (errorOrPath.IsOK()) {
                ResolvedPath_ = errorOrPath.Value();
                break;
            }

            innerErrors.push_back(errorOrPath);

            if (retryIndex == 0) {
                THROW_ERROR_EXCEPTION("Failed to resolve binary path %v", Path_)
                    << innerErrors;
            }

            TDelayedExecutor::WaitForDuration(ResolveRetryTimeout);
        }

        DoSpawn();
    } catch (const std::exception& ex) {
        FinishedPromise_.TrySet(TError(EProcessErrorCode::CannotStartProcess,
            "Cannot spawn child process %v",
            ResolvedPath_)
            << ex);
    }
    return FinishedPromise_;
}

void TSimpleProcess::DoSpawn()
{
#ifdef _unix_
    auto finally = Finally([&] {
        StdPipes_[STDIN_FILENO].CloseReadFD();
        StdPipes_[STDOUT_FILENO].CloseWriteFD();
        StdPipes_[STDERR_FILENO].CloseWriteFD();
        PipeFactory_.Clear();
    });

    YT_VERIFY(ProcessId_ == InvalidProcessId && !Finished_);

    // Make sure no spawn action closes Pipe_.WriteFD
    PrepareErrorPipe();

    Env_.push_back(nullptr);
    Args_.push_back(nullptr);

    // Block all signals around vfork; see http://ewontfix.com/7/

    // As the child may run in the same address space as the parent until
    // the actual execve() system call, any (custom) signal handlers that
    // the parent has might alter parent's memory if invoked in the child,
    // with undefined results.  So we block all signals in the parent before
    // vfork(), which will cause them to be blocked in the child as well (we
    // rely on the fact that Linux, just like all sane implementations, only
    // clones the calling thread).  Then, in the child, we reset all signals
    // to their default dispositions (while still blocked), and unblock them
    // (so the exec()ed process inherits the parent's signal mask)

    sigset_t allBlocked;
    sigfillset(&allBlocked);
    sigset_t oldSignals;

    if (!TrySetSignalMask(&allBlocked, &oldSignals)) {
        THROW_ERROR_EXCEPTION("Failed to block all signals")
            << TError::FromSystem();
    }

    PrepareSpawnActions(&oldSignals);

    SpawnChild();

    // This should not fail ever.
    YT_VERIFY(TrySetSignalMask(&oldSignals, nullptr));

    CloseErrorPipe();

    ValidateSpawnResult();

    AsyncWaitExecutor_ = New<TPeriodicExecutor>(
        GetSyncInvoker(),
        BIND(&TSimpleProcess::AsyncPeriodicTryWait, MakeStrong(this)),
        PollPeriod_);

    AsyncWaitExecutor_->Start();
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

void TSimpleProcess::PrepareErrorPipe()
{
#if defined(YT_USE_POSIX_SPAWN_API)
    YT_LOG_DEBUG("Spawning new process (Path: %v, Arguments: %v, Environment: %v)",
        ResolvedPath_,
        Args_,
        Env_);
#else
    TPipeFactory pipeFactory(MaxSpawnActionFD_ + 1);
    Pipe_ = pipeFactory.Create();
    SpawnState_->SetErrorPipe(&Pipe_);

    pipeFactory.Clear();

    YT_LOG_DEBUG("Spawning new process (Path: %v, ErrorPipe: %v, Arguments: %v, Environment: %v)",
        ResolvedPath_,
        Pipe_,
        Args_,
        Env_);
#endif
}

void TSimpleProcess::CloseErrorPipe()
{
#if !defined(YT_USE_POSIX_SPAWN_API)
    Pipe_.CloseWriteFD();
#endif
}

void TSimpleProcess::ValidateSpawnResult()
{
#if defined (YT_USE_POSIX_SPAWN_API)

    YT_LOG_DEBUG("Child process spawned successfully (Pid: %v)", ProcessId_);
    return;
#else
    auto result = SpawnState_->ValidateSpawnResult();

    if (result.IsOK()) {
        // Child successfully spawned or was killed by a signal.
        // But there is no way to distinguish between these two cases:
        // * child killed by signal before exec
        // * child killed by signal after exec
        // So we treat kill-before-exec the same way as kill-after-exec.
        YT_LOG_DEBUG("Child process spawned successfully (Pid: %v)", ProcessId_);
        return;
    }

    Finished_ = true;

    Cleanup(ProcessId_);
    ProcessId_ = InvalidProcessId;

    THROW_ERROR_EXCEPTION(std::move(result));
#endif
}

void TSimpleProcess::PrepareSpawnActions(sigset_t* oldSignals)
{
    SpawnState_->AddSignalSafetySpawnActions(oldSignals);

    if (!WorkingDirectory_.empty()) {
        SpawnState_->AddChdirSpawnAction(WorkingDirectory_);
    }

    if (CreateProcessGroup_) {
        SpawnState_->AddCreateProcessGroupAction();
    }

    SpawnState_->AddExecvSpawnAction(
        ResolvedPath_.c_str(),
        const_cast<char* const*>(Args_.data()),
        const_cast<char* const*>(Env_.data()));
}

void TSimpleProcess::SpawnChild()
{
#ifdef _unix_
    ProcessId_ = SpawnState_->SpawnChild();
    Started_ = true;
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

void TSimpleProcess::AsyncPeriodicTryWait()
{
#ifdef _unix_
    siginfo_t processInfo;
    memset(&processInfo, 0, sizeof(siginfo_t));

    // Note WNOWAIT flag.
    // This call just waits for a process to be finished but does not clear zombie flag.

    if (!TryWaitid(P_PID, ProcessId_, &processInfo, WEXITED | WNOWAIT | WNOHANG) ||
        processInfo.si_pid != ProcessId_)
    {
        return;
    }

    YT_UNUSED_FUTURE(AsyncWaitExecutor_->Stop());
    AsyncWaitExecutor_ = nullptr;

    // This call just should return immediately
    // because we have already waited for this process with WNOHANG
    rusage rusage;
    Wait4OrDie(ProcessId_, nullptr, WNOHANG, &rusage);

    Finished_ = true;
    auto error = ProcessInfoToError(processInfo);
    YT_LOG_DEBUG(error, "Process finished (Pid: %v, MajorFaults: %v)",
        ProcessId_,
        rusage.ru_majflt);

    FinishedPromise_.Set(error);
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

void TSimpleProcess::Kill(int signal)
{
#ifdef _unix_
    if (!Started_) {
        THROW_ERROR_EXCEPTION("Process is not started yet");
    }

    if (Finished_) {
        return;
    }

    YT_LOG_DEBUG("Killing child process (Pid: %v)", ProcessId_);

    bool result = false;
    if (!CreateProcessGroup_) {
        result = TryKillProcessByPid(ProcessId_, signal);
    } else {
        result = TryKillProcessByPid(-1 * ProcessId_, signal);
    }

    if (!result) {
        THROW_ERROR_EXCEPTION("Failed to kill child process %v", ProcessId_)
            << TError::FromSystem();
    }
    return;
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

TString TProcessBase::GetPath() const
{
    return Path_;
}

int TProcessBase::GetProcessId() const
{
    return ProcessId_;
}

bool TProcessBase::IsStarted() const
{
    return Started_;
}

bool TProcessBase::IsFinished() const
{
    return Finished_;
}

TString TProcessBase::GetCommandLine() const
{
    TStringBuilder builder;
    builder.AppendString(Path_);

    bool first = true;
    for (const auto& arg_ : Args_) {
        TStringBuf arg(arg_);
        if (first) {
            first = false;
        } else {
            if (arg) {
                builder.AppendChar(' ');
                bool needQuote = false;
                for (size_t i = 0; i < arg.length(); ++i) {
                    if (!IsAsciiAlnum(arg[i]) &&
                        arg[i] != '-' && arg[i] != '_' && arg[i] != '=' && arg[i] != '/')
                    {
                        needQuote = true;
                        break;
                    }
                }
                if (needQuote) {
                    builder.AppendChar('"');
                    TStringBuf left, right;
                    while (arg.TrySplit('"', left, right)) {
                        builder.AppendString(left);
                        builder.AppendString("\\\"");
                        arg = right;
                    }
                    builder.AppendString(arg);
                    builder.AppendChar('"');
                } else {
                    builder.AppendString(arg);
                }
            }
        }
    }

    return builder.Flush();
}

const char* TProcessBase::Capture(TStringBuf arg)
{
    StringHolders_.push_back(TString(arg));
    return StringHolders_.back().c_str();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
