#pragma once

#include "pipe.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/public.h>

#include <atomic>
#include <vector>
#include <array>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TErrorOr<TString> ResolveBinaryPath(const TString& binary);
std::vector<TString> GetEnviron();
bool TryKillProcessByPid(int pid, int signal);

////////////////////////////////////////////////////////////////////////////////

class TProcessBase
    : public TRefCounted
{
public:
    explicit TProcessBase(const TString& path);

    void AddArgument(TStringBuf arg);
    void AddEnvVar(TStringBuf var);

    void AddArguments(std::initializer_list<TStringBuf> args);
    void AddArguments(const std::vector<TString>& args);

    void SetWorkingDirectory(const TString& path);
    void CreateProcessGroup();

    virtual NNet::IConnectionWriterPtr GetStdInWriter() = 0;
    virtual NNet::IConnectionReaderPtr GetStdOutReader() = 0;
    virtual NNet::IConnectionReaderPtr GetStdErrReader() = 0;

    //! Returns process completion future, which ends with EErrorCode::OK or EProcessErrorCode.
    TFuture<void> Spawn();

    virtual void Kill(int signal) = 0;

    TString GetPath() const;
    int GetProcessId() const;
    bool IsStarted() const;
    bool IsFinished() const;

    TString GetCommandLine() const;

protected:
    const TString Path_;

    int ProcessId_;
    std::atomic<bool> Started_ = {false};
    std::atomic<bool> Finished_ = {false};
    int MaxSpawnActionFD_ = - 1;
    NPipes::TPipe Pipe_;
    // Container for owning string data. Use std::deque because it never moves contained objects.
    std::deque<std::string> StringHolders_;
    std::vector<const char*> Args_;
    std::vector<const char*> Env_;
    TString ResolvedPath_;
    TString WorkingDirectory_;
    bool CreateProcessGroup_ = false;
    TPromise<void> FinishedPromise_ = NewPromise<void>();

    virtual void DoSpawn() = 0;
    const char* Capture(TStringBuf arg);

private:
    void SpawnChild();
    void ValidateSpawnResult();
    void Child();
    void AsyncPeriodicTryWait();
};

DEFINE_REFCOUNTED_TYPE(TProcessBase)

////////////////////////////////////////////////////////////////////////////////

// Read this
// http://ewontfix.com/7/
// before making any changes.
class TSimpleProcess
    : public TProcessBase
{
public:
    explicit TSimpleProcess(
        const TString& path,
        bool copyEnv = true,
        TDuration pollPeriod = TDuration::MilliSeconds(100));
    // We move dtor in .cpp file to avoid
    // instantiation of ~std::unique_ptr of a forward
    // declared class.
    ~TSimpleProcess();

    void Kill(int signal) override;
    NNet::IConnectionWriterPtr GetStdInWriter() override;
    NNet::IConnectionReaderPtr GetStdOutReader() override;
    NNet::IConnectionReaderPtr GetStdErrReader() override;

private:
    const TDuration PollPeriod_;

    NPipes::TPipeFactory PipeFactory_;
    std::array<NPipes::TPipe, 3> StdPipes_;

    NConcurrency::TPeriodicExecutorPtr AsyncWaitExecutor_;

    class TProcessSpawnState;
    std::unique_ptr<TProcessSpawnState> SpawnState_;

    void AddDup2FileAction(int oldFD, int newFD);

    void PrepareErrorPipe();
    void CloseErrorPipe();

    void PrepareSpawnActions(sigset_t* oldSignals);

    void DoSpawn() override;
    void SpawnChild();
    void AsyncPeriodicTryWait();

    void ValidateSpawnResult();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
