#include "subprocess.h"

#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/net/connection.h>

#include <util/system/execpath.h>

#include <array>

namespace NYT {

using namespace NConcurrency;
using namespace NPipes;

////////////////////////////////////////////////////////////////////////////////

const static size_t PipeBlockSize = 64 * 1024;

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Subprocess");

////////////////////////////////////////////////////////////////////////////////

TSubprocess::TSubprocess(TString path, bool copyEnv)
    : Path_(std::move(path))
    , Process_(New<TSimpleProcess>(Path_, copyEnv))
{ }

TSubprocess TSubprocess::CreateCurrentProcessSpawner()
{
    return TSubprocess(GetExecPath());
}

void TSubprocess::AddArgument(TStringBuf arg)
{
    Process_->AddArgument(arg);
}

void TSubprocess::AddArguments(std::initializer_list<TStringBuf> args)
{
    Process_->AddArguments(args);
}

TSubprocessResult TSubprocess::Execute(const TSharedRef& input, TDuration timeout)
{
#ifdef _unix_
    auto killCookie = TDelayedExecutor::Submit(
        BIND([=, path = Path_, process = GetProcess()] {
            YT_LOG_WARNING("Killing process due to timeout (Path: %v, ProcessId: %v, Timeout: %v)",
                path,
                process->GetProcessId(),
                timeout);

            try {
                process->Kill(SIGKILL);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Failed to kill process (Path: %v, ProcessId: %v)",
                    path,
                    process->GetProcessId());
            }
        }),
        timeout);

    auto cookieGuard = Finally([&] {
        TDelayedExecutor::Cancel(killCookie);
    });

    auto inputStream = Process_->GetStdInWriter();
    auto outputStream = Process_->GetStdOutReader();
    auto errorStream = Process_->GetStdErrReader();
    auto finished = Process_->Spawn();

    auto readIntoBlob = [] (IAsyncInputStreamPtr stream) {
        TBlob output;
        auto buffer = TSharedMutableRef::Allocate(PipeBlockSize, {.InitializeStorage = false});
        while (true) {
            auto size = WaitFor(stream->Read(buffer))
                .ValueOrThrow();

            if (size == 0) {
                break;
            }

            // ToDo(psushin): eliminate copying.
            output.Append(buffer.Begin(), size);
        }
        return TSharedRef::FromBlob(std::move(output));
    };

    auto writeStdin = BIND([=] {
        if (input.Size() > 0) {
            WaitFor(inputStream->Write(input))
                .ThrowOnError();
        }

        WaitFor(inputStream->Close())
            .ThrowOnError();

        //! Return dummy ref, so later we cat put Future into vector
        //! along with stdout and stderr.
        return TSharedRef::MakeEmpty();
    });

    std::vector<TFuture<TSharedRef>> futures = {
        BIND(readIntoBlob, outputStream).AsyncVia(GetCurrentInvoker()).Run(),
        BIND(readIntoBlob, errorStream).AsyncVia(GetCurrentInvoker()).Run(),
        writeStdin.AsyncVia(GetCurrentInvoker()).Run(),
    };

    try {
        auto outputsOrError = WaitFor(AllSucceeded(futures));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            outputsOrError,
            "IO error occurred during subprocess call");

        const auto& outputs = outputsOrError.Value();
        YT_VERIFY(outputs.size() == 3);

        // This can block indefinitely.
        auto exitCode = WaitFor(finished);
        return TSubprocessResult{outputs[0], outputs[1], exitCode};
    } catch (...) {
        try {
            Process_->Kill(SIGKILL);
        } catch (...) { }
        Y_UNUSED(WaitFor(finished));
        throw;
    }
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

void TSubprocess::Kill(int signal)
{
    Process_->Kill(signal);
}

TString TSubprocess::GetCommandLine() const
{
    return Process_->GetCommandLine();
}

TProcessBasePtr TSubprocess::GetProcess() const
{
    return Process_;
}

////////////////////////////////////////////////////////////////////////////////

void RunSubprocess(const std::vector<TString>& cmd)
{
    if (cmd.empty()) {
        THROW_ERROR_EXCEPTION("Command can't be empty");
    }

    auto process = TSubprocess(cmd[0]);
    for (int index = 1; index < std::ssize(cmd); ++index) {
        process.AddArgument(cmd[index]);
    }

    auto result = process.Execute();
    if (!result.Status.IsOK()) {
        THROW_ERROR_EXCEPTION("Failed to run %v", cmd[0])
            << result.Status
            << TErrorAttribute("command_line", process.GetCommandLine())
            << TErrorAttribute("error", TString(result.Error.Begin(), result.Error.End()));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
