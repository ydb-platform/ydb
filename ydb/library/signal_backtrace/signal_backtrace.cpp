#include "signal_backtrace.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

#include <library/cpp/dwarf_backtrace/backtrace.h>
#include <library/cpp/logger/log.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>

#include <sys/wait.h>

using namespace NKikimr;

namespace {

    using TSignalHandlerFn = std::function<void(int, siginfo_t*, void*)>;

    void SetSignalHandler(int signal, TSignalHandlerFn&& newHandler, struct sigaction* oldAction) {
        static std::array<TSignalHandlerFn, NSIG> signalHandlers;

        Y_VERIFY_S(signal > 0 && signal < NSIG, "Unsupported signal: " << signal);
        signalHandlers[signal] = std::move(newHandler);

        struct sigaction action;
        memset(&action, 0, sizeof(action));
        action.sa_flags = SA_SIGINFO | SA_RESTART;
        if (signal == SIGCHLD) {
            action.sa_flags |= SA_NOCLDSTOP; // always receive SIGCHLD only on process termination.
        }
        action.sa_sigaction = [](int sig, siginfo_t* info, void* ucontext) {
            signalHandlers[sig](sig, info, ucontext);
        };
        sigaction(signal, &action, oldAction);
    }

} // namespace

class TTraceCollector::TPipeConnection {
public:
    TPipeConnection() {
        TPipe::Pipe(ReadPipe, WritePipe);
    }

    ~TPipeConnection() {
        ReadPipe.Close();
        WritePipe.Close();
    }

    size_t Write(const void* buffer, size_t size) const {
        return WritePipe.Write(buffer, size);
    }

    size_t Read(void* buffer, size_t size) const {
        return ReadPipe.Read(buffer, size);
    }

    void CloseRead() {
        ReadPipe.Close();
    }
    void CloseWrite() {
        WritePipe.Close();
    }

private:
    TPipe ReadPipe;
    TPipe WritePipe;
};

class TTraceCollector::TStackTrace {
public:
    TStackTrace() : Size(BackTrace(Backtrace.data(), Backtrace.size())) {}

    inline const void* const* Get() const {
        return Backtrace.data();
    }

    inline size_t GetSize() const {
        return Size;
    }

private:
    static_assert(PIPE_BUF >= 512);

    // The constant 60 is used because PIPE_BUF >= 512 and
    // sizeof(TStackTrace) = Backtrace.size() * 8 + 8 should be <= 512
    std::array<void*, 60> Backtrace;
    const size_t Size;
};

// static
const THashSet<int> TTraceCollector::DEFAULT_SIGNALS = {SIGABRT, SIGBUS, SIGILL, SIGSEGV};

TTraceCollector::TTraceCollector(const THashSet<int>& signalHandlers)
    : HandledSignals(signalHandlers)
    , Connection(MakeHolder<TPipeConnection>())
{
    static_assert(sizeof(TStackTrace) <= PIPE_BUF, "Reading and writing TStackTrace to the pipe should be atomic");

    CollectorPid = fork();

    if (CollectorPid < 0) {
        // TODO: DISTBUILD_LOG(ERROR, "Failed to fork process: " << strerror(-CollectorPid));
    } else if (CollectorPid == 0) {
        // TODO:
        // if (logBackend) {
        //     auto* ptr = logBackend.Get();
        //     ChangeLogBackend(std::move(logBackend), ptr);
        // }
        // Trace collector process
        RunChildMain();
    } else {
        // Main process
        Connection->CloseRead();
        SetSignalHandlers();

        // TODO: DISTBUILD_LOG(INFO, "Trace collector pid: " << CollectorPid);
    }
}

TTraceCollector::~TTraceCollector() {
    Connection->CloseWrite();
    if (CollectorPid != -1) {
        waitpid(CollectorPid, nullptr, 0);
        RestoreSignalHandlers();
    }
}

void TTraceCollector::SetSignalHandlers() {
    // Send the stacktrace when the handled signal is received
    for (const auto signal: HandledSignals) {
        // SIGCHLD cannot be caught because it has a special handler
        Y_VERIFY_S(signal != SIGCHLD, "Trace collector doesn't support signal SIGCHLD");
        Y_VERIFY_S(signal < NSIG, "Signal number is too big");

        SetSignalHandler(signal, [&](int sig, siginfo_t*, void*) {
            // TODO: it's a dubious place for log - make sure that in case of heap corruption we don't make things worse,
            // LOG("Received signal " << sig);

            static_assert(PIPE_BUF >= 512);
            static_assert(sizeof(TStackTrace) <= PIPE_BUF, "Only write to pipe the chunk of size PIPE_BUF is atomic");

            TStackTrace stackTrace;
            size_t written = Connection->Write(&stackTrace, sizeof(stackTrace));
            Y_VERIFY_S(sizeof(stackTrace) == written, "Write to pipe is not atomic!");

            waitpid(CollectorPid, nullptr, 0);
            CollectorPid = -1;

            RestoreSignalHandlers();
            raise(sig);
        }, &OldActions[signal]);
    }

    // Special handler to log the shutdown of the trace collector process
    SetSignalHandler(SIGCHLD, [&](int sig, siginfo_t* info, void* ucontext) {
        if (info->si_pid == CollectorPid) {
            switch(info->si_code) {
                case CLD_EXITED:
                    if (info->si_status == 0) {
                        // LOG("The trace collector has finished work normally");
                    } else {
                        // LOG("The trace collector has finished work with exit_code=" << info->si_status);
                    }
                    break;
                case CLD_KILLED:
                    // LOG("The trace collector was killed by signal=" << info->si_status);
                    break;
                case CLD_DUMPED:
                    // LOG("The trace collector terminated abnormally by signal=" << info->si_status);
                    break;
                default: [[unlikely]]
                    // LOG("Unexpected si_code: " << info->si_code);
                    ;
            }

            RestoreSignalHandlers();
        } else {
            const auto& oldHandler = OldActions[sig].sa_handler;
            if (oldHandler == SIG_DFL || oldHandler == SIG_IGN) {
                // TODO(ilezhankin): we should raise signal again in case of SIG_DFL
                return;
            } else {
                // Call previous signal handler
                OldActions[sig].sa_sigaction(sig, info, ucontext);
            }
        }
    },
    &OldActions[SIGCHLD]);
}

void TTraceCollector::RestoreSignalHandlers() {
    for (auto sig: HandledSignals) {
        sigaction(sig, &OldActions[sig], nullptr);
    }
}

void TTraceCollector::RunChildMain() {
    Cerr << "The trace collector is running" << Endl;

    Connection->CloseWrite();
    try {
        TStackTrace trace;
        size_t read = Connection->Read(&trace, sizeof(trace));

        if (read != 0) {
            Y_VERIFY_S(sizeof(TStackTrace) == read, "Read from pipe is not atomic");
            Cerr << "Backtrace:\n" << Symbolize(trace) << Endl;
        }
    } catch (const std::exception& error) {
        Y_FAIL_S("Error while the trace collector is running: " << error.what());
    }

    Connection->CloseRead();
    std::exit(0);
}

TString TTraceCollector::Symbolize(const TStackTrace& stackTrace) const {
    TStringStream trace;

    auto error = NDwarf::ResolveBacktrace({stackTrace.Get() + 1, stackTrace.GetSize() - 1}, [&trace](const NDwarf::TLineInfo& info) {
        trace << "#" << info.Index << " " << info.FunctionName << " at " << info.FileName << ':' << info.Line << ':' << info.Col << '\n';
        return NDwarf::EResolving::Continue;
    });

    if (error) {
        trace << "Failed to resolve backtrace\n";
    } else {
        trace << '\n';
    }

    return trace.Str();
}

void NKikimr::WaitSignals() {
    sigset_t signal_mask;
    sigemptyset(&signal_mask);
    sigaddset(&signal_mask, SIGTERM);
    sigaddset(&signal_mask, SIGINT);

    int sig;
    sigwait(&signal_mask, &sig);

    // TODO: DISTBUILD_LOG(FATAL, "Received " << sig << " signal: " << strsignal(sig));
}
