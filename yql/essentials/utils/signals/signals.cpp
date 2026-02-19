#include "signals.h"
#include "utils.h"

#include <yql/essentials/utils/backtrace/backtrace.h>

#include <util/stream/output.h>
#include <util/generic/yexception.h>
#include <util/datetime/base.h>
#include <util/network/socket.h>
#include <util/system/getpid.h>

#ifdef _linux_
    #include <sys/prctl.h>
#endif

#include <cstring>
#include <csignal>
#include <cerrno>
#include <cstdlib>

namespace NYql {

volatile sig_atomic_t NeedTerminate = 0;
volatile sig_atomic_t NeedQuit = 0;
volatile sig_atomic_t NeedReconfigure = 0;
volatile sig_atomic_t NeedReopenLog = 0;
volatile sig_atomic_t NeedReapZombies = 0;
volatile sig_atomic_t NeedInterrupt = 0;

volatile sig_atomic_t CatchInterrupt = 0;

TPipe SignalPipeW;
TPipe SignalPipeR;

namespace {

void SignalHandler(int signo)
{
    switch (signo) {
        case SIGTERM:
            NeedTerminate = 1;
            break;

        case SIGQUIT:
            NeedQuit = 1;
            break;

#ifdef _unix_
        case SIGHUP:
            NeedReconfigure = 1;
            break;

        case SIGUSR1:
            NeedReopenLog = 1;
            break;

        case SIGCHLD:
            NeedReapZombies = 1;
            break;
#endif

        case SIGINT:
            if (CatchInterrupt) {
                NeedInterrupt = 1;
            } else {
                fprintf(stderr, "%s (pid=%d) captured SIGINT\n",
                        GetProcTitle(), getpid());
                signal(signo, SIG_DFL);
                raise(signo);
            }
            break;

        default:
            break;
    }
}

void SignalHandlerWithSelfPipe(int signo)
{
    SignalHandler(signo);

    int savedErrno = errno;
    if (write(SignalPipeW.GetHandle(), "x", 1) == -1 && errno != EAGAIN) {
        static TStringBuf Msg("cannot write to signal pipe");
#ifndef STDERR_FILENO
    #define STDERR_FILENO 2
#endif
        write(STDERR_FILENO, Msg.data(), Msg.size());
        abort();
    }
    errno = savedErrno;
}

#ifndef _unix_
const char* strsignal(int signo)
{
    switch (signo) {
        case SIGTERM:
            return "SIGTERM";
        case SIGINT:
            return "SIGINT";
        case SIGQUIT:
            return "SIGQUIT";
        default:
            return "UNKNOWN";
    }
}
#endif

#ifdef _unix_
int SetSignalHandler(int signo, void (*handler)(int))
{
    struct sigaction sa;
    sa.sa_flags = SA_RESTART;
    sa.sa_handler = handler;
    sigemptyset(&sa.sa_mask);

    return sigaction(signo, &sa, nullptr);
}

#else
int SetSignalHandler(int signo, void (*handler)(int))
{
    return (signal(signo, handler) == SIG_ERR) ? -1 : 0;
}

#endif

struct TSignalHandlerDesc {
    int Signo;
    void (*Handler)(int);
};

void SetSignalHandlers(const TSignalHandlerDesc* handlerDescs)
{
    sigset_t interestedSignals;
    SigEmptySet(&interestedSignals);

    for (int i = 0; handlerDescs[i].Signo != -1; i++) {
        int signo = handlerDescs[i].Signo;
        SigAddSet(&interestedSignals, signo);

        if (SetSignalHandler(signo, handlerDescs[i].Handler) == -1) {
            ythrow TSystemError() << "Cannot set handler for signal "
                                  << strsignal(signo);
        }
    }

    if (SigProcMask(SIG_BLOCK, &interestedSignals, NULL) == -1) {
        ythrow TSystemError() << "Cannot set sigprocmask";
    }

    NYql::NBacktrace::RegisterKikimrFatalActions();
}

} // namespace

void InitSignals()
{
    TSignalHandlerDesc handlerDescs[] = {
        {SIGTERM, SignalHandler},
        {SIGINT, SignalHandler},
        {SIGQUIT, SignalHandler},
#ifdef _unix_
        {SIGPIPE, SIG_IGN},
        {SIGHUP, SignalHandler},
        {SIGUSR1, SignalHandler},
        {SIGCHLD, SignalHandler},
#endif
        {-1, nullptr}};

    SetSignalHandlers(handlerDescs);
}

void InitSignalsWithSelfPipe()
{
    TSignalHandlerDesc handlerDescs[] = {
        {SIGTERM, SignalHandlerWithSelfPipe},
        {SIGINT, SignalHandlerWithSelfPipe},
        {SIGQUIT, SignalHandlerWithSelfPipe},
#ifdef _unix_
        {SIGPIPE, SIG_IGN},
        {SIGHUP, SignalHandlerWithSelfPipe},
        {SIGUSR1, SignalHandlerWithSelfPipe},
        {SIGCHLD, SignalHandlerWithSelfPipe},
#endif
        {-1, nullptr}};

    TPipe::Pipe(SignalPipeR, SignalPipeW);
    SetNonBlock(SignalPipeR.GetHandle());
    SetNonBlock(SignalPipeW.GetHandle());

    SetSignalHandlers(handlerDescs);
}

void CatchInterruptSignal(bool doCatch) {
    CatchInterrupt = doCatch;
}

void SigSuspend(const sigset_t* mask)
{
#ifdef _unix_
    sigsuspend(mask);
#else
    Y_UNUSED(mask);
    Sleep(TDuration::Seconds(1));
#endif
}

void AllowAnySignals()
{
    sigset_t blockMask;
    SigEmptySet(&blockMask);

    if (SigProcMask(SIG_SETMASK, &blockMask, NULL) == -1) {
        ythrow TSystemError() << "Cannot set sigprocmask";
    }
}

bool HasPendingQuitOrTerm() {
#ifdef _unix_
    sigset_t signals;
    SigEmptySet(&signals);
    if (sigpending(&signals)) {
        ythrow TSystemError() << "Error in sigpending";
    }

    return (SigIsMember(&signals, SIGQUIT) == 1) || (SigIsMember(&signals, SIGTERM) == 1);
#else
    return false;
#endif
}
} // namespace NYql
