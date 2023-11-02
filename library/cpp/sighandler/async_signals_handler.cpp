#include "async_signals_handler.h"

#include <util/system/platform.h>

#if !defined(_win_)

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>

#include <unistd.h>

#if defined(_linux_)
#include <dlfcn.h>
#endif

#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/defaults.h>
#include <util/system/event.h>
#include <util/system/rwlock.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>
#include <util/system/yassert.h>
#include <util/generic/hash.h>

namespace {
    volatile int SIGNAL_PIPE_WRITE_FD = 0; // will be initialized in ctor

    void WriteAllOrDie(const int fd, const void* buf, size_t bufsize) {
        size_t totalBytesWritten = 0;

        while (totalBytesWritten != bufsize) {
            const ssize_t result = write(fd, (const char*)buf + totalBytesWritten, bufsize - totalBytesWritten);

            Y_ABORT_UNLESS(result >= 0 || (result == -1 && errno == EINTR), "write failed: %s (errno = %d)", strerror(errno), errno);
            totalBytesWritten += static_cast<size_t>(result);
        }
    }

    void PipeWriterSignalHandler(int, siginfo_t* info, void*) {
        const ui8 signum = static_cast<ui8>(info->si_signo);

        WriteAllOrDie(SIGNAL_PIPE_WRITE_FD, &signum, 1);
    }

    // Handler for the "asynchronous" unix signals (those which can occur
    // at arbitrary point of execution and have no need to be reacted on instantly
    // and/or to preserve execution context at the point of interrupt).
    //
    // Async signals -- SIGHUP, SIGUSR1 (used to cause configuration files reread for example)
    // Sync signals -- fatal errors like SIGSEGV, SIGBUS...
    class TAsyncSignalsHandler {
    private:
        TThread Thread;
        int SignalPipeReadFd;
        typedef THolder<TEventHandler> TEventHandlerPtr;
        THashMap<int, TEventHandlerPtr> Handlers;
        TRWMutex HandlersLock;

        TAtomic ShouldDie;
        TSystemEvent DieEvent;

        static void* ThreadFunc(void* data) {
            reinterpret_cast<TAsyncSignalsHandler*>(data)->RealThreadFunc();

            return nullptr;
        }

        inline void RealThreadFunc() {
            for (;;) {
                ui8 signum;
                const ssize_t bytesRead = read(SignalPipeReadFd, &signum, 1);

                Y_ABORT_UNLESS(bytesRead >= 0 || (bytesRead == -1 && errno == EINTR), "read failed: %s (errno = %d)", strerror(errno), errno);

                if (AtomicAdd(ShouldDie, 0) != 0) {
                    DieEvent.Signal();

                    break;
                }

                if (bytesRead == 0) {
                    break;
                } else if (bytesRead == -1) {
                    continue;
                }

                {
                    TReadGuard dnd(HandlersLock);

                    const TEventHandlerPtr* handler = Handlers.FindPtr(signum);
                    Y_ABORT_UNLESS(handler && handler->Get(), "Async signal handler is not set, it's a bug!");
                    handler->Get()->Handle(signum);
                }
            }
        }

    public:
        TAsyncSignalsHandler()
            : Thread(TThread::TParams(ThreadFunc, this).SetName("sighandler"))
            , SignalPipeReadFd(0)
            , ShouldDie(0)
        {
            int filedes[2] = {-1};

#ifdef _linux_
            int result;

            {
                using pipe2_t = decltype(pipe2);
                pipe2_t* pipe2Ptr = (pipe2_t*)dlsym(RTLD_DEFAULT, "pipe2");

#if defined(_musl_)
                if (!pipe2Ptr) {
                    pipe2Ptr = pipe2;
                }
#endif

                if (pipe2Ptr) {
                    result = pipe2Ptr(filedes, O_CLOEXEC);
                } else {
                    result = -1;
                    errno = ENOSYS;
                }
            }

            if (result != 0 && errno == ENOSYS) { // linux older than 2.6.27 returns "not implemented"
#endif
                Y_ABORT_UNLESS(pipe(filedes) == 0, "pipe failed: %s (errno = %d)", strerror(errno), errno);

                SignalPipeReadFd = filedes[0];
                SIGNAL_PIPE_WRITE_FD = filedes[1];

                Y_ABORT_UNLESS(fcntl(SignalPipeReadFd, F_SETFD, FD_CLOEXEC) == 0, "fcntl failed: %s (errno = %d)", strerror(errno), errno);
                Y_ABORT_UNLESS(fcntl(SIGNAL_PIPE_WRITE_FD, F_SETFD, FD_CLOEXEC) == 0, "fcntl failed: %s (errno = %d)", strerror(errno), errno);
#ifdef _linux_
            } else {
                Y_ABORT_UNLESS(result == 0, "pipe2 failed: %s (errno = %d)", strerror(errno), errno);
                SignalPipeReadFd = filedes[0];
                SIGNAL_PIPE_WRITE_FD = filedes[1];
            }
#endif

            Thread.Start();
            Thread.Detach();
        }

        ~TAsyncSignalsHandler() {
            AtomicSwap(&ShouldDie, TAtomic(1));
            ui8 fakeSignal = 0;
            WriteAllOrDie(SIGNAL_PIPE_WRITE_FD, &fakeSignal, 1);

            DieEvent.WaitT(TDuration::Seconds(15));

            /* may cause VERIFY failure in signal handler, propably we should leave it to process clean procedure
        close(SIGNAL_PIPE_WRITE_FD);
        close(SignalPipeReadFd);
*/
        }

        bool DoInstall(int signum, THolder<TEventHandler> handler) {
            TWriteGuard dnd(HandlersLock);
            TEventHandlerPtr& ev = Handlers[signum];
            const bool ret = !ev;

            ev = std::move(handler);

            return ret;
        }

        void Install(int signum, THolder<TEventHandler> handler) {
            if (DoInstall(signum, std::move(handler))) {
                struct sigaction a;

                memset(&a, 0, sizeof(a));
                a.sa_sigaction = PipeWriterSignalHandler;
                a.sa_flags = SA_SIGINFO | SA_RESTART;

                Y_ABORT_UNLESS(!sigaction(signum, &a, nullptr), "sigaction failed: %s (errno = %d)", strerror(errno), errno);
            }
        }
    };

    // This pointer is never deleted - yeah, it's intended memory leak.
    // It is necessary to prevent problems when user's signal handler calls exit function
    // which destroys all global variables including this one.
    // It such situation we have 2 options:
    //  - wait for auxiliary thread to die - which will cause dead lock
    //  - destruct variable, ignoring thread - which will cause data corruption.
    std::atomic<TAsyncSignalsHandler*> SIGNALS_HANDLER = nullptr;
}

void SetAsyncSignalHandler(int signum, THolder<TEventHandler> handler) {
    static TAdaptiveLock lock;

    // Must be in HB with Handler's constructor.
    auto* currentHandler = SIGNALS_HANDLER.load(std::memory_order::acquire);

    if (Y_UNLIKELY(currentHandler == nullptr)) {
        TGuard dnd(lock);

        // If we read non-null here it means that we have a concurrent thread
        // unlocking the lock establishing strongly HB with us.
        // next line is sequenced before lock call thus relaxed is enough here.
        currentHandler = SIGNALS_HANDLER.load(std::memory_order::relaxed);

        if (currentHandler == nullptr) {
            // NEVERS GETS DESTROYED
            currentHandler = new TAsyncSignalsHandler();

            // Ensure HB with constructor for future readers.
            SIGNALS_HANDLER.store(currentHandler, std::memory_order::release);
        }
    }

    currentHandler->Install(signum, std::move(handler));
}

#else //_win_

void SetAsyncSignalHandler(int, THolder<TEventHandler>) {
    // TODO: it's really easy to port using _pipe, _read and _write, but it must be tested properly.
}

#endif

namespace {
    template <typename TFunc>
    class TFunctionEventHandler: public TEventHandler {
        TFunc Func;

    public:
        TFunctionEventHandler(TFunc func) {
            if (func)
                Func = func;
        }

        int Handle(int signum) override {
            if (Func) {
                Func(signum);
            }

            return 0;
        }
    };
}

void SetAsyncSignalHandler(int signum, void (*handler)(int)) {
    SetAsyncSignalHandler(signum, MakeHolder<TFunctionEventHandler<void (*)(int)>>(handler));
}

void SetAsyncSignalFunction(int signum, std::function<void(int)> func) {
    typedef std::function<void(int)> TFunc;
    SetAsyncSignalHandler(signum, MakeHolder<TFunctionEventHandler<TFunc>>(func));
}
