#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/backtrace_introspector/introspect.h>

#include <yt/yt/library/signals/signal_blocking.h>

#include <signal.h>

#ifdef _linux_
    #include <sys/epoll.h>
    #include <sys/signalfd.h>
#endif // _linux_

namespace NYT::NSignals {

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_

YT_TRY_BLOCK_SIGNAL_FOR_PROCESS(SIGRTMIN, [] (bool ok, int threadCount) {
    if (!ok) {
        NLogging::TLogger Logger("SignalBlocking");
        YT_LOG_WARNING("Thread count is not 1, trying to get thread infos (ThreadCount: %v)", threadCount);
        auto threadInfos = NYT::NBacktraceIntrospector::IntrospectThreads();
        auto descripion = NYT::NBacktraceIntrospector::FormatIntrospectionInfos(threadInfos);
        AbortProcessDramatically(
            EProcessExitCode::GenericError,
            Format(
                "Thread count is not 1, threadCount: %v, threadInfos: %v",
                threadCount,
                descripion));
    }
});

class TSignalBlockingTest
    : public ::testing::Test
{
public:
    int OpenSignalFD()
    {
        sigset_t mask;
        if (::sigemptyset(&mask) == -1) {
            Cerr << "sigemptyset failed" << Endl;
            ::exit(-1);
        }

        if (::sigaddset(&mask, SIGRTMIN) == -1) {
            Cerr << "sigaddset failed" << Endl;
            ::exit(-1);
        }

        int sigFD = ::signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
        if (sigFD == -1) {
            Cerr << "signalfd failed" << Endl;
            ::exit(-1);
        }

        return sigFD;
    }

    int CreateEpollAndRegisterSigFD(int sigFD)
    {
        auto epollFd = ::epoll_create1(0);
        if (epollFd == -1) {
            err(EXIT_FAILURE, "epoll_create1");
        }

        ::epoll_event ev;
        memset((void*)&ev, 0, sizeof(ev));

        ev.events = EPOLLIN;
        ev.data.fd = sigFD;

        if (::epoll_ctl(epollFd, EPOLL_CTL_ADD, sigFD, &ev) == -1) {
            err(EXIT_FAILURE, "epoll_ctl");
        }

        return epollFd;
    }
};

TEST_F(TSignalBlockingTest, SignalBlockingJustWorks)
{
    ::raise(SIGRTMIN);
}

#ifdef _linux_

TEST_F(TSignalBlockingTest, TestSignalFD)
{
    auto sigFD = OpenSignalFD();

    auto epollFd = CreateEpollAndRegisterSigFD(sigFD);

    ::raise(SIGRTMIN);

    ::epoll_event events[10];

    auto nfds = ::epoll_wait(epollFd, events, 10, 5000);
    if (nfds == -1) {
        Cerr << "epoll_wait failed" << Endl;
        ::exit(-1);
    }

    bool gotSignal = false;
    for (int i = 0; i < nfds; ++i) {
        if (events[i].data.fd == sigFD) {
            gotSignal = true;
        }
    }

    EXPECT_TRUE(gotSignal);

    ::signalfd_siginfo fdsi;
    ssize_t s = ::read(sigFD, &fdsi, sizeof(fdsi));
    if (s <= 0) {
        Cerr << "sigfd is empty" << Endl;
        ::exit(-1);
    }
    if (s != sizeof(fdsi)) {
        Cerr << "read from sigfd failed" << Endl;
    }

    EXPECT_TRUE(fdsi.ssi_signo == static_cast<uint32_t>(SIGRTMIN));
}

#endif // _linux_

#endif // _win_

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignals
