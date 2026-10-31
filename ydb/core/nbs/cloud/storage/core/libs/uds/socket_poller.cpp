#include "socket_poller.h"

#include <util/datetime/base.h>

#include <errno.h>

#if defined(_linux_) && !defined(_bionic_)
#define HAVE_EPOLL_POLLER
#endif

#if defined(HAVE_EPOLL_POLLER)
#include <sys/epoll.h>
#endif

namespace NYdb::NBS::NServer {

////////////////////////////////////////////////////////////////////////////////

#if defined(HAVE_EPOLL_POLLER)

class TSocketPoller::TImpl
{
private:
    static constexpr TDuration PollInterval = TDuration::Minutes(35);
    const int Fd;

public:
    TImpl()
        : Fd(epoll_create1(0))
    {
        if (Fd == -1) {
            ythrow TSystemError() << "epoll_create failed";
        }
    }

    ~TImpl()
    {
        close(Fd);
    }

    void WaitRead(SOCKET sock, void* cookie)
    {
        struct epoll_event ev;
        Zero(ev);
        ev.events = EPOLLIN;
        ev.data.ptr = cookie;

        auto res = epoll_ctl(Fd, EPOLL_CTL_ADD, sock, &ev);
        if (res == -1) {
            ythrow TSystemError() << "epoll add failed";
        }
    }

    void WaitClose(SOCKET sock, void* cookie)
    {
        struct epoll_event ev;
        Zero(ev);
        ev.data.ptr = cookie;

        auto res = epoll_ctl(Fd, EPOLL_CTL_ADD, sock, &ev);
        if (res == -1) {
            ythrow TSystemError() << "epoll add failed";
        }
    }

    void Unwait(SOCKET sock)
    {
        struct epoll_event ev;
        Zero(ev);

        epoll_ctl(Fd, EPOLL_CTL_DEL, sock, &ev);
        // if epoll_ctl returns a error it means the sock is already unwaited
    }

    size_t Wait(void** events, size_t len)
    {
        TTempArray<struct epoll_event> tmpEvents(len);
        return DoWait(events, tmpEvents.Data(), len);
    }

private:
    size_t DoWait(void** ev, struct epoll_event* events, size_t len)
    {
        const size_t ret = WaitImpl(events, len);

        for (size_t i = 0; i < ret; ++i) {
            ev[i] = events[i].data.ptr;
        }

        return ret;
    }

    size_t WaitImpl(struct epoll_event* events, size_t len)
    {
        if (!len) {
            return 0;
        }

        int ret;

        do {
            ret = epoll_wait(Fd, events, len, PollInterval.MicroSeconds());
        } while (ret == -1 && errno == EINTR);

        Y_ABORT_UNLESS(ret >= 0, "epoll wait error: %s", LastSystemErrorText());
        return (size_t)ret;
    }
};

#else

class TSocketPoller::TImpl
{
public:
    void WaitRead(SOCKET, void*)
    {
        Y_ABORT("Not implemented");
    }

    void WaitClose(SOCKET, void*)
    {
        Y_ABORT("Not implemented");
    }

    void Unwait(SOCKET)
    {
        Y_ABORT("Not implemented");
    }

    size_t Wait(void**, size_t)
    {
        Y_ABORT("Not implemented");
    }
};

#endif

////////////////////////////////////////////////////////////////////////////////

TSocketPoller::TSocketPoller()
    : Impl(new TImpl())
{}

TSocketPoller::~TSocketPoller() = default;

void TSocketPoller::WaitRead(SOCKET sock, void* cookie)
{
    Impl->WaitRead(sock, cookie);
}

void TSocketPoller::WaitClose(SOCKET sock, void* cookie)
{
    Impl->WaitClose(sock, cookie);
}

void TSocketPoller::Unwait(SOCKET sock)
{
    Impl->Unwait(sock);
}

size_t TSocketPoller::Wait(void** events, size_t len)
{
    return Impl->Wait(events, len);
}

}   // namespace NYdb::NBS::NServer
