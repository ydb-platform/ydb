#include "poller_tcp_unit_epoll.h"
#if !defined(_win_) && !defined(_darwin_)
#include <unistd.h>
#include <sys/epoll.h>

#include <csignal>
#include <cerrno>

namespace NInterconnect {
    namespace {
        void
        DeleteEpoll(int epoll, SOCKET stream) {
            ::epoll_event event = {0, {.fd = stream}};
            if (::epoll_ctl(epoll, EPOLL_CTL_DEL, stream, &event)) {
                Cerr << "epoll_ctl errno: " << errno << Endl;
                Y_ABORT("epoll delete error!");
            }
        }

        template <ui32 Events>
        void
        AddEpoll(int epoll, SOCKET stream) {
            ::epoll_event event = {.events = Events};
            event.data.fd = stream;
            if (::epoll_ctl(epoll, EPOLL_CTL_ADD, stream, &event)) {
                Cerr << "epoll_ctl errno: " << errno << Endl;
                Y_ABORT("epoll add error!");
            }
        }

        int
        Initialize() {
            const auto epoll = ::epoll_create(10000);
            Y_DEBUG_ABORT_UNLESS(epoll > 0);
            return epoll;
        }

    }

    TPollerUnitEpoll::TPollerUnitEpoll()
        : ReadDescriptor(Initialize())
        , WriteDescriptor(Initialize())
    {
        // Block on the epoll descriptor.
        ::sigemptyset(&sigmask);
        ::sigaddset(&sigmask, SIGPIPE);
        ::sigaddset(&sigmask, SIGTERM);
    }

    TPollerUnitEpoll::~TPollerUnitEpoll() {
        ::close(ReadDescriptor);
        ::close(WriteDescriptor);
    }

    template <>
    int TPollerUnitEpoll::GetDescriptor<false>() const {
        return ReadDescriptor;
    }

    template <>
    int TPollerUnitEpoll::GetDescriptor<true>() const {
        return WriteDescriptor;
    }

    void
    TPollerUnitEpoll::StartReadOperation(
        const TIntrusivePtr<TSharedDescriptor>& s,
        TFDDelegate&& operation) {
        TPollerUnit::StartReadOperation(s, std::move(operation));
        AddEpoll<EPOLLRDHUP | EPOLLIN>(ReadDescriptor, s->GetDescriptor());
    }

    void
    TPollerUnitEpoll::StartWriteOperation(
        const TIntrusivePtr<TSharedDescriptor>& s,
        TFDDelegate&& operation) {
        TPollerUnit::StartWriteOperation(s, std::move(operation));
        AddEpoll<EPOLLRDHUP | EPOLLOUT>(WriteDescriptor, s->GetDescriptor());
    }

    constexpr int EVENTS_BUF_SIZE = 128;

    template <bool WriteOp>
    void
    TPollerUnitEpoll::Process() {
        ::epoll_event events[EVENTS_BUF_SIZE];

        const int epoll = GetDescriptor<WriteOp>();

        /* Timeout just to check StopFlag sometimes */
        const int result =
            ::epoll_pwait(epoll, events, EVENTS_BUF_SIZE, 200, &sigmask);

        if (result == -1 && errno != EINTR)
            Y_ABORT("epoll wait error!");

        auto& side = GetSide<WriteOp>();
        side.ProcessInput();

        for (int i = 0; i < result; ++i) {
            const auto it = side.Operations.find(events[i].data.fd);
            if (side.Operations.end() == it)
                continue;
            if (const auto& finalizer = it->second.second(it->second.first)) {
                DeleteEpoll(epoll, it->first);
                side.Operations.erase(it);
                finalizer();
            }
        }
    }

    void
    TPollerUnitEpoll::ProcessRead() {
        Process<false>();
    }

    void
    TPollerUnitEpoll::ProcessWrite() {
        Process<true>();
    }

}

#endif
