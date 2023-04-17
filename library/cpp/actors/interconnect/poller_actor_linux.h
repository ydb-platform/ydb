#pragma once

#include <sys/epoll.h>

namespace NActors {

    class TEpollThread : public TPollerThreadBase<TEpollThread> {
        // epoll file descriptor
        int EpollDescriptor;

    public:
        TEpollThread(TActorSystem *actorSystem)
            : TPollerThreadBase(actorSystem)
        {
            EpollDescriptor = epoll_create1(EPOLL_CLOEXEC);
            Y_VERIFY(EpollDescriptor != -1, "epoll_create1() failed with %s", strerror(errno));

            epoll_event event;
            event.data.ptr = nullptr;
            event.events = EPOLLIN;
            if (epoll_ctl(EpollDescriptor, EPOLL_CTL_ADD, ReadEnd, &event) == -1) {
                Y_FAIL("epoll_ctl(EPOLL_CTL_ADD) failed with %s", strerror(errno));
            }

            ISimpleThread::Start(); // start poller thread
        }

        ~TEpollThread() {
            Stop();
            close(EpollDescriptor);
        }

        bool ProcessEventsInLoop() {
            // preallocated array for events
            std::array<epoll_event, 256> events;

            // wait indefinitely for event to arrive
            LWPROBE(EpollStartWaitIn);
            int numReady = epoll_wait(EpollDescriptor, events.data(), events.size(), -1);
            LWPROBE(EpollFinishWaitIn, numReady);

            // check return status for any errors
            if (numReady == -1) {
                if (errno == EINTR) {
                    return false; // restart the call a bit later
                } else {
                    Y_FAIL("epoll_wait() failed with %s", strerror(errno));
                }
            }

            bool res = false;

            for (int i = 0; i < numReady; ++i) {
                const epoll_event& ev = events[i];
                if (auto *record = static_cast<TSocketRecord*>(ev.data.ptr)) {
                    const bool read = ev.events & (EPOLLIN | EPOLLHUP | EPOLLRDHUP | EPOLLERR);
                    const bool write = ev.events & (EPOLLOUT | EPOLLERR);

                    // remove hit flags from the bit set
                    ui32 flags = record->Flags;
                    const ui32 remove = (read ? EPOLLIN : 0) | (write ? EPOLLOUT : 0);
                    while (!record->Flags.compare_exchange_weak(flags, flags & ~remove))
                    {}
                    flags &= ~remove;

                    // rearm poller if some flags remain
                    if (flags) {
                        epoll_event event;
                        event.events = EPOLLONESHOT | EPOLLRDHUP | flags;
                        event.data.ptr = record;
                        if (epoll_ctl(EpollDescriptor, EPOLL_CTL_MOD, record->Socket->GetDescriptor(), &event) == -1) {
                            Y_FAIL("epoll_ctl(EPOLL_CTL_MOD) failed with %s", strerror(errno));
                        }
                    }

                    // issue notifications
                    Notify(record, read, write);
                } else {
                    res = true;
                }
            }

            return res;
        }

        void UnregisterSocketInLoop(const TIntrusivePtr<TSharedDescriptor>& socket) {
            if (epoll_ctl(EpollDescriptor, EPOLL_CTL_DEL, socket->GetDescriptor(), nullptr) == -1) {
                Y_FAIL("epoll_ctl(EPOLL_CTL_DEL) failed with %s", strerror(errno));
            }
        }

        void RegisterSocket(const TIntrusivePtr<TSocketRecord>& record) {
            epoll_event event;
            event.events = EPOLLONESHOT | EPOLLRDHUP;
            event.data.ptr = record.Get();
            if (epoll_ctl(EpollDescriptor, EPOLL_CTL_ADD, record->Socket->GetDescriptor(), &event) == -1) {
                Y_FAIL("epoll_ctl(EPOLL_CTL_ADD) failed with %s", strerror(errno));
            }
        }

        void Request(const TIntrusivePtr<TSocketRecord>& record, bool read, bool write) {
            const ui32 add = (read ? EPOLLIN : 0) | (write ? EPOLLOUT : 0);
            ui32 flags = record->Flags;
            while (!record->Flags.compare_exchange_weak(flags, flags | add))
            {}
            flags |= add;
            if (flags) {
                epoll_event event;
                event.events = EPOLLONESHOT | EPOLLRDHUP | flags;
                event.data.ptr = record.Get();
                if (epoll_ctl(EpollDescriptor, EPOLL_CTL_MOD, record->Socket->GetDescriptor(), &event) == -1) {
                    Y_FAIL("epoll_ctl(EPOLL_CTL_MOD) failed with %s", strerror(errno));
                }
            }
        }
    };

    using TPollerThread = TEpollThread;

} // namespace NActors
