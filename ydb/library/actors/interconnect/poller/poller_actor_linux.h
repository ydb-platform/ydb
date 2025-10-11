#pragma once

#include <sys/epoll.h>

namespace NActors {

    enum {
        ReadExpected = 1,
        ReadHit = 2,
        WriteExpected = 4,
        WriteHit = 8,
    };

    class TEpollThread : public TPollerThreadBase<TEpollThread> {
        // epoll file descriptor
        int EpollDescriptor;

    public:
        TEpollThread(TActorSystem *actorSystem)
            : TPollerThreadBase(actorSystem)
        {
            EpollDescriptor = epoll_create1(EPOLL_CLOEXEC);
            Y_ABORT_UNLESS(EpollDescriptor != -1, "epoll_create1() failed with %s", strerror(errno));

            epoll_event event;
            event.data.ptr = nullptr;
            event.events = EPOLLIN;
            if (epoll_ctl(EpollDescriptor, EPOLL_CTL_ADD, ReadEnd, &event) == -1) {
                Y_ABORT("epoll_ctl(EPOLL_CTL_ADD) failed with %s", strerror(errno));
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
                    Y_ABORT("epoll_wait() failed with %s", strerror(errno));
                }
            }

            bool res = false;

            for (int i = 0; i < numReady; ++i) {
                const epoll_event& ev = events[i];
                if (auto *record = static_cast<TSocketRecord*>(ev.data.ptr)) {
                    const bool read = ev.events & (EPOLLIN | EPOLLHUP | EPOLLRDHUP | EPOLLERR);
                    const bool write = ev.events & (EPOLLOUT | EPOLLERR);
                    UpdateFlags(record, (read ? ReadHit : 0) | (write ? WriteHit : 0), false /*suppressNotify*/,
                        false /*checkQueues*/);
                } else {
                    res = true;
                }
            }

            return res;
        }

        bool UpdateFlags(TSocketRecord *record, ui32 addMask, bool suppressNotify, bool checkQueues) {
            ui32 flags = record->Flags.load(std::memory_order_acquire);
            for (;;) {
                ui32 updated = flags | addMask;
                static constexpr ui32 fullRead = ReadExpected | ReadHit;
                static constexpr ui32 fullWrite = WriteExpected | WriteHit;
                bool read = (updated & fullRead) == fullRead;
                bool write = (updated & fullWrite) == fullWrite;
                if (checkQueues) {
                    const bool queryRead = updated & ReadExpected && !read;
                    const bool queryWrite = updated & WriteExpected && !write;
                    if (queryRead || queryWrite) {
                        pollfd fd;
                        fd.fd = record->Socket->GetDescriptor();
                        fd.events = (queryRead ? POLLIN | POLLRDHUP : 0) | (queryWrite ? POLLOUT : 0);
                        if (poll(&fd, 1, 0) != -1) {
                            read |= queryRead && fd.revents & (POLLIN | POLLHUP | POLLRDHUP | POLLERR);
                            write |= queryWrite && fd.revents & (POLLOUT | POLLERR);
                        }
                    }
                }
                updated &= ~((read ? fullRead : 0) | (write ? fullWrite : 0));
                if (record->Flags.compare_exchange_weak(flags, updated, std::memory_order_acq_rel)) {
                    if (suppressNotify) {
                        return read || write;
                    } else {
                        Notify(record, read, write);
                        return false;
                    }
                }
            }
        }

        void UnregisterSocketInLoop(const TIntrusivePtr<TSharedDescriptor>& socket) {
            if (epoll_ctl(EpollDescriptor, EPOLL_CTL_DEL, socket->GetDescriptor(), nullptr) == -1) {
                Y_ABORT("epoll_ctl(EPOLL_CTL_DEL) failed with %s", strerror(errno));
            }
        }

        void RegisterSocket(const TIntrusivePtr<TSocketRecord>& record) {
            epoll_event event;
            event.events = EPOLLET | EPOLLRDHUP | EPOLLIN | EPOLLOUT;
            event.data.ptr = record.Get();
            if (epoll_ctl(EpollDescriptor, EPOLL_CTL_ADD, record->Socket->GetDescriptor(), &event) == -1) {
                Y_ABORT("epoll_ctl(EPOLL_CTL_ADD) failed with %s", strerror(errno));
            }
        }

        bool Request(const TIntrusivePtr<TSocketRecord>& record, bool read, bool write, bool suppressNotify,
                bool afterWouldBlock) {
            return UpdateFlags(record.Get(), (read ? ReadExpected : 0) | (write ? WriteExpected : 0), suppressNotify,
                !afterWouldBlock);
        }
    };

    using TPollerThread = TEpollThread;

} // namespace NActors
