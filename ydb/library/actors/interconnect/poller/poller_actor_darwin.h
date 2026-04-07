#pragma once

#include <sys/event.h>

namespace NActors {

    class TKqueueThread : public TPollerThreadBase<TKqueueThread> {
        // KQueue file descriptor
        int KqDescriptor;

        void SafeKevent(const struct kevent* ev, int size) {
            int rc;
            do {
                rc = kevent(KqDescriptor, ev, size, nullptr, 0, nullptr);
            } while (rc == -1 && errno == EINTR);
            Y_ABORT_UNLESS(rc != -1, "kevent() failed with %s", strerror(errno));
        }

    public:
        TKqueueThread(TActorSystem *actorSystem)
            : TPollerThreadBase(actorSystem)
        {
            // create kqueue
            KqDescriptor = kqueue();
            Y_ABORT_UNLESS(KqDescriptor != -1, "kqueue() failed with %s", strerror(errno));

            // set close-on-exit flag
            {
                int flags = fcntl(KqDescriptor, F_GETFD);
                Y_ABORT_UNLESS(flags >= 0, "fcntl(F_GETFD) failed with %s", strerror(errno));
                int rc = fcntl(KqDescriptor, F_SETFD, flags | FD_CLOEXEC);
                Y_ABORT_UNLESS(rc != -1, "fcntl(F_SETFD, +FD_CLOEXEC) failed with %s", strerror(errno));
            }

            // register pipe's read end in poller
            struct kevent ev;
            EV_SET(&ev, (int)ReadEnd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, nullptr);
            SafeKevent(&ev, 1);

            ISimpleThread::Start(); // start poller thread
        }

        ~TKqueueThread() {
            Stop();
            close(KqDescriptor);
        }

        bool ProcessEventsInLoop() {
            std::array<struct kevent, 256> events;

            int numReady = kevent(KqDescriptor, nullptr, 0, events.data(), events.size(), nullptr);
            if (numReady == -1) {
                if (errno == EINTR) {
                    return false;
                } else {
                    Y_ABORT("kevent() failed with %s", strerror(errno));
                }
            }

            bool res = false;

            for (int i = 0; i < numReady; ++i) {
                const struct kevent& ev = events[i];
                if (ev.udata) {
                    TSocketRecord *it = static_cast<TSocketRecord*>(ev.udata);
                    const bool error = ev.flags & (EV_EOF | EV_ERROR);
                    const bool read = error || ev.filter == EVFILT_READ;
                    const bool write = error || ev.filter == EVFILT_WRITE;
                    Notify(it, read, write);
                } else {
                    res = true;
                }
            }

            return res;
        }

        void UnregisterSocketInLoop(const TIntrusivePtr<TSharedDescriptor>& socket) {
            struct kevent ev[2];
            const int fd = socket->GetDescriptor();
            EV_SET(&ev[0], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
            EV_SET(&ev[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
            SafeKevent(ev, 2);
        }

        void RegisterSocket(const TIntrusivePtr<TSocketRecord>& record) {
            int flags = EV_ADD | EV_CLEAR | EV_ENABLE;
            struct kevent ev[2];
            const int fd = record->Socket->GetDescriptor();
            EV_SET(&ev[0], fd, EVFILT_READ, flags, 0, 0, record.Get());
            EV_SET(&ev[1], fd, EVFILT_WRITE, flags, 0, 0, record.Get());
            SafeKevent(ev, 2);
        }

        bool Request(const TIntrusivePtr<TSocketRecord>& /*socket*/, bool /*read*/, bool /*write*/, bool /*suppressNotify*/,
                bool /*afterWouldBlock*/) {
            return false; // no special processing here as we use kqueue in edge-triggered mode
        }
    };

    using TPollerThread = TKqueueThread;

}
