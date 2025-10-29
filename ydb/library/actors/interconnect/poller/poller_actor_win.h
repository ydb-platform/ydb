#pragma once

namespace NActors {

    class TSelectThread : public TPollerThreadBase<TSelectThread> {
        TMutex Mutex;
        std::unordered_map<SOCKET, TIntrusivePtr<TSocketRecord>> Descriptors;

        enum {
            READ = 1,
            WRITE = 2,
        };

    public:
        TSelectThread(TActorSystem *actorSystem)
            : TPollerThreadBase(actorSystem)
        {
            Descriptors.emplace(ReadEnd, nullptr);
            ISimpleThread::Start();
        }

        ~TSelectThread() {
            Stop();
        }

        bool ProcessEventsInLoop() {
            fd_set readfds, writefds, exceptfds;

            FD_ZERO(&readfds);
            FD_ZERO(&writefds);
            FD_ZERO(&exceptfds);
            int nfds = 0;
            with_lock (Mutex) {
                for (const auto& [key, record] : Descriptors) {
                    const int fd = key;
                    auto add = [&](auto& set) {
                        FD_SET(fd, &set);
                        nfds = Max<int>(nfds, fd + 1);
                    };
                    if (!record || (record->Flags & READ)) {
                        add(readfds);
                    }
                    if (!record || (record->Flags & WRITE)) {
                        add(writefds);
                    }
                    add(exceptfds);
                }
            }

            int res = select(nfds, &readfds, &writefds, &exceptfds, nullptr);
            if (res == -1) {
                const int err = LastSocketError();
                if (err == EINTR) {
                    return false; // try a bit later
                } else {
                    Y_ABORT("select() failed with %s", strerror(err));
                }
            }

            bool flag = false;

            with_lock (Mutex) {
                for (const auto& [fd, record] : Descriptors) {
                    if (record) {
                        const bool error = FD_ISSET(fd, &exceptfds);
                        const bool read = error || FD_ISSET(fd, &readfds);
                        const bool write = error || FD_ISSET(fd, &writefds);
                        if (read) {
                            record->Flags &= ~READ;
                        }
                        if (write) {
                            record->Flags &= ~WRITE;
                        }
                        Notify(record.Get(), read, write);
                    } else {
                        flag = true;
                    }
                }
            }

            return flag;
        }

        void UnregisterSocketInLoop(const TIntrusivePtr<TSharedDescriptor>& socket) {
            with_lock (Mutex) {
                Descriptors.erase(socket->GetDescriptor());
            }
        }

        void RegisterSocket(const TIntrusivePtr<TSocketRecord>& record) {
            with_lock (Mutex) {
                Descriptors.emplace(record->Socket->GetDescriptor(), record);
            }
            ExecuteSyncOperation(TPollerWakeup());
        }

        bool Request(const TIntrusivePtr<TSocketRecord>& record, bool read, bool write, bool /*suppressNotify*/,
                bool /*afterWouldBlock*/) {
            with_lock (Mutex) {
                const auto it = Descriptors.find(record->Socket->GetDescriptor());
                Y_ABORT_UNLESS(it != Descriptors.end());
                it->second->Flags |= (read ? READ : 0) | (write ? WRITE : 0);
            }
            ExecuteSyncOperation(TPollerWakeup());
            return false;
        }
    };

    using TPollerThread = TSelectThread;

} // NActors
