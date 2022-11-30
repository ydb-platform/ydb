#pragma once

#include "coctx.h"

#include <library/cpp/coroutine/engine/network.h>
#include <library/cpp/http/fetch_gpl/sockhandler.h>

#include <util/system/error.h>

namespace NHttpFetcher {
    class TCoSocketHandler {
    public:
        TCoSocketHandler() = default;

        ~TCoSocketHandler() {
            Disconnect();
        }

        int Good() const {
            return (Fd != INVALID_SOCKET);
        }

        int Connect(const TAddrList& addrs, TDuration timeout) {
            TCont* cont = CoCtx()->Cont();
            Timeout = timeout;
            for (const auto& item : addrs) {
                try {
                    const sockaddr* sa = item->Addr();
                    TSocketHolder s(NCoro::Socket(sa->sa_family, SOCK_STREAM, 0));
                    if (s.Closed()) {
                        continue;
                    }
                    int err = NCoro::ConnectT(cont, s, sa, item->Len(), Timeout);
                    if (err) {
                        s.Close();
                        errno = err;
                        continue;
                    }
                    SetZeroLinger(s);
                    SetKeepAlive(s, true);
                    Fd.Swap(s);
                    return 0;
                } catch (const TSystemError&) {
                }
            }
            return errno ? errno : EBADF;
        }

        void Disconnect() {
            if (Fd.Closed())
                return;
            try {
                ShutDown(Fd, SHUT_RDWR);
            } catch (const TSystemError&) {
            }
            Fd.Close();
        }

        void shutdown() {
            try {
                ShutDown(Fd, SHUT_WR);
            } catch (TSystemError&) {
            }
        }

        ssize_t send(const void* message, size_t messlen) {
            TCont* cont = CoCtx()->Cont();
            TContIOStatus status = NCoro::WriteT(cont, Fd, message, messlen, Timeout);
            errno = status.Status();
            return status.Status() ? -1 : (ssize_t)status.Processed();
        }

        bool peek() {
            TCont* cont = CoCtx()->Cont();
            if ((errno = NCoro::PollT(cont, Fd, CONT_POLL_READ, Timeout)))
                return false;
            char buf[1];
#ifdef _win32_
            return (1 == ::recv(Fd, buf, 1, MSG_PEEK));
#else
            return (1 == ::recv(Fd, buf, 1, MSG_PEEK | MSG_DONTWAIT));
#endif
        }

        ssize_t read(void* message, size_t messlen) {
            TCont* cont = CoCtx()->Cont();
            TContIOStatus status = NCoro::ReadT(cont, Fd, message, messlen, Timeout);
            errno = status.Status();
            return status.Status() ? -1 : (ssize_t)status.Processed();
        }

    protected:
        TSocketHolder Fd;
        TDuration Timeout;
        static THolder<TIpAddress> AddrToBind;
    };
}
