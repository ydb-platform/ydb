#include "pool.h"

namespace NHttpFetcher {
    void TSocketPool::Clear() {
        TSocketMap sockets;

        {
            auto g(Guard(Lock_));
            Sockets_.swap(sockets);
        }
    }

    void TSocketPool::Drain(const TDuration timeout) {
        const TInstant now = TInstant::Now();
        TVector<THolder<TSocketHandle>> sockets;

        {
            auto g(Guard(Lock_));
            for (auto si = Sockets_.begin(); si != Sockets_.end();) {
                if (si->second.Touched + timeout < now) {
                    sockets.push_back(std::move(si->second.Socket));
                    Sockets_.erase(si++);
                } else {
                    ++si;
                }
            }
        }
    }

    THolder<TSocketPool::TSocketHandle> TSocketPool::GetSocket(const TString& host, const TIpPort port) {
        THolder<TSocketPool::TSocketHandle> socket;

        {
            auto g(Guard(Lock_));
            auto si = Sockets_.find(std::make_pair(host, port));
            if (si != Sockets_.end()) {
                socket = std::move(si->second.Socket);
                Sockets_.erase(si);
            }
        }

        return socket;
    }

    void TSocketPool::ReturnSocket(const TString& host, const TIpPort port, THolder<TSocketHandle> socket) {
        TConnection conn;

        conn.Socket = std::move(socket);
        conn.Touched = TInstant::Now();

        {
            auto g(Guard(Lock_));
            Sockets_.emplace(std::make_pair(host, port), std::move(conn));
        }
    }

}
