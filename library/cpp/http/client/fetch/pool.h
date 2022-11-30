#pragma once

#include "cosocket.h"

#include <library/cpp/http/client/ssl/sslsock.h>

#include <util/generic/hash_multi_map.h>
#include <util/generic/ptr.h>
#include <util/system/mutex.h>

namespace NHttpFetcher {
    class TSocketPool {
    public:
        using TSocketHandle = TSslSocketHandler<TCoSocketHandler, TSslSocketBase::TFakeLogger>;

    public:
        /// Closes all sockets.
        void Clear();

        /// Closes all sockets that have been opened too long.
        void Drain(const TDuration timeout);

        /// Returns socket for the given endpoint if available.
        THolder<TSocketHandle> GetSocket(const TString& host, const TIpPort port);

        /// Puts socket to the pool.
        void ReturnSocket(const TString& host, const TIpPort port, THolder<TSocketHandle> socket);

    private:
        struct TConnection {
            THolder<TSocketHandle> Socket;
            TInstant Touched;
        };

        using TSocketMap = THashMultiMap<std::pair<TString, TIpPort>, TConnection>;

        TMutex Lock_;
        TSocketMap Sockets_;
    };

}
