#pragma once

#include "netaddr.h"

#include <util/generic/array_ref.h>
#include <util/generic/ptr.h>
#include <util/network/socket.h>

#include <utility>

namespace NBus {
    namespace NPrivate {
        void SetSockOptTcpCork(SOCKET s, bool value);

        [[nodiscard]] ssize_t SocketSend(SOCKET s, TArrayRef<const char> data);

        [[nodiscard]] ssize_t SocketRecv(SOCKET s, TArrayRef<char> buffer);

    }

    struct TBindResult {
        TSimpleSharedPtr<TSocketHolder> Socket;
        TNetAddr Addr;
    };

    std::pair<unsigned, TVector<TBindResult>> BindOnPort(int port, bool reusePort);

}
