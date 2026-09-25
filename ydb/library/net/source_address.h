#pragma once

#include <util/generic/string.h>
#include <util/network/init.h>

namespace NKikimr::NNet {

    // Peer IP without port via NAddr::PrintHost. AF_INET -> "192.0.2.1",
    // AF_INET6 -> "2001:db8::1" or "::1". nullptr, unsupported family, or
    // PrintHost failure -> "unknown".
    TString FormatSourceAddress(const sockaddr* addr);

    // Peer of socket via NAddr::GetPeerAddr + PrintHost (getpeername, not
    // getsockname). IPv4 loopback -> "127.0.0.1", IPv6 loopback -> "::1".
    // getpeername / PrintHost failure -> "unknown".
    TString PeerSourceAddressFromSocket(SOCKET socket);

} // namespace NKikimr::NNet
