#pragma once

#include <util/generic/string.h>
#include <util/network/init.h>

namespace NKikimr::NNet {

    // Textual peer IP without port. AF_INET -> "192.0.2.1", AF_INET6 -> "2001:db8::1"
    // or "::1". nullptr and any other family -> "unknown".
    TString FormatSourceAddress(const sockaddr* addr);

    // Peer IP of socket via getpeername, then FormatSourceAddress.
    // IPv4 loopback -> "127.0.0.1", IPv6 loopback -> "::1".
    // getpeername failure or non-IP family -> "unknown".
    TString PeerSourceAddressFromSocket(SOCKET socket);

} // namespace NKikimr::NNet
