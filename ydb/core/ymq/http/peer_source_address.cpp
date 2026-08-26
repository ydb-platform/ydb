#include "peer_source_address.h"

namespace NKikimr::NSQS {

TString PeerSourceAddressFromSocket(SOCKET socket) {
    sockaddr_storage addr{};
    socklen_t addrSize = sizeof(addr);
    if (getpeername(socket, reinterpret_cast<sockaddr*>(&addr), &addrSize) != 0) {
        return "unknown";
    }

    const void* src = nullptr;
    const int family = addr.ss_family;
    if (family == AF_INET) {
        src = &reinterpret_cast<const sockaddr_in*>(&addr)->sin_addr;
    } else if (family == AF_INET6) {
        src = &reinterpret_cast<const sockaddr_in6*>(&addr)->sin6_addr;
    }

    char address[INET6_ADDRSTRLEN];
    if (src && inet_ntop(family, src, address, sizeof(address)) != nullptr) {
        return address;
    }
    return "unknown";
}

} // namespace NKikimr::NSQS
