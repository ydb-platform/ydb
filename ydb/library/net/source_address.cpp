#include "source_address.h"

namespace NKikimr::NNet {

    TString FormatSourceAddress(const sockaddr* addr) {
        if (!addr) {
            return "unknown";
        }

        switch (addr->sa_family) {
            case AF_INET: {
                const auto* in = reinterpret_cast<const sockaddr_in*>(addr);
                char ip[INET_ADDRSTRLEN];
                if (inet_ntop(AF_INET, &in->sin_addr, ip, sizeof(ip)) != nullptr) {
                    return ip;
                }
                break;
            }
            case AF_INET6: {
                const auto* in6 = reinterpret_cast<const sockaddr_in6*>(addr);
                char ip6[INET6_ADDRSTRLEN];
                if (inet_ntop(AF_INET6, &in6->sin6_addr, ip6, sizeof(ip6)) != nullptr) {
                    return ip6;
                }
                break;
            }
        }

        return "unknown";
    }

    TString PeerSourceAddressFromSocket(SOCKET socket) {
        sockaddr_storage addr{};
        socklen_t addrSize = sizeof(addr);
        if (getpeername(socket, reinterpret_cast<sockaddr*>(&addr), &addrSize) != 0) {
            return "unknown";
        }
        return FormatSourceAddress(reinterpret_cast<const sockaddr*>(&addr));
    }

} // namespace NKikimr::NNet
