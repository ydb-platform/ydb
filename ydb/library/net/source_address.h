#pragma once

#include <util/generic/string.h>
#include <util/network/init.h>

namespace NKikimr::NNet {

    // Textual IP for AF_INET / AF_INET6. Anything else, including nullptr, is "unknown".
    TString FormatSourceAddress(const sockaddr* addr);

    TString PeerSourceAddressFromSocket(SOCKET socket);

} // namespace NKikimr::NNet
