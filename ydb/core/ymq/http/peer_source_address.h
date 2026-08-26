#pragma once

#include <util/generic/string.h>
#include <util/network/init.h>

namespace NKikimr::NSQS {

TString PeerSourceAddressFromSocket(SOCKET socket);

} // namespace NKikimr::NSQS
