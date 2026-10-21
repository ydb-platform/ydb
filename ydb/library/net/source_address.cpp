#include "source_address.h"

#include <util/generic/yexception.h>
#include <util/network/address.h>
#include <util/string/strip.h>

namespace NKikimr::NNet {

    TString ExtractFirstForwardedForAddress(TStringBuf forwardedFor) {
        return TString(StripString(forwardedFor.Before(',')));
    }

    TString FormatSourceAddress(const sockaddr* addr) {
        if (!addr) {
            return "unknown";
        }
        try {
            return NAddr::PrintHost(NAddr::TOpaqueAddr(addr));
        } catch (const yexception&) {
            return "unknown";
        }
    }

    TString PeerSourceAddressFromSocket(SOCKET socket) {
        try {
            return NAddr::PrintHost(*NAddr::GetPeerAddr(socket));
        } catch (const yexception&) {
            return "unknown";
        }
    }

} // namespace NKikimr::NNet
