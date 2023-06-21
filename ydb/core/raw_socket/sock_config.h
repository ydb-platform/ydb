#pragma once

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/protos/services_common.pb.h>

#include "sock64.h"

namespace NKikimr::NRawSocket {

struct TNetworkConfig {
    static constexpr size_t BUFFER_SIZE = 1 * 1024;
    static constexpr int LISTEN_QUEUE = 10;
    static constexpr TDuration SOCKET_TIMEOUT = TDuration::MilliSeconds(60000);
    static constexpr TDuration CONNECTION_TIMEOUT = TDuration::MilliSeconds(60000);

    using TSocketType = TInet64StreamSocket;
    using TSecureSocketType = TInet64SecureStreamSocket;
    using TSocketAddressType = std::shared_ptr<ISockAddr>;
};

} // namespace NKikimr::NRawSocket

inline IOutputStream& operator <<(IOutputStream& out, const std::shared_ptr<ISockAddr>& addr) {
    return out << addr->ToString();
}
