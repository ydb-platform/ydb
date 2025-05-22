#pragma once

namespace NKikimr::NRawSocket {

struct TSocketSettings {
    int AF = {};
    bool TcpNotDelay = false;
};

} // namespace NKikimr::NRawSocket
