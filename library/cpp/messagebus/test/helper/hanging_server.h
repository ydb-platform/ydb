#pragma once

#include <library/cpp/messagebus/network.h>

#include <util/network/sock.h>

class THangingServer {
private:
    std::pair<unsigned, TVector<NBus::TBindResult>> BindResult;

public:
    // listen on given port, and nothing else
    THangingServer(int port = 0);
    // actual port
    int GetPort() const;
};
