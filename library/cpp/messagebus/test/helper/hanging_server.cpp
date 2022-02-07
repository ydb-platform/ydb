#include "hanging_server.h"

#include <util/system/yassert.h>

using namespace NBus;

THangingServer::THangingServer(int port) {
    BindResult = BindOnPort(port, false);
}

int THangingServer::GetPort() const {
    return BindResult.first;
}
