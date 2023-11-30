#pragma once
#include <util/network/sock.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>

class TInet64StreamSocket;

namespace NHttp {

struct THttpConfig {
    static constexpr NActors::NLog::EComponent HttpLog = NActorsServices::EServiceCommon::HTTP;
    static constexpr size_t BUFFER_SIZE = 64 * 1024;
    static constexpr size_t BUFFER_MIN_STEP = 10 * 1024;
    static constexpr int LISTEN_QUEUE = 10;
    static constexpr TDuration SOCKET_TIMEOUT = TDuration::MilliSeconds(60000);
    static constexpr TDuration CONNECTION_TIMEOUT = TDuration::MilliSeconds(60000);
    using SocketType = TInet64StreamSocket;
    using SocketAddressType = std::shared_ptr<ISockAddr>;
};

}
