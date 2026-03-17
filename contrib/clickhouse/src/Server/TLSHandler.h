#pragma once

#include <DBPoco/Net/TCPServerConnection.h>
#include <Server/TCPProtocolStackData.h>
#include <DBPoco/Util/LayeredConfiguration.h>

#include "clickhouse_config.h"

#if USE_SSL
#   include <DBPoco/Net/Context.h>
#   include <DBPoco/Net/SecureStreamSocket.h>
#   include <DBPoco/Net/SSLManager.h>
#endif

namespace DB
{

class TLSHandler : public DBPoco::Net::TCPServerConnection
{
#if USE_SSL
    using SecureStreamSocket = DBPoco::Net::SecureStreamSocket;
    using SSLManager = DBPoco::Net::SSLManager;
    using Context = DBPoco::Net::Context;
#endif
    using StreamSocket = DBPoco::Net::StreamSocket;
    using LayeredConfiguration = DBPoco::Util::LayeredConfiguration;
public:
    explicit TLSHandler(const StreamSocket & socket, const LayeredConfiguration & config_, const std::string & prefix_, TCPProtocolStackData & stack_data_);

    void run() override;

private:
#if USE_SSL
    Context::Params params [[maybe_unused]];
    Context::Usage usage [[maybe_unused]];
    int disabled_protocols = 0;
    bool extended_verification = false;
    bool prefer_server_ciphers = false;
    const LayeredConfiguration & config [[maybe_unused]];
    std::string prefix [[maybe_unused]];
#endif
    TCPProtocolStackData & stack_data [[maybe_unused]];
};


}
