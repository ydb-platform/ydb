#pragma once

#include <CHDBPoco/Net/TCPServerConnection.h>
#include <Server/TCPProtocolStackData.h>
#include <CHDBPoco/Util/LayeredConfiguration.h>

#include "clickhouse_config.h"

#if USE_SSL
#   include <CHDBPoco/Net/Context.h>
#   include <CHDBPoco/Net/SecureStreamSocket.h>
#   include <CHDBPoco/Net/SSLManager.h>
#endif

namespace DB_CHDB
{

class TLSHandler : public CHDBPoco::Net::TCPServerConnection
{
#if USE_SSL
    using SecureStreamSocket = CHDBPoco::Net::SecureStreamSocket;
    using SSLManager = CHDBPoco::Net::SSLManager;
    using Context = CHDBPoco::Net::Context;
#endif
    using StreamSocket = CHDBPoco::Net::StreamSocket;
    using LayeredConfiguration = CHDBPoco::Util::LayeredConfiguration;
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
