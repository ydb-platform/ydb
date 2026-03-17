#if 0
#include "StorageMongoDBSocketFactory.h"

#include <Common/Exception.h>

#include "clickhouse_config.h"

#include <CHDBPoco/Net/IPAddress.h>
#include <CHDBPoco/Net/SocketAddress.h>

#if USE_SSL
#   include <CHDBPoco/Net/SecureStreamSocket.h>
#endif


namespace DB_CHDB
{

namespace ErrorCodes
{
    extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
}

CHDBPoco::Net::StreamSocket StorageMongoDBSocketFactory::createSocket(const std::string & host, int port, CHDBPoco::Timespan connectTimeout, bool secure)
{
    return secure ? createSecureSocket(host, port, connectTimeout) : createPlainSocket(host, port, connectTimeout);
}

CHDBPoco::Net::StreamSocket StorageMongoDBSocketFactory::createPlainSocket(const std::string & host, int port, CHDBPoco::Timespan connectTimeout)
{
    CHDBPoco::Net::SocketAddress address(host, port);
    CHDBPoco::Net::StreamSocket socket;

    socket.connect(address, connectTimeout);

    return socket;
}


CHDBPoco::Net::StreamSocket StorageMongoDBSocketFactory::createSecureSocket(const std::string & host [[maybe_unused]], int port [[maybe_unused]], CHDBPoco::Timespan connectTimeout [[maybe_unused]])
{
#if USE_SSL
    CHDBPoco::Net::SocketAddress address(host, port);
    CHDBPoco::Net::SecureStreamSocket socket;

    socket.setPeerHostName(host);

    socket.connect(address, connectTimeout);

    return socket;
#else
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "SSL is not enabled at build time.");
#endif
}

}
#endif
