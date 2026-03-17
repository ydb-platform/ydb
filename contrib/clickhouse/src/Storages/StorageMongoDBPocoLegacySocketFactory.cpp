#include "clickhouse_config.h"

#if USE_MONGODB
#include "StorageMongoDBPocoLegacySocketFactory.h"

#include <Common/Exception.h>

#include <DBPoco/Net/IPAddress.h>
#include <DBPoco/Net/SocketAddress.h>

#if USE_SSL
#   include <DBPoco/Net/SecureStreamSocket.h>
#endif


namespace DB
{

namespace ErrorCodes
{
extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
}

DBPoco::Net::StreamSocket StorageMongoDBPocoLegacySocketFactory::createSocket(const std::string & host, int port, DBPoco::Timespan connectTimeout, bool secure)
{
    return secure ? createSecureSocket(host, port, connectTimeout) : createPlainSocket(host, port, connectTimeout);
}

DBPoco::Net::StreamSocket StorageMongoDBPocoLegacySocketFactory::createPlainSocket(const std::string & host, int port, DBPoco::Timespan connectTimeout)
{
    DBPoco::Net::SocketAddress address(host, port);
    DBPoco::Net::StreamSocket socket;

    socket.connect(address, connectTimeout);

    return socket;
}


DBPoco::Net::StreamSocket StorageMongoDBPocoLegacySocketFactory::createSecureSocket(const std::string & host [[maybe_unused]], int port [[maybe_unused]], DBPoco::Timespan connectTimeout [[maybe_unused]])
{
#if USE_SSL
    DBPoco::Net::SocketAddress address(host, port);
    DBPoco::Net::SecureStreamSocket socket;

    socket.setPeerHostName(host);

    socket.connect(address, connectTimeout);

    return socket;
#else
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "SSL is not enabled at build time.");
#endif
}

}
#endif
