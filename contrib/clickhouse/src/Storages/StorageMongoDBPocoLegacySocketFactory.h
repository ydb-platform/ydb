#pragma once

#include "clickhouse_config.h"

#if USE_MONGODB
#error #include <DBPoco/MongoDB/Connection.h>


namespace DB
{

/// Deprecated, will be removed soon.
class StorageMongoDBPocoLegacySocketFactory : public DBPoco::MongoDB::Connection::SocketFactory
{
public:
    DBPoco::Net::StreamSocket createSocket(const std::string & host, int port, DBPoco::Timespan connectTimeout, bool secure) override;

private:
    static DBPoco::Net::StreamSocket createPlainSocket(const std::string & host, int port, DBPoco::Timespan connectTimeout);
    static DBPoco::Net::StreamSocket createSecureSocket(const std::string & host, int port, DBPoco::Timespan connectTimeout);
};

}
#endif
