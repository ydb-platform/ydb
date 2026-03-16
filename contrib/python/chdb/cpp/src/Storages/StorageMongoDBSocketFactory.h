#pragma once

#error #include <CHDBPoco/MongoDB/Connection.h>


namespace DB_CHDB
{

class StorageMongoDBSocketFactory : public CHDBPoco::MongoDB::Connection::SocketFactory
{
public:
    CHDBPoco::Net::StreamSocket createSocket(const std::string & host, int port, CHDBPoco::Timespan connectTimeout, bool secure) override;

private:
    static CHDBPoco::Net::StreamSocket createPlainSocket(const std::string & host, int port, CHDBPoco::Timespan connectTimeout);
    static CHDBPoco::Net::StreamSocket createSecureSocket(const std::string & host, int port, CHDBPoco::Timespan connectTimeout);
};

}
