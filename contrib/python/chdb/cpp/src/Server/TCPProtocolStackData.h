#pragma once

#include <string>
#include <CHDBPoco/Net/StreamSocket.h>

namespace DB_CHDB
{

// Data to communicate between protocol layers
struct TCPProtocolStackData
{
    // socket implementation can be replaced by some layer - TLS as an example
    CHDBPoco::Net::StreamSocket socket;
    // host from PROXY layer
    std::string forwarded_for;
    // certificate path from TLS layer to TCP layer
    std::string certificate;
    // default database from endpoint configuration to TCP layer
    std::string default_database;
};

}
