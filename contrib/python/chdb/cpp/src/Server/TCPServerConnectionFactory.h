#pragma once

#include <CHDBPoco/SharedPtr.h>
#include <Server/TCPProtocolStackData.h>

namespace CHDBPoco
{
namespace Net
{
    class StreamSocket;
    class TCPServerConnection;
}
}
namespace DB_CHDB
{
class TCPServer;

class TCPServerConnectionFactory
{
public:
    using Ptr = CHDBPoco::SharedPtr<TCPServerConnectionFactory>;

    virtual ~TCPServerConnectionFactory() = default;

    /// Same as CHDBPoco::Net::TCPServerConnectionFactory except we can pass the TCPServer
    virtual CHDBPoco::Net::TCPServerConnection * createConnection(const CHDBPoco::Net::StreamSocket & socket, TCPServer & tcp_server) = 0;
    virtual CHDBPoco::Net::TCPServerConnection * createConnection(const CHDBPoco::Net::StreamSocket & socket, TCPServer & tcp_server, TCPProtocolStackData &/* stack_data */)
    {
        return createConnection(socket, tcp_server);
    }
};
}
