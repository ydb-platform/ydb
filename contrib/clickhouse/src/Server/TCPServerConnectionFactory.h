#pragma once

#include <DBPoco/SharedPtr.h>
#include <Server/TCPProtocolStackData.h>

namespace DBPoco
{
namespace Net
{
    class StreamSocket;
    class TCPServerConnection;
}
}
namespace DB
{
class TCPServer;

class TCPServerConnectionFactory
{
public:
    using Ptr = DBPoco::SharedPtr<TCPServerConnectionFactory>;

    virtual ~TCPServerConnectionFactory() = default;

    /// Same as DBPoco::Net::TCPServerConnectionFactory except we can pass the TCPServer
    virtual DBPoco::Net::TCPServerConnection * createConnection(const DBPoco::Net::StreamSocket & socket, TCPServer & tcp_server) = 0;
    virtual DBPoco::Net::TCPServerConnection * createConnection(const DBPoco::Net::StreamSocket & socket, TCPServer & tcp_server, TCPProtocolStackData &/* stack_data */)
    {
        return createConnection(socket, tcp_server);
    }
};
}
