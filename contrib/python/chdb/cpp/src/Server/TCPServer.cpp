#include <CHDBPoco/Net/TCPServerConnectionFactory.h>
#include <Server/TCPServer.h>

namespace DB_CHDB
{

class TCPServerConnectionFactoryImpl : public CHDBPoco::Net::TCPServerConnectionFactory
{
public:
    TCPServerConnectionFactoryImpl(TCPServer & tcp_server_, DB_CHDB::TCPServerConnectionFactory::Ptr factory_)
        : tcp_server(tcp_server_)
        , factory(factory_)
    {}

    CHDBPoco::Net::TCPServerConnection * createConnection(const CHDBPoco::Net::StreamSocket & socket) override
    {
        return factory->createConnection(socket, tcp_server);
    }
private:
    TCPServer & tcp_server;
    DB_CHDB::TCPServerConnectionFactory::Ptr factory;
};

TCPServer::TCPServer(
    TCPServerConnectionFactory::Ptr factory_,
    CHDBPoco::ThreadPool & thread_pool,
    CHDBPoco::Net::ServerSocket & socket_,
    CHDBPoco::Net::TCPServerParams::Ptr params)
    : CHDBPoco::Net::TCPServer(new TCPServerConnectionFactoryImpl(*this, factory_), thread_pool, socket_, params)
    , factory(factory_)
    , socket(socket_)
    , is_open(true)
    , port_number(socket.address().port())
{}

}
