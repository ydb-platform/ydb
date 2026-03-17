#include <DBPoco/Net/TCPServerConnectionFactory.h>
#include <Server/TCPServer.h>

namespace DB
{

class TCPServerConnectionFactoryImpl : public DBPoco::Net::TCPServerConnectionFactory
{
public:
    TCPServerConnectionFactoryImpl(TCPServer & tcp_server_, DB::TCPServerConnectionFactory::Ptr factory_)
        : tcp_server(tcp_server_)
        , factory(factory_)
    {}

    DBPoco::Net::TCPServerConnection * createConnection(const DBPoco::Net::StreamSocket & socket) override
    {
        return factory->createConnection(socket, tcp_server);
    }
private:
    TCPServer & tcp_server;
    DB::TCPServerConnectionFactory::Ptr factory;
};

TCPServer::TCPServer(
    TCPServerConnectionFactory::Ptr factory_,
    DBPoco::ThreadPool & thread_pool,
    DBPoco::Net::ServerSocket & socket_,
    DBPoco::Net::TCPServerParams::Ptr params)
    : DBPoco::Net::TCPServer(new TCPServerConnectionFactoryImpl(*this, factory_), thread_pool, socket_, params)
    , factory(factory_)
    , socket(socket_)
    , is_open(true)
    , port_number(socket.address().port())
{}

}
