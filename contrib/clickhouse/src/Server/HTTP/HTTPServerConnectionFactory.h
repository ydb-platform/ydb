#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTP/HTTPContext.h>
#include <Server/TCPServerConnectionFactory.h>

#include <DBPoco/Net/HTTPServerParams.h>

namespace DB
{

class HTTPServerConnectionFactory : public TCPServerConnectionFactory
{
public:
    HTTPServerConnectionFactory(HTTPContextPtr context, DBPoco::Net::HTTPServerParams::Ptr params, HTTPRequestHandlerFactoryPtr factory, const ProfileEvents::Event & read_event_ = ProfileEvents::end(), const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    DBPoco::Net::TCPServerConnection * createConnection(const DBPoco::Net::StreamSocket & socket, TCPServer & tcp_server) override;
    DBPoco::Net::TCPServerConnection * createConnection(const DBPoco::Net::StreamSocket & socket, TCPServer & tcp_server, TCPProtocolStackData & stack_data) override;

private:
    HTTPContextPtr context;
    DBPoco::Net::HTTPServerParams::Ptr params;
    HTTPRequestHandlerFactoryPtr factory;
    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;
};

}
