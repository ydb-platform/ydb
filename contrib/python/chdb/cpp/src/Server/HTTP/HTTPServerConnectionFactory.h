#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTP/HTTPContext.h>
#include <Server/TCPServerConnectionFactory.h>

#include <CHDBPoco/Net/HTTPServerParams.h>

namespace DB_CHDB
{

class HTTPServerConnectionFactory : public TCPServerConnectionFactory
{
public:
    HTTPServerConnectionFactory(HTTPContextPtr context, CHDBPoco::Net::HTTPServerParams::Ptr params, HTTPRequestHandlerFactoryPtr factory, const ProfileEvents::Event & read_event_ = ProfileEvents::end(), const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    CHDBPoco::Net::TCPServerConnection * createConnection(const CHDBPoco::Net::StreamSocket & socket, TCPServer & tcp_server) override;
    CHDBPoco::Net::TCPServerConnection * createConnection(const CHDBPoco::Net::StreamSocket & socket, TCPServer & tcp_server, TCPProtocolStackData & stack_data) override;

private:
    HTTPContextPtr context;
    CHDBPoco::Net::HTTPServerParams::Ptr params;
    HTTPRequestHandlerFactoryPtr factory;
    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;
};

}
