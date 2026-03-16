#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTP/HTTPContext.h>

#include <CHDBPoco/Net/HTTPServerParams.h>
#include <CHDBPoco/Net/HTTPServerSession.h>
#include <CHDBPoco/Net/TCPServerConnection.h>

namespace DB_CHDB
{
class TCPServer;

class HTTPServerConnection : public CHDBPoco::Net::TCPServerConnection
{
public:
    HTTPServerConnection(
        HTTPContextPtr context,
        TCPServer & tcp_server,
        const CHDBPoco::Net::StreamSocket & socket,
        CHDBPoco::Net::HTTPServerParams::Ptr params,
        HTTPRequestHandlerFactoryPtr factory,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    HTTPServerConnection(
        HTTPContextPtr context_,
        TCPServer & tcp_server_,
        const CHDBPoco::Net::StreamSocket & socket_,
        CHDBPoco::Net::HTTPServerParams::Ptr params_,
        HTTPRequestHandlerFactoryPtr factory_,
        const String & forwarded_for_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end())
    : HTTPServerConnection(context_, tcp_server_, socket_, params_, factory_, read_event_, write_event_)
    {
        forwarded_for = forwarded_for_;
    }

    void run() override;

protected:
    static void sendErrorResponse(CHDBPoco::Net::HTTPServerSession & session, CHDBPoco::Net::HTTPResponse::HTTPStatus status);

private:
    HTTPContextPtr context;
    TCPServer & tcp_server;
    CHDBPoco::Net::HTTPServerParams::Ptr params;
    HTTPRequestHandlerFactoryPtr factory;
    String forwarded_for;
    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;
    bool stopped;
    std::mutex mutex;  // guards the |factory| with assumption that creating handlers is not thread-safe.
};

}
