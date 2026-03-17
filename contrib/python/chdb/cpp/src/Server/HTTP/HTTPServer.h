#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTP/HTTPContext.h>
#include <Server/TCPServer.h>

#include <CHDBPoco/Net/HTTPServerParams.h>

#include <base/types.h>


namespace DB_CHDB
{

class HTTPServer : public TCPServer
{
public:
    explicit HTTPServer(
        HTTPContextPtr context,
        HTTPRequestHandlerFactoryPtr factory,
        CHDBPoco::ThreadPool & thread_pool,
        CHDBPoco::Net::ServerSocket & socket,
        CHDBPoco::Net::HTTPServerParams::Ptr params,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    ~HTTPServer() override;

    void stopAll(bool abort_current = false);

private:
    HTTPRequestHandlerFactoryPtr factory;
};

}
