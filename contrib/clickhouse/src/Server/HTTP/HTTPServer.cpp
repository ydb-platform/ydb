#include <Server/HTTP/HTTPServer.h>

#include <Server/HTTP/HTTPServerConnectionFactory.h>


namespace DB
{
HTTPServer::HTTPServer(
    HTTPContextPtr context,
    HTTPRequestHandlerFactoryPtr factory_,
    DBPoco::ThreadPool & thread_pool,
    DBPoco::Net::ServerSocket & socket_,
    DBPoco::Net::HTTPServerParams::Ptr params,
    const ProfileEvents::Event & read_event,
    const ProfileEvents::Event & write_event)
    : TCPServer(new HTTPServerConnectionFactory(context, params, factory_, read_event, write_event), thread_pool, socket_, params), factory(factory_)
{
}

HTTPServer::~HTTPServer()
{
    /// We should call stop and join thread here instead of destructor of parent TCPHandler,
    /// because there's possible race on 'vptr' between this virtual destructor and 'run' method.
    stop();
}

void HTTPServer::stopAll(bool /* abortCurrent */)
{
    stop();
}

}
