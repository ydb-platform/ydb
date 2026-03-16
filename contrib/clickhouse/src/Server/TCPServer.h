#pragma once

#include <DBPoco/Net/TCPServer.h>

#include <base/types.h>
#include <Server/TCPServerConnectionFactory.h>


namespace DB
{
class Context;

class TCPServer : public DBPoco::Net::TCPServer
{
public:
    explicit TCPServer(
        TCPServerConnectionFactory::Ptr factory,
        DBPoco::ThreadPool & thread_pool,
        DBPoco::Net::ServerSocket & socket,
        DBPoco::Net::TCPServerParams::Ptr params = new DBPoco::Net::TCPServerParams);

    /// Close the socket and ask existing connections to stop serving queries
    void stop()
    {
        if (!is_open)
            return;

        // FIXME: On darwin calling shutdown(SHUT_RD) on the socket blocked in accept() leads to ENOTCONN
#ifndef OS_DARWIN
        // Shutdown the listen socket before stopping tcp server to avoid 2.5second delay
        socket.shutdownReceive();
#endif

        DBPoco::Net::TCPServer::stop();
        // This notifies already established connections that they should stop serving
        // queries and close their socket as soon as they can.
        is_open = false;
        // Poco's stop() stops listening on the socket but leaves it open.
        // To be able to hand over control of the listening port to a new server, and
        // to get fast connection refusal instead of timeouts, we also need to close
        // the listening socket.
        socket.close();
    }

    bool isOpen() const { return is_open; }

    UInt16 portNumber() const { return port_number; }

    const DBPoco::Net::ServerSocket& getSocket() { return socket; }

private:
    TCPServerConnectionFactory::Ptr factory;
    DBPoco::Net::ServerSocket socket;
    std::atomic<bool> is_open;
    UInt16 port_number;
};

}
