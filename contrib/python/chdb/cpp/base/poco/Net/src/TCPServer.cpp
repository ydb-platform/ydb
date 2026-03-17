//
// TCPServer.cpp
//
// Library: Net
// Package: TCPServer
// Module:  TCPServer
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:    BSL-1.0
//


#include "CHDBPoco/Net/TCPServer.h"
#include "CHDBPoco/Net/TCPServerDispatcher.h"
#include "CHDBPoco/Net/TCPServerConnection.h"
#include "CHDBPoco/Net/TCPServerConnectionFactory.h"
#include "CHDBPoco/Timespan.h"
#include "CHDBPoco/Exception.h"
#include "CHDBPoco/ErrorHandler.h"


using CHDBPoco::ErrorHandler;


namespace CHDBPoco {
namespace Net {


//
// TCPServerConnectionFilter
//


TCPServerConnectionFilter::~TCPServerConnectionFilter()
{
}


//
// TCPServer
//


TCPServer::TCPServer(TCPServerConnectionFactory::Ptr pFactory, CHDBPoco::UInt16 portNumber, TCPServerParams::Ptr pParams):
    _socket(ServerSocket(portNumber)),
    _thread(threadName(_socket)),
    _stopped(true)
{
    CHDBPoco::ThreadPool& pool = CHDBPoco::ThreadPool::defaultPool();
    if (pParams)
    {
        int toAdd = pParams->getMaxThreads() - pool.capacity();
        if (toAdd > 0) pool.addCapacity(toAdd);
    }
    _pDispatcher = new TCPServerDispatcher(pFactory, pool, pParams);

}


TCPServer::TCPServer(TCPServerConnectionFactory::Ptr pFactory, const ServerSocket& socket, TCPServerParams::Ptr pParams):
    _socket(socket),
    _thread(threadName(socket)),
    _stopped(true)
{
    CHDBPoco::ThreadPool& pool = CHDBPoco::ThreadPool::defaultPool();
    if (pParams)
    {
        int toAdd = pParams->getMaxThreads() - pool.capacity();
        if (toAdd > 0) pool.addCapacity(toAdd);
    }
    _pDispatcher = new TCPServerDispatcher(pFactory, pool, pParams);
}


TCPServer::TCPServer(TCPServerConnectionFactory::Ptr pFactory, CHDBPoco::ThreadPool& threadPool, const ServerSocket& socket, TCPServerParams::Ptr pParams):
    _socket(socket),
    _pDispatcher(new TCPServerDispatcher(pFactory, threadPool, pParams)),
    _thread(threadName(socket)),
    _stopped(true)
{
}


TCPServer::~TCPServer()
{
    try
    {
        stop();
        _pDispatcher->release();
    }
    catch (...)
    {
        CHDB_poco_unexpected();
    }
}


const TCPServerParams& TCPServer::params() const
{
    return _pDispatcher->params();
}


void TCPServer::start()
{
    CHDB_poco_assert (_stopped);

    _stopped = false;
    _thread.start(*this);
}


void TCPServer::stop()
{
    if (!_stopped)
    {
        _stopped = true;
        _thread.join();
        _pDispatcher->stop();
    }
}


void TCPServer::run()
{
    while (!_stopped)
    {
        CHDBPoco::Timespan timeout(250000);
        try
        {
            if (_socket.poll(timeout, Socket::SELECT_READ))
            {
                try
                {
                    StreamSocket ss = _socket.acceptConnection();

                    if (!_pConnectionFilter || _pConnectionFilter->accept(ss))
                    {
                        // enable nodelay per default: OSX really needs that
#if defined(CHDB_POCO_OS_FAMILY_UNIX)
                        if (ss.address().family() != AddressFamily::UNIX_LOCAL)
#endif
                        {
                            ss.setNoDelay(true);
                        }
                        _pDispatcher->enqueue(ss);
                    }
                    else
                    {
                        ErrorHandler::logMessage(Message::PRIO_WARNING, "Filtered out connection from " + ss.peerAddress().toString());
                    }
                }
                catch (CHDBPoco::Exception& exc)
                {
                    ErrorHandler::handle(exc);
                }
                catch (std::exception& exc)
                {
                    ErrorHandler::handle(exc);
                }
                catch (...)
                {
                    ErrorHandler::handle();
                }
            }
        }
        catch (CHDBPoco::Exception& exc)
        {
            ErrorHandler::handle(exc);
            // possibly a resource issue since poll() failed;
            // give some time to recover before trying again
            CHDBPoco::Thread::sleep(50);
        }
    }
}


int TCPServer::currentThreads() const
{
    return _pDispatcher->currentThreads();
}


int TCPServer::maxThreads() const
{
    return _pDispatcher->maxThreads();
}


int TCPServer::totalConnections() const
{
    return _pDispatcher->totalConnections();
}


int TCPServer::currentConnections() const
{
    return _pDispatcher->currentConnections();
}


int TCPServer::maxConcurrentConnections() const
{
    return _pDispatcher->maxConcurrentConnections();
}


int TCPServer::queuedConnections() const
{
    return _pDispatcher->queuedConnections();
}


int TCPServer::refusedConnections() const
{
    return _pDispatcher->refusedConnections();
}


void TCPServer::setConnectionFilter(const TCPServerConnectionFilter::Ptr& pConnectionFilter)
{
    CHDB_poco_assert (_stopped);

    _pConnectionFilter = pConnectionFilter;
}


std::string TCPServer::threadName(const ServerSocket& socket)
{
    std::string name("TCPServer: ");
    name.append(socket.address().toString());
    return name;

}


} } // namespace CHDBPoco::Net
