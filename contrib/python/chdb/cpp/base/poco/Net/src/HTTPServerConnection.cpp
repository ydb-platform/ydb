//
// HTTPServerConnection.cpp
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServerConnection
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/HTTPServerConnection.h"
#include "CHDBPoco/Net/HTTPServerSession.h"
#include "CHDBPoco/Net/HTTPServerRequestImpl.h"
#include "CHDBPoco/Net/HTTPServerResponseImpl.h"
#include "CHDBPoco/Net/HTTPRequestHandler.h"
#include "CHDBPoco/Net/HTTPRequestHandlerFactory.h"
#include "CHDBPoco/Net/NetException.h"
#include "CHDBPoco/NumberFormatter.h"
#include "CHDBPoco/Timestamp.h"
#include "CHDBPoco/Delegate.h"
#include <memory>


namespace CHDBPoco {
namespace Net {


HTTPServerConnection::HTTPServerConnection(const StreamSocket& socket, HTTPServerParams::Ptr pParams, HTTPRequestHandlerFactory::Ptr pFactory):
	TCPServerConnection(socket),
	_pParams(pParams),
	_pFactory(pFactory),
	_stopped(false)
{
	CHDB_poco_check_ptr (pFactory);
	
	_pFactory->serverStopped += CHDBPoco::delegate(this, &HTTPServerConnection::onServerStopped);
}


HTTPServerConnection::~HTTPServerConnection()
{
	try
	{
		_pFactory->serverStopped -= CHDBPoco::delegate(this, &HTTPServerConnection::onServerStopped);
	}
	catch (...)
	{
		CHDB_poco_unexpected();
	}
}


void HTTPServerConnection::run()
{
	std::string server = _pParams->getSoftwareVersion();
	HTTPServerSession session(socket(), _pParams);
	while (!_stopped && session.hasMoreRequests())
	{
		try
		{
			CHDBPoco::FastMutex::ScopedLock lock(_mutex);
			if (!_stopped)
			{
				HTTPServerResponseImpl response(session);
				HTTPServerRequestImpl request(response, session, _pParams);
			
				CHDBPoco::Timestamp now;
				response.setDate(now);
				response.setVersion(request.getVersion());
				response.setKeepAlive(_pParams->getKeepAlive() && request.getKeepAlive() && session.canKeepAlive());
				if (!server.empty())
					response.set("Server", server);
				try
				{
#ifndef CHDB_POCO_ENABLE_CPP11
					std::auto_ptr<HTTPRequestHandler> pHandler(_pFactory->createRequestHandler(request));
#else
					std::unique_ptr<HTTPRequestHandler> pHandler(_pFactory->createRequestHandler(request));
#endif
					if (pHandler.get())
					{
						if (request.getExpectContinue() && response.getStatus() == HTTPResponse::HTTP_OK)
							response.sendContinue();
					
						pHandler->handleRequest(request, response);
						session.setKeepAlive(_pParams->getKeepAlive() && response.getKeepAlive() && session.canKeepAlive());

                        /// all that fuzz is all about to make session close with less timeout than 15s (set in HTTPServerParams c-tor)
                        if (_pParams->getKeepAlive() && response.getKeepAlive() && session.canKeepAlive())
                        {
                            int value = response.getKeepAliveTimeout();
                            if (value < 0)
                                value = request.getKeepAliveTimeout();
                            if (value > 0)
                                session.setKeepAliveTimeout(CHDBPoco::Timespan(value, 0));
                        }

                    }
					else sendErrorResponse(session, HTTPResponse::HTTP_NOT_IMPLEMENTED);
				}
				catch (CHDBPoco::Exception&)
				{
					if (!response.sent())
					{
						try
						{
							sendErrorResponse(session, HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
						}
						catch (...)
						{
						}
					}
					throw;
				}
			}
		}
		catch (NoMessageException&)
		{
			break;
		}
		catch (MessageException&)
		{
			sendErrorResponse(session, HTTPResponse::HTTP_BAD_REQUEST);
		}
		catch (CHDBPoco::Exception&)
		{
			if (session.networkException())
			{
				session.networkException()->rethrow();
			}
			else throw;
		}
	}
}


void HTTPServerConnection::sendErrorResponse(HTTPServerSession& session, HTTPResponse::HTTPStatus status)
{
	HTTPServerResponseImpl response(session);
	response.setVersion(HTTPMessage::HTTP_1_1);
	response.setStatusAndReason(status);
	response.setKeepAlive(false);
	response.send();
	session.setKeepAlive(false);
}


void HTTPServerConnection::onServerStopped(const bool& abortCurrent)
{
	_stopped = true;
	if (abortCurrent)
	{
		try
		{
			// Note: On Windows, select() will not return if one of its socket is being
			// shut down. Therefore we have to call close(), which works better.
			// On other platforms, we do the more graceful thing.
			socket().shutdown();
		}
		catch (...)
		{
		}
	}
	else
	{
		CHDBPoco::FastMutex::ScopedLock lock(_mutex);

		try
		{
			socket().shutdown();
		}
		catch (...)
		{
		}
	}
}


} } // namespace CHDBPoco::Net
