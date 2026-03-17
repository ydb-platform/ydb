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


#include "DBPoco/Net/HTTPServerConnection.h"
#include "DBPoco/Net/HTTPServerSession.h"
#include "DBPoco/Net/HTTPServerRequestImpl.h"
#include "DBPoco/Net/HTTPServerResponseImpl.h"
#include "DBPoco/Net/HTTPRequestHandler.h"
#include "DBPoco/Net/HTTPRequestHandlerFactory.h"
#include "DBPoco/Net/NetException.h"
#include "DBPoco/NumberFormatter.h"
#include "DBPoco/Timestamp.h"
#include "DBPoco/Delegate.h"
#include <memory>


namespace DBPoco {
namespace Net {


HTTPServerConnection::HTTPServerConnection(const StreamSocket& socket, HTTPServerParams::Ptr pParams, HTTPRequestHandlerFactory::Ptr pFactory):
	TCPServerConnection(socket),
	_pParams(pParams),
	_pFactory(pFactory),
	_stopped(false)
{
	DB_poco_check_ptr (pFactory);
	
	_pFactory->serverStopped += DBPoco::delegate(this, &HTTPServerConnection::onServerStopped);
}


HTTPServerConnection::~HTTPServerConnection()
{
	try
	{
		_pFactory->serverStopped -= DBPoco::delegate(this, &HTTPServerConnection::onServerStopped);
	}
	catch (...)
	{
		DB_poco_unexpected();
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
			DBPoco::FastMutex::ScopedLock lock(_mutex);
			if (!_stopped)
			{
				HTTPServerResponseImpl response(session);
				HTTPServerRequestImpl request(response, session, _pParams);
			
				DBPoco::Timestamp now;
				response.setDate(now);
				response.setVersion(request.getVersion());
				response.setKeepAlive(_pParams->getKeepAlive() && request.getKeepAlive() && session.canKeepAlive());
				if (!server.empty())
					response.set("Server", server);
				try
				{
#ifndef DB_POCO_ENABLE_CPP11
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
                                session.setKeepAliveTimeout(DBPoco::Timespan(value, 0));
                        }

                    }
					else sendErrorResponse(session, HTTPResponse::HTTP_NOT_IMPLEMENTED);
				}
				catch (DBPoco::Exception&)
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
		catch (DBPoco::Exception&)
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
		DBPoco::FastMutex::ScopedLock lock(_mutex);

		try
		{
			socket().shutdown();
		}
		catch (...)
		{
		}
	}
}


} } // namespace DBPoco::Net
