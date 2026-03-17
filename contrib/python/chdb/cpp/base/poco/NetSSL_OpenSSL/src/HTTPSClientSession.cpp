//
// HTTPSClientSession.cpp
//
// Library: NetSSL_OpenSSL
// Package: HTTPSClient
// Module:  HTTPSClientSession
//
// Copyright (c) 2006-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/HTTPSClientSession.h"
#include "CHDBPoco/Net/HTTPSSessionInstantiator.h"
#include "CHDBPoco/Net/SecureStreamSocket.h"
#include "CHDBPoco/Net/SecureStreamSocketImpl.h"
#include "CHDBPoco/Net/SSLManager.h"
#include "CHDBPoco/Net/SSLException.h"
#include "CHDBPoco/Net/HTTPRequest.h"
#include "CHDBPoco/Net/HTTPResponse.h"
#include "CHDBPoco/Net/NetException.h"
#include "CHDBPoco/NumberFormatter.h"


using CHDBPoco::NumberFormatter;
using CHDBPoco::IllegalStateException;


namespace CHDBPoco {
namespace Net {


HTTPSClientSession::HTTPSClientSession():
	HTTPClientSession(SecureStreamSocket()),
	_pContext(SSLManager::instance().defaultClientContext())
{
	setPort(HTTPS_PORT);
	_proxySessionFactory.registerProtocol("https", new HTTPSSessionInstantiator);
}


HTTPSClientSession::HTTPSClientSession(const SecureStreamSocket& socket):
	HTTPClientSession(socket),
	_pContext(socket.context())
{
	setPort(HTTPS_PORT);
	_proxySessionFactory.registerProtocol("https", new HTTPSSessionInstantiator);
}


HTTPSClientSession::HTTPSClientSession(const SecureStreamSocket& socket, Session::Ptr pSession):
	HTTPClientSession(socket),
	_pContext(socket.context()),
	_pSession(pSession)
{
	setPort(HTTPS_PORT);
	_proxySessionFactory.registerProtocol("https", new HTTPSSessionInstantiator);
}


HTTPSClientSession::HTTPSClientSession(const std::string& host, CHDBPoco::UInt16 port):
	HTTPClientSession(SecureStreamSocket()),
	_pContext(SSLManager::instance().defaultClientContext())
{
	setHost(host);
	setPort(port);
	_proxySessionFactory.registerProtocol("https", new HTTPSSessionInstantiator);
}


HTTPSClientSession::HTTPSClientSession(Context::Ptr pContext):
	HTTPClientSession(SecureStreamSocket(pContext)),
	_pContext(pContext)
{
	_proxySessionFactory.registerProtocol("https", new HTTPSSessionInstantiator(pContext));
}


HTTPSClientSession::HTTPSClientSession(Context::Ptr pContext, Session::Ptr pSession):
	HTTPClientSession(SecureStreamSocket(pContext, pSession)),
	_pContext(pContext),
	_pSession(pSession)
{
	_proxySessionFactory.registerProtocol("https", new HTTPSSessionInstantiator(pContext));
}


HTTPSClientSession::HTTPSClientSession(const std::string& host, CHDBPoco::UInt16 port, Context::Ptr pContext):
	HTTPClientSession(SecureStreamSocket(pContext)),
	_pContext(pContext)
{
	setHost(host);
	setPort(port);
	_proxySessionFactory.registerProtocol("https", new HTTPSSessionInstantiator(pContext));
}


HTTPSClientSession::HTTPSClientSession(const std::string& host, CHDBPoco::UInt16 port, Context::Ptr pContext, Session::Ptr pSession):
	HTTPClientSession(SecureStreamSocket(pContext, pSession)),
	_pContext(pContext),
	_pSession(pSession)
{
	setHost(host);
	setPort(port);
	_proxySessionFactory.registerProtocol("https", new HTTPSSessionInstantiator(pContext));
}


HTTPSClientSession::~HTTPSClientSession()
{
	_proxySessionFactory.unregisterProtocol("https");
}


bool HTTPSClientSession::secure() const
{
	return true;
}


void HTTPSClientSession::abort()
{
	SecureStreamSocket sss(socket());
	sss.abort();
}


X509Certificate HTTPSClientSession::serverCertificate()
{
	SecureStreamSocket sss(socket());
	return sss.peerCertificate();
}


std::string HTTPSClientSession::proxyRequestPrefix() const
{
	std::string result("https://");
	result.append(getHost());
	/// Do not append default by default, since this may break some servers.
	/// One example of such server is GCS (Google Cloud Storage).
	if (getPort() != HTTPS_PORT)
	{
		result.append(":");
		NumberFormatter::append(result, getPort());
	}
	return result;
}


void HTTPSClientSession::proxyAuthenticate(HTTPRequest& request)
{
}


void HTTPSClientSession::connect(const SocketAddress& address)
{
	bool useProxy = !getProxyHost().empty() && !bypassProxy();
	
	if (useProxy && isProxyTunnel())
	{
		StreamSocket proxySocket(proxyConnect());
		SecureStreamSocket secureSocket = SecureStreamSocket::attach(proxySocket, getHost(), _pContext, _pSession);
		attachSocket(secureSocket);
		if (_pContext->sessionCacheEnabled())
		{
			_pSession = secureSocket.currentSession();
		}
	}
	else
	{
		SecureStreamSocket sss(socket());
		if (sss.getPeerHostName().empty())
		{
			sss.setPeerHostName(useProxy ? getProxyHost() : getHost());
		}
		if (_pContext->sessionCacheEnabled())
		{
			sss.useSession(_pSession);
		}
		HTTPSession::connect(address);
		if (_pContext->sessionCacheEnabled())
		{
			_pSession = sss.currentSession();
		}
	}
}


int HTTPSClientSession::read(char* buffer, std::streamsize length)
{
	try
	{
		return HTTPSession::read(buffer, length);
	}
	catch(SSLConnectionUnexpectedlyClosedException&)
	{
		return 0;
	}
}


Session::Ptr HTTPSClientSession::sslSession()
{
	return _pSession;
}


} } // namespace CHDBPoco::Net
