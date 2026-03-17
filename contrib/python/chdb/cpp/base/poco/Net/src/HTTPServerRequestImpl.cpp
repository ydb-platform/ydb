//
// HTTPServerRequestImpl.cpp
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServerRequestImpl
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/HTTPServerRequestImpl.h"
#include "CHDBPoco/Net/HTTPServerResponseImpl.h"
#include "CHDBPoco/Net/HTTPServerSession.h"
#include "CHDBPoco/Net/HTTPHeaderStream.h"
#include "CHDBPoco/Net/HTTPStream.h"
#include "CHDBPoco/Net/HTTPFixedLengthStream.h"
#include "CHDBPoco/Net/HTTPChunkedStream.h"
#include "CHDBPoco/Net/HTTPServerParams.h"
#include "CHDBPoco/Net/StreamSocket.h"
#include "CHDBPoco/String.h"


using CHDBPoco::icompare;


namespace CHDBPoco {
namespace Net {


HTTPServerRequestImpl::HTTPServerRequestImpl(HTTPServerResponseImpl& response, HTTPServerSession& session, HTTPServerParams* pParams):
	_response(response),
	_session(session),
	_pStream(0),
	_pParams(pParams, true)
{
	response.attachRequest(this);

	HTTPHeaderInputStream hs(session);
	read(hs);
	
	// Now that we know socket is still connected, obtain addresses
	_clientAddress = session.clientAddress();
	_serverAddress = session.serverAddress();
	
	if (getChunkedTransferEncoding())
		_pStream = new HTTPChunkedInputStream(session);
	else if (hasContentLength())
		_pStream = new HTTPFixedLengthInputStream(session, getContentLength64());
	else if (getMethod() == HTTPRequest::HTTP_GET || getMethod() == HTTPRequest::HTTP_HEAD || getMethod() == HTTPRequest::HTTP_DELETE)
		_pStream = new HTTPFixedLengthInputStream(session, 0);
	else
		_pStream = new HTTPInputStream(session);
}


HTTPServerRequestImpl::~HTTPServerRequestImpl()
{
	delete _pStream;
}


bool HTTPServerRequestImpl::secure() const
{
	return _session.socket().secure();
}


StreamSocket& HTTPServerRequestImpl::socket()
{
	return _session.socket();
}


StreamSocket HTTPServerRequestImpl::detachSocket()
{
	return _session.detachSocket();
}


} } // namespace CHDBPoco::Net
