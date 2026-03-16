//
// HTTPServerResponseImpl.cpp
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServerResponseImpl
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/HTTPServerResponseImpl.h"
#include "CHDBPoco/Net/HTTPServerRequestImpl.h"
#include "CHDBPoco/Net/HTTPServerSession.h"
#include "CHDBPoco/Net/HTTPHeaderStream.h"
#include "CHDBPoco/Net/HTTPStream.h"
#include "CHDBPoco/Net/HTTPFixedLengthStream.h"
#include "CHDBPoco/Net/HTTPChunkedStream.h"
#include "CHDBPoco/File.h"
#include "CHDBPoco/Timestamp.h"
#include "CHDBPoco/NumberFormatter.h"
#include "CHDBPoco/StreamCopier.h"
#include "CHDBPoco/CountingStream.h"
#include "CHDBPoco/Exception.h"
#include "CHDBPoco/FileStream.h"
#include "CHDBPoco/DateTimeFormatter.h"
#include "CHDBPoco/DateTimeFormat.h"


using CHDBPoco::File;
using CHDBPoco::Timestamp;
using CHDBPoco::NumberFormatter;
using CHDBPoco::StreamCopier;
using CHDBPoco::OpenFileException;
using CHDBPoco::DateTimeFormatter;
using CHDBPoco::DateTimeFormat;


namespace CHDBPoco {
namespace Net {


HTTPServerResponseImpl::HTTPServerResponseImpl(HTTPServerSession& session):
	_session(session),
	_pRequest(0),
	_pStream(0),
	_pHeaderStream(0)
{
}


HTTPServerResponseImpl::~HTTPServerResponseImpl()
{
	if (_pHeaderStream && _pHeaderStream != _pStream)
		delete _pHeaderStream;
	if (_pStream)
		delete _pStream;
}


void HTTPServerResponseImpl::sendContinue()
{
	HTTPHeaderOutputStream hs(_session);
	hs << getVersion() << " 100 Continue\r\n\r\n";
}


std::ostream& HTTPServerResponseImpl::send()
{
	CHDB_poco_assert (!_pStream);

	if ((_pRequest && _pRequest->getMethod() == HTTPRequest::HTTP_HEAD) ||
		getStatus() < 200 ||
		getStatus() == HTTPResponse::HTTP_NO_CONTENT ||
		getStatus() == HTTPResponse::HTTP_NOT_MODIFIED)
	{
		CHDBPoco::CountingOutputStream cs;
		write(cs);
		_pStream = new HTTPFixedLengthOutputStream(_session, cs.chars());
		write(*_pStream);
	}
	else if (getChunkedTransferEncoding())
	{
		HTTPHeaderOutputStream hs(_session);
		write(hs);
		_pStream = new HTTPChunkedOutputStream(_session);
	}
	else if (hasContentLength())
	{
		CHDBPoco::CountingOutputStream cs;
		write(cs);
		_pStream = new HTTPFixedLengthOutputStream(_session, getContentLength64() + cs.chars());
		write(*_pStream);
	}
	else
	{
		_pStream = new HTTPOutputStream(_session);
		setKeepAlive(false);
		write(*_pStream);
	}
	return *_pStream;
}


std::pair<std::ostream *, std::ostream *> HTTPServerResponseImpl::beginSend()
{
	CHDB_poco_assert (!_pStream);
	CHDB_poco_assert (!_pHeaderStream);

	// NOTE Code is not exception safe.

	if ((_pRequest && _pRequest->getMethod() == HTTPRequest::HTTP_HEAD) ||
		getStatus() < 200 ||
		getStatus() == HTTPResponse::HTTP_NO_CONTENT ||
		getStatus() == HTTPResponse::HTTP_NOT_MODIFIED)
	{
		throw Exception("HTTPServerResponse::beginSend is invalid for HEAD request");
	}
	else if (getChunkedTransferEncoding())
	{
		_pHeaderStream = new HTTPHeaderOutputStream(_session);
		beginWrite(*_pHeaderStream);
		_pStream = new HTTPChunkedOutputStream(_session);
	}
	else if (hasContentLength())
	{
		throw Exception("HTTPServerResponse::beginSend is invalid for response with Content-Length header");
	}
	else
	{
		_pStream = new HTTPOutputStream(_session);
		_pHeaderStream = _pStream;
		setKeepAlive(false);
		beginWrite(*_pStream);
	}

	return std::make_pair(_pHeaderStream, _pStream);
}


void HTTPServerResponseImpl::sendFile(const std::string& path, const std::string& mediaType)
{
	CHDB_poco_assert (!_pStream);

	File f(path);
	Timestamp dateTime    = f.getLastModified();
	File::FileSize length = f.getSize();
	set("Last-Modified", DateTimeFormatter::format(dateTime, DateTimeFormat::HTTP_FORMAT));
	setContentLength64(length);
	setContentType(mediaType);
	setChunkedTransferEncoding(false);

	CHDBPoco::FileInputStream istr(path);
	if (istr.good())
	{
		_pStream = new HTTPHeaderOutputStream(_session);
		write(*_pStream);
		if (_pRequest && _pRequest->getMethod() != HTTPRequest::HTTP_HEAD)
		{
			StreamCopier::copyStream(istr, *_pStream);
		}
	}
	else throw OpenFileException(path);
}


void HTTPServerResponseImpl::sendBuffer(const void* pBuffer, std::size_t length)
{
	CHDB_poco_assert (!_pStream);

	setContentLength(static_cast<int>(length));
	setChunkedTransferEncoding(false);
	
	_pStream = new HTTPHeaderOutputStream(_session);
	write(*_pStream);
	if (_pRequest && _pRequest->getMethod() != HTTPRequest::HTTP_HEAD)
	{
		_pStream->write(static_cast<const char*>(pBuffer), static_cast<std::streamsize>(length));
	}
}


void HTTPServerResponseImpl::redirect(const std::string& uri, HTTPStatus status)
{
	CHDB_poco_assert (!_pStream);

	setContentLength(0);
	setChunkedTransferEncoding(false);

	setStatusAndReason(status);
	set("Location", uri);

	_pStream = new HTTPHeaderOutputStream(_session);
	write(*_pStream);
}


void HTTPServerResponseImpl::requireAuthentication(const std::string& realm)
{
	CHDB_poco_assert (!_pStream);
	
	setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
	std::string auth("Basic realm=\"");
	auth.append(realm);
	auth.append("\"");
	set("WWW-Authenticate", auth);
}


} } // namespace CHDBPoco::Net
