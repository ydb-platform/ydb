//
// HTTPIOStream.cpp
//
// Library: Net
// Package: HTTP
// Module:  HTTPIOStream
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/HTTPIOStream.h"
#include "CHDBPoco/Net/HTTPClientSession.h"


using CHDBPoco::UnbufferedStreamBuf;


namespace CHDBPoco {
namespace Net {


HTTPResponseStreamBuf::HTTPResponseStreamBuf(std::istream& istr):
	_istr(istr)
{
	// make sure exceptions from underlying string propagate
	_istr.exceptions(std::ios::badbit);
}


HTTPResponseStreamBuf::~HTTPResponseStreamBuf()
{
}


HTTPResponseIOS::HTTPResponseIOS(std::istream& istr):
	_buf(istr)
{
	CHDB_poco_ios_init(&_buf);
}


HTTPResponseIOS::~HTTPResponseIOS()
{
}


HTTPResponseStream::HTTPResponseStream(std::istream& istr, HTTPClientSession* pSession):
	HTTPResponseIOS(istr),
	std::istream(&_buf),
	_pSession(pSession)
{
}


HTTPResponseStream::~HTTPResponseStream()
{
	delete _pSession;
}


} } // namespace CHDBPoco::Net
