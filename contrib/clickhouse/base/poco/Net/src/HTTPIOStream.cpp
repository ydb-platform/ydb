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


#include "DBPoco/Net/HTTPIOStream.h"
#include "DBPoco/Net/HTTPClientSession.h"


using DBPoco::UnbufferedStreamBuf;


namespace DBPoco {
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
	DB_poco_ios_init(&_buf);
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


} } // namespace DBPoco::Net
