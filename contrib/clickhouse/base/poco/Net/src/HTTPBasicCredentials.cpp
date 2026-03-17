//
// HTTPBasicCredentials.cpp
//
// Library: Net
// Package: HTTP
// Module:  HTTPBasicCredentials
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/HTTPBasicCredentials.h"
#include "DBPoco/Net/HTTPRequest.h"
#include "DBPoco/Net/NetException.h"
#include "DBPoco/Base64Encoder.h"
#include "DBPoco/Base64Decoder.h"
#include "DBPoco/String.h"
#include <sstream>


using DBPoco::Base64Decoder;
using DBPoco::Base64Encoder;
using DBPoco::icompare;


namespace DBPoco {
namespace Net {


const std::string HTTPBasicCredentials::SCHEME = "Basic";


HTTPBasicCredentials::HTTPBasicCredentials()
{
}


HTTPBasicCredentials::HTTPBasicCredentials(const std::string& username, const std::string& password):
	_username(username),
	_password(password)
{
}


HTTPBasicCredentials::HTTPBasicCredentials(const HTTPRequest& request)
{
	std::string scheme;
	std::string authInfo;
	request.getCredentials(scheme, authInfo);
	if (icompare(scheme, SCHEME) == 0)
	{
		parseAuthInfo(authInfo);
	}
	else throw NotAuthenticatedException("Basic authentication expected");
}


HTTPBasicCredentials::HTTPBasicCredentials(const std::string& authInfo)
{
	parseAuthInfo(authInfo);
}


HTTPBasicCredentials::~HTTPBasicCredentials()
{
}


void HTTPBasicCredentials::clear()
{
	_username.clear();
	_password.clear();
}


void HTTPBasicCredentials::setUsername(const std::string& username)
{
	_username = username;
}


void HTTPBasicCredentials::setPassword(const std::string& password)
{
	_password = password;
}


void HTTPBasicCredentials::authenticate(HTTPRequest& request) const
{
	std::ostringstream ostr;
	Base64Encoder encoder(ostr);
	encoder.rdbuf()->setLineLength(0);
	encoder << _username << ":" << _password;
	encoder.close();
	request.setCredentials(SCHEME, ostr.str());
}


void HTTPBasicCredentials::proxyAuthenticate(HTTPRequest& request) const
{
	std::ostringstream ostr;
	Base64Encoder encoder(ostr);
	encoder.rdbuf()->setLineLength(0);
	encoder << _username << ":" << _password;
	encoder.close();
	request.setProxyCredentials(SCHEME, ostr.str());
}


void HTTPBasicCredentials::parseAuthInfo(const std::string& authInfo)
{
	static const int eof = std::char_traits<char>::eof();

	std::istringstream istr(authInfo);
	Base64Decoder decoder(istr);
	int ch = decoder.get();
	while (ch != eof && ch != ':')
	{
		_username += (char) ch;
		ch = decoder.get();
	}
	if (ch == ':') ch = decoder.get();
	while (ch != eof)
	{
		_password += (char) ch;
		ch = decoder.get();
	}
}


} } // namespace DBPoco::Net
