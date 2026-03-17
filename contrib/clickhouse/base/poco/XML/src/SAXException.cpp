//
// SAXException.cpp
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/SAX/SAXException.h"
#include "DBPoco/SAX/Locator.h"
#include <typeinfo>
#include <sstream>


namespace DBPoco {
namespace XML {


DB_POCO_IMPLEMENT_EXCEPTION(SAXException, XMLException, "SAX Exception")
DB_POCO_IMPLEMENT_EXCEPTION(SAXNotRecognizedException, SAXException, "Unrecognized SAX feature or property identifier")
DB_POCO_IMPLEMENT_EXCEPTION(SAXNotSupportedException, SAXException, "Unsupported SAX feature or property identifier")


SAXParseException::SAXParseException(const std::string& msg, const Locator& loc):
	SAXException(buildMessage(msg, loc.getPublicId(), loc.getSystemId(), loc.getLineNumber(), loc.getColumnNumber())),
	_publicId(loc.getPublicId()),
	_systemId(loc.getSystemId()),
	_lineNumber(loc.getLineNumber()),
	_columnNumber(loc.getColumnNumber())
{
}


SAXParseException::SAXParseException(const std::string& msg, const Locator& loc, const DBPoco::Exception& exc):
	SAXException(buildMessage(msg, loc.getPublicId(), loc.getSystemId(), loc.getLineNumber(), loc.getColumnNumber()), exc),
	_publicId(loc.getPublicId()),
	_systemId(loc.getSystemId()),
	_lineNumber(loc.getLineNumber()),
	_columnNumber(loc.getColumnNumber())
{
}

	
SAXParseException::SAXParseException(const std::string& msg, const XMLString& publicId, const XMLString& systemId, int lineNumber, int columnNumber):
	SAXException(buildMessage(msg, publicId, systemId, lineNumber, columnNumber)),
	_publicId(publicId),
	_systemId(systemId),
	_lineNumber(lineNumber),
	_columnNumber(columnNumber)
{
}


SAXParseException::SAXParseException(const std::string& msg, const XMLString& publicId, const XMLString& systemId, int lineNumber, int columnNumber, const DBPoco::Exception& exc):
	SAXException(buildMessage(msg, publicId, systemId, lineNumber, columnNumber), exc),
	_publicId(publicId),
	_systemId(systemId),
	_lineNumber(lineNumber),
	_columnNumber(columnNumber)
{
}


SAXParseException::SAXParseException(const SAXParseException& exc):
	SAXException(exc),
	_publicId(exc._publicId),
	_systemId(exc._systemId),
	_lineNumber(exc._lineNumber),
	_columnNumber(exc._columnNumber)
{
}


SAXParseException::~SAXParseException() noexcept
{
}


SAXParseException& SAXParseException::operator = (const SAXParseException& exc)
{
	if (&exc != this)
	{
		SAXException::operator = (exc);
		_publicId     = exc._publicId;
		_systemId     = exc._systemId;
		_lineNumber   = exc._lineNumber;
		_columnNumber = exc._columnNumber;
	}
	return *this;
}

	
const char* SAXParseException::name() const noexcept
{
	return "SAXParseException";
}


const char* SAXParseException::className() const noexcept
{
	return typeid(*this).name();
}


DBPoco::Exception* SAXParseException::clone() const
{
	return new SAXParseException(*this);
}


void SAXParseException::rethrow() const
{
	throw *this;
}


std::string SAXParseException::buildMessage(const std::string& msg, const XMLString& publicId, const XMLString& systemId, int lineNumber, int columnNumber)
{
	std::ostringstream result;
	if (!msg.empty()) result << msg << " ";
	result << "in ";
	if (!systemId.empty())
		result << "'" << fromXMLString(systemId) << "', ";
	else if (!publicId.empty())
		result << "'" << fromXMLString(publicId) << "', ";
	if (lineNumber > 0)
		result << "line " << lineNumber << " column " << columnNumber;
	return result.str();
}


} } // namespace DBPoco::XML
