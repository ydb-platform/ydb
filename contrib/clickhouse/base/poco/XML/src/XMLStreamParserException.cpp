//
// XMLStreamParserException.cpp
//
// Library: XML
// Package: XML
// Module:  XMLStreamParserException
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/XML/XMLStreamParserException.h"
#include "DBPoco/XML/XMLStreamParser.h"


namespace DBPoco {
namespace XML {


XMLStreamParserException::~XMLStreamParserException() throw ()
{
}


XMLStreamParserException::XMLStreamParserException(const std::string& n, DBPoco::UInt64 l, DBPoco::UInt64 c, const std::string& d):
	_name(n),
	_line(l),
	_column(c),
	_description(d)
{
	init();
}


XMLStreamParserException::XMLStreamParserException(const XMLStreamParser& p, const std::string& d):
	_name(p.inputName()),
	_line(p.line()),
	_column(p.column()),
	_description(d)
{
	init();
}


void XMLStreamParserException::init()
{
	std::ostringstream os;
	if (!_name.empty())
		os << _name << ':';
	os << _line << ':' << _column << ": error: " << _description;
	_what = os.str();
}


const char* XMLStreamParserException::name() const noexcept
{
	return _name.c_str();
}


DBPoco::UInt64 XMLStreamParserException::line() const
{
	return _line;
}


DBPoco::UInt64 XMLStreamParserException::column() const
{
	return _column;
}


const std::string& XMLStreamParserException::description() const
{
	return _description;
}


char const* XMLStreamParserException::what() const throw ()
{
	return _what.c_str();
}


} } // namespace DBPoco::XML
