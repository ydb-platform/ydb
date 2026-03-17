//
// Parser.cpp
//
// Library: JSON
// Package: JSON
// Module:  Parser
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/JSON/Parser.h"
#include "DBPoco/JSON/JSONException.h"
#include "DBPoco/Ascii.h"
#include "DBPoco/Token.h"
#include "DBPoco/UTF8Encoding.h"
#include "DBPoco/String.h"
#undef min
#undef max
#include <limits>
#include <clocale>
#include <istream>


namespace DBPoco {
namespace JSON {


Parser::Parser(const Handler::Ptr& pHandler, std::size_t bufSize):
	ParserImpl(pHandler, bufSize)
{
}


Parser::~Parser()
{
}


void Parser::setHandler(const Handler::Ptr& pHandler)
{
	setHandlerImpl(pHandler);
}


} } // namespace DBPoco::JSON
