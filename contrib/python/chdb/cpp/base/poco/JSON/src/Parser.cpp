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


#include "CHDBPoco/JSON/Parser.h"
#include "CHDBPoco/JSON/JSONException.h"
#include "CHDBPoco/Ascii.h"
#include "CHDBPoco/Token.h"
#include "CHDBPoco/UTF8Encoding.h"
#include "CHDBPoco/String.h"
#undef min
#undef max
#include <limits>
#include <clocale>
#include <istream>


namespace CHDBPoco {
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


} } // namespace CHDBPoco::JSON
