//
// ValueTraits.cpp
//
// Library: XML
// Package: XML
// Module:  ValueTraits
//
// Definition of the ValueTraits templates.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// Based on libstudxml (http://www.codesynthesis.com/projects/libstudxml/).
// Copyright (c) 2009-2013 Code Synthesis Tools CC.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/XML/XMLStreamParser.h"
#include "CHDBPoco/XML/XMLStreamParserException.h"


namespace CHDBPoco {
namespace XML {


bool DefaultValueTraits<bool>::parse(std::string s, const XMLStreamParser& p)
{
	if (s == "true" || s == "1" || s == "True" || s == "TRUE")
		return true;
	else if (s == "false" || s == "0" || s == "False" || s == "FALSE")
		return false;
	else
		throw XMLStreamParserException(p, "invalid bool value '" + s + "'");
}


} } // namespace CHDBPoco::XML
