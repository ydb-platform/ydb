//
// XMLException.h
//
// Library: XML
// Package: XML
// Module:  XMLException
//
// Definition of the XMLException class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_XML_XMLException_INCLUDED
#define DB_XML_XMLException_INCLUDED


#include "DBPoco/Exception.h"
#include "DBPoco/XML/XML.h"


namespace DBPoco
{
namespace XML
{


    DB_POCO_DECLARE_EXCEPTION(XML_API, XMLException, DBPoco::RuntimeException)
    /// The base class for all XML-related exceptions like SAXException
    /// and DOMException.


}
} // namespace DBPoco::XML


#endif // DB_XML_XMLException_INCLUDED
