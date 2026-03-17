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


#ifndef CHDB_XML_XMLException_INCLUDED
#define CHDB_XML_XMLException_INCLUDED


#include "CHDBPoco/Exception.h"
#include "CHDBPoco/XML/XML.h"


namespace CHDBPoco
{
namespace XML
{


    CHDB_POCO_DECLARE_EXCEPTION(XML_API, XMLException, CHDBPoco::RuntimeException)
    /// The base class for all XML-related exceptions like SAXException
    /// and DOMException.


}
} // namespace CHDBPoco::XML


#endif // CHDB_XML_XMLException_INCLUDED
