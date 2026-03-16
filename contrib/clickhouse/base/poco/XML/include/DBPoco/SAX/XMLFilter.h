//
// XMLFilter.h
//
// Library: XML
// Package: SAX
// Module:  SAXFilters
//
// SAX2 XMLFilter Interface.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_SAX_XMLFilter_INCLUDED
#define DB_SAX_XMLFilter_INCLUDED


#include "DBPoco/SAX/XMLReader.h"
#include "DBPoco/XML/XML.h"


namespace DBPoco
{
namespace XML
{


    class XML_API XMLFilter : public XMLReader
    /// Interface for an XML filter.
    ///
    /// An XML filter is like an XML reader, except that it obtains its events from another XML reader
    /// rather than a primary source like an XML document or database. Filters can modify a stream of
    /// events as they pass on to the final application.
    ///
    /// The XMLFilterImpl helper class provides a convenient base for creating SAX2 filters, by passing on
    /// all EntityResolver, DTDHandler, ContentHandler and ErrorHandler events automatically.
    {
    public:
        virtual XMLReader * getParent() const = 0;
        /// Set the parent reader.
        ///
        /// This method allows the application to link the filter to a parent reader (which may be another
        /// filter). The argument may not be null.

        virtual void setParent(XMLReader * pParent) = 0;
        /// Get the parent reader.
        ///
        /// This method allows the application to query the parent reader (which may be another filter).
        /// It is generally a bad idea to perform any operations on the parent reader directly: they should
        /// all pass through this filter.

    protected:
        virtual ~XMLFilter();
    };


}
} // namespace DBPoco::XML


#endif // DB_SAX_XMLFilter_INCLUDED
