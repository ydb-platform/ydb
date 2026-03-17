//
// XMLStreamParserException.h
//
// Library: XML
// Package: XML
// Module:  XMLStreamParserException
//
// Definition of the XMLStreamParserException class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_XML_XMLStreamParserException_INCLUDED
#define CHDB_XML_XMLStreamParserException_INCLUDED


#include "CHDBPoco/XML/XMLException.h"


namespace CHDBPoco
{
namespace XML
{


    class XMLStreamParser;


    class XML_API XMLStreamParserException : public CHDBPoco::XML::XMLException
    {
    public:
        XMLStreamParserException(const std::string & name, CHDBPoco::UInt64 line, CHDBPoco::UInt64 column, const std::string & description);
        XMLStreamParserException(const XMLStreamParser &, const std::string & description);
        virtual ~XMLStreamParserException() throw();

        const char * name() const noexcept;
        CHDBPoco::UInt64 line() const;
        CHDBPoco::UInt64 column() const;
        const std::string & description() const;
        virtual const char * what() const throw();

    private:
        void init();

        std::string _name;
        CHDBPoco::UInt64 _line;
        CHDBPoco::UInt64 _column;
        std::string _description;
        std::string _what;
    };


}
} // namespace CHDBPoco::XML


#endif // CHDB_XML_XMLStreamParserException_INCLUDED
