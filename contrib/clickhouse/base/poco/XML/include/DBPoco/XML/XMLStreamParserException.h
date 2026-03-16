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


#ifndef DB_XML_XMLStreamParserException_INCLUDED
#define DB_XML_XMLStreamParserException_INCLUDED


#include "DBPoco/XML/XMLException.h"


namespace DBPoco
{
namespace XML
{


    class XMLStreamParser;


    class XML_API XMLStreamParserException : public DBPoco::XML::XMLException
    {
    public:
        XMLStreamParserException(const std::string & name, DBPoco::UInt64 line, DBPoco::UInt64 column, const std::string & description);
        XMLStreamParserException(const XMLStreamParser &, const std::string & description);
        virtual ~XMLStreamParserException() throw();

        const char * name() const noexcept;
        DBPoco::UInt64 line() const;
        DBPoco::UInt64 column() const;
        const std::string & description() const;
        virtual const char * what() const throw();

    private:
        void init();

        std::string _name;
        DBPoco::UInt64 _line;
        DBPoco::UInt64 _column;
        std::string _description;
        std::string _what;
    };


}
} // namespace DBPoco::XML


#endif // DB_XML_XMLStreamParserException_INCLUDED
