//
// DOMWriter.h
//
// Library: XML
// Package: DOM
// Module:  DOMWriter
//
// Definition of class DOMWriter.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_DOM_DOMWriter_INCLUDED
#define DB_DOM_DOMWriter_INCLUDED


#include "DBPoco/TextEncoding.h"
#include "DBPoco/XML/XML.h"
#include "DBPoco/XML/XMLStream.h"
#include "DBPoco/XML/XMLString.h"


namespace DBPoco
{
namespace XML
{


    class Node;
    class Document;


    class XML_API DOMWriter
    /// The DOMWriter uses a DOMSerializer with an
    /// XMLWriter to serialize a DOM document into
    /// textual XML.
    {
    public:
        DOMWriter();
        /// Creates a DOMWriter.

        ~DOMWriter();
        /// Destroys a DOMWriter.

        void setEncoding(const std::string & encodingName, DBPoco::TextEncoding & textEncoding);
        /// Sets the encoding, which will be reflected in the written XML declaration.

        const std::string & getEncoding() const;
        /// Returns the encoding name set with setEncoding.

        void setOptions(int options);
        /// Sets options for the internal XMLWriter.
        ///
        /// See class XMLWriter for available options.

        int getOptions() const;
        /// Returns the options for the internal XMLWriter.

        void setNewLine(const std::string & newLine);
        /// Sets the line ending characters for the internal
        /// XMLWriter. See XMLWriter::setNewLine() for a list
        /// of supported values.

        const std::string & getNewLine() const;
        /// Returns the line ending characters used by the
        /// internal XMLWriter.

        void setIndent(const std::string & indent);
        /// Sets the string used for one indentation step.
        ///
        /// The default is a single TAB character.
        /// The given string should only contain TAB or SPACE
        /// characters (e.g., a single TAB character, or
        /// two to four SPACE characters).

        const std::string & getIndent() const;
        /// Returns the string used for one indentation step.

        void writeNode(XMLByteOutputStream & ostr, const Node * pNode);
        /// Writes the XML for the given node to the specified stream.

        void writeNode(const std::string & systemId, const Node * pNode);
        /// Writes the XML for the given node to the file specified in systemId,
        /// using a standard file output stream (DBPoco::FileOutputStream).

    private:
        std::string _encodingName;
        DBPoco::TextEncoding * _pTextEncoding;
        int _options;
        std::string _newLine;
        std::string _indent;
    };


    //
    // inlines
    //
    inline const std::string & DOMWriter::getEncoding() const
    {
        return _encodingName;
    }


    inline int DOMWriter::getOptions() const
    {
        return _options;
    }


    inline const std::string & DOMWriter::getNewLine() const
    {
        return _newLine;
    }


    inline const std::string & DOMWriter::getIndent() const
    {
        return _indent;
    }


}
} // namespace DBPoco::XML


#endif // DB_DOM_DOMWriter_INCLUDED
