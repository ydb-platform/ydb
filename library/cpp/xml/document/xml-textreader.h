#pragma once

#include "xml-document.h"
#include "xml-options.h"

#include <contrib/libs/libxml/include/libxml/xmlreader.h>

#include <library/cpp/string_utils/ztstrbuf/ztstrbuf.h>

#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <functional>
#include <util/stream/input.h>
#include <util/stream/str.h>

namespace NXml {
    /**
     * TextReader Parser
     *
     * API of the XML streaming API based on C# interfaces.
     * Provides fast, non-cached, forward-only access to XML data.
     *
     * Like the SAX parser, the TextReader parser is suitable for sequential
     * parsing, but instead of implementing handlers for specific parts of the
     * document, it allows you to detect the current node type, process the node
     * accordingly, and skip forward in the document as much as necessary.
     *
     * Unlike the DOM parser, you may not move backwards in the XML document.
     * And unlike the SAX parser, you must not waste time processing nodes that do not
     * interest you.
     *
     * All methods are on the single parser instance, but their result depends on the current context.
     * For instance, use Read() to move to the next node, and MoveToElement() to navigate to child nodes.
     * These methods will return false when no more nodes are available. Then use
     * methods such as GetName() and GetValue() to examine the elements and their attributes.
     *
     * This wrapper is inspired by TextReader from libxml++.
     */

    class TTextReader: private TNonCopyable {
    public:
        // strongly-typed alias for enum from xmlreader.h
        enum class ENodeType : int {
            // clang-format off
            Attribute             = XML_READER_TYPE_ATTRIBUTE,
            CDATA                 = XML_READER_TYPE_CDATA,
            Comment               = XML_READER_TYPE_COMMENT,
            Document              = XML_READER_TYPE_DOCUMENT,
            DocumentFragment      = XML_READER_TYPE_DOCUMENT_FRAGMENT,
            DocumentType          = XML_READER_TYPE_DOCUMENT_TYPE,
            Element               = XML_READER_TYPE_ELEMENT,
            EndElement            = XML_READER_TYPE_END_ELEMENT,
            EndEntity             = XML_READER_TYPE_END_ENTITY,
            Entity                = XML_READER_TYPE_ENTITY,
            EntityReference       = XML_READER_TYPE_ENTITY_REFERENCE,
            None                  = XML_READER_TYPE_NONE,
            Notation              = XML_READER_TYPE_NOTATION,
            ProcessingInstruction = XML_READER_TYPE_PROCESSING_INSTRUCTION,
            SignificantWhitespace = XML_READER_TYPE_SIGNIFICANT_WHITESPACE,
            Text                  = XML_READER_TYPE_TEXT,
            Whitespace            = XML_READER_TYPE_WHITESPACE,
            XmlDeclaration        = XML_READER_TYPE_XML_DECLARATION,
            // clang-format on
        };

        enum class EReadState : int {
            // clang-format off
            Closed      = XML_TEXTREADER_MODE_CLOSED,
            EndOfFile   = XML_TEXTREADER_MODE_EOF,
            Error       = XML_TEXTREADER_MODE_ERROR,
            Initial     = XML_TEXTREADER_MODE_INITIAL,
            Interactive = XML_TEXTREADER_MODE_INTERACTIVE,
            Reading     = XML_TEXTREADER_MODE_READING,
            // clang-format on
        };

    public:
        TTextReader(IInputStream& stream, const TOptions& options = TOptions());
        ~TTextReader();

        /**
         * Moves the position of the current instance to the next node in the stream, exposing its properties.
         * @return true if the node was read successfully, false if there are no more nodes to read
         */
        bool Read();

        /**
         * Reads the contents of the current node, including child nodes and markup.
         * @return A string containing the XML content, or an empty string
         *         if the current node is neither an element nor attribute, or has no child nodes
         */
        TString ReadInnerXml() const;

        /**
         * Reads the current node and its contents, including child nodes and markup.
         * @return A string containing the XML content, or an empty string
         *         if the current node is neither an element nor attribute
         */
        TString ReadOuterXml() const;

        /**
         * Reads the contents of an element or a text node as a string.
         * @return A string containing the contents of the Element or Text node,
         *         or an empty string if the reader is positioned on any other type of node
         */
        TString ReadString() const;

        /**
         * Parses an attribute value into one or more Text and EntityReference nodes.
         * @return A bool where true indicates the attribute value was parsed,
         *         and false indicates the reader was not positioned on an attribute node
         *         or all the attribute values have been read
         */
        bool ReadAttributeValue() const;

        /**
         * Gets the number of attributes on the current node.
         * @return The number of attributes on the current node, or zero if the current node
         *         does not support attributes
         */
        int GetAttributeCount() const;

        /**
         * Gets the base Uniform Resource Identifier (URI) of the current node.
         * @return The base URI of the current node or an empty string if not available
         */
        TStringBuf GetBaseUri() const;

        /**
         * Gets the depth of the current node in the XML document.
         * @return The depth of the current node in the XML document
         */
        int GetDepth() const;

        /**
         * Gets a value indicating whether the current node has any attributes.
         * @return true if the current has attributes, false otherwise
         */
        bool HasAttributes() const;

        /**
         * Whether the node can have a text value.
         * @return true if the current node can have an associated text value, false otherwise
         */
        bool HasValue() const;

        /**
         * Whether an Attribute node was generated from the default value defined in the DTD or schema.
         * @return true if defaulted, false otherwise
         */
        bool IsDefault() const;

        /**
         * Check if the current node is empty.
         * @return true if empty, false otherwise
         */
        bool IsEmptyElement() const;

        /**
         * The local name of the node.
         * @return the local name or empty string if not available
         */
        TStringBuf GetLocalName() const;

        /**
         * The qualified name of the node, equal to Prefix:LocalName.
         * @return the name or empty string if not available
         */
        TStringBuf GetName() const;

        /**
         * The URI defining the namespace associated with the node.
         * @return the namespace URI or empty string if not available
         */
        TStringBuf GetNamespaceUri() const;

        /**
         * Get the node type of the current node.
         * @return the ENodeType of the current node
         */
        ENodeType GetNodeType() const;

        /**
         * Get the namespace prefix associated with the current node.
         * @return the namespace prefix, or an empty string if not available
         */
        TStringBuf GetPrefix() const;

        /**
         * Get the quotation mark character used to enclose the value of an attribute.
         * @return " or '
         */
        char GetQuoteChar() const;

        /**
         * Provides the text value of the node if present.
         * @return the string or empty if not available
         */
        TStringBuf GetValue() const;

        /**
         * Gets the read state of the reader.
         * @return the state value
         */
        EReadState GetReadState() const;

        /**
         * This method releases any resources allocated by the current instance
         * changes the state to Closed and close any underlying input.
         */
        void Close();

        /**
         * Provides the value of the attribute with the specified index relative to the containing element.
         * @param number the zero-based index of the attribute relative to the containing element
         */
        TString GetAttribute(int number) const;

        /**
         * Provides the value of the attribute with the specified qualified name.
         * @param name the qualified name of the attribute
         */
        TString GetAttribute(TZtStringBuf name) const;

        /**
         * Provides the value of the specified attribute.
         * @param localName the local name of the attribute
         * @param nsUri the namespace URI of the attribute
         */
        TString GetAttribute(TZtStringBuf localName, TZtStringBuf nsUri) const;

        /**
         * Resolves a namespace prefix in the scope of the current element.
         * @param prefix the prefix whose namespace URI is to be resolved. To return the default namespace, specify empty string.
         * @return a string containing the namespace URI to which the prefix maps.
         */
        TString LookupNamespace(TZtStringBuf prefix) const;

        /**
         * Moves the position of the current instance to the attribute with the specified index relative to the containing element.
         * @param number the zero-based index of the attribute relative to the containing element
         * @return true in case of success, false if not found
         */
        bool MoveToAttribute(int number);

        /**
         * Moves the position of the current instance to the attribute with the specified qualified name.
         * @param name the qualified name of the attribute
         * @return true in case of success, false if not found
         */
        bool MoveToAttribute(TZtStringBuf name);

        /**
         * Moves the position of the current instance to the attribute with the specified local name and namespace URI.
         * @param localName the local name of the attribute
         * @param nsUri the namespace URI of the attribute
         * @return true in case of success, false if not found
         */
        bool MoveToAttribute(TZtStringBuf localName, TZtStringBuf nsUri);

        /**
         * Moves the position of the current instance to the first attribute associated with the current node.
         * @return true in case of success, false if not found
         */
        bool MoveToFirstAttribute();

        /**
         * Moves the position of the current instance to the next attribute associated with the current node.
         * @return true in case of success, false if not found
         */
        bool MoveToNextAttribute();

        /**
         * Moves the position of the current instance to the node that contains the current Attribute node.
         * @return true in case of success, false if not found
         */
        bool MoveToElement();

        /**
         * Reads the contents of the current node and the full subtree. It then makes the subtree available until the next Read() call.
         */
        TConstNode Expand() const;

        /**
         * Skip to the node following the current one in document order while avoiding the subtree if any.
         * @return true if the node was read successfully, false if there is no more nodes to read
         */
        bool Next();

        /**
         * Retrieve the validity status from the parser context.
         */
        bool IsValid() const;

    private:
        static int ReadFromInputStreamCallback(void* context, char* buffer, int len);
        static void OnLibxmlError(void* arg, const char* msg, xmlParserSeverities severity, xmlTextReaderLocatorPtr locator);

        void SetupErrorHandler();
        TStringStream& LogError() const;
        void CheckForExceptions() const;
        void ThrowException() const;

        // helpers that check return codes of C functions from libxml
        bool BoolResult(int value) const;
        int IntResult(int value) const;
        char CharResult(int value) const;
        TStringBuf ConstStringResult(const xmlChar* value) const;
        TStringBuf ConstStringOrEmptyResult(const xmlChar* value) const;
        TString TempStringResult(TCharPtr value) const;
        TString TempStringOrEmptyResult(TCharPtr value) const;

    private:
        IInputStream& Stream;

        mutable bool IsError;
        mutable TStringStream ErrorBuffer;

        struct TDeleter;
        THolder<xmlTextReader, TDeleter> Impl;
    };

}
