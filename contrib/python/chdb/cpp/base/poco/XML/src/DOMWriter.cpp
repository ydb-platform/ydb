//
// DOMWriter.cpp
//
// Library: XML
// Package: DOM
// Module:  DOMWriter
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//



#include "CHDBPoco/DOM/DOMWriter.h"
#include "CHDBPoco/XML/XMLWriter.h"
#include "CHDBPoco/DOM/Document.h"
#include "CHDBPoco/DOM/DocumentFragment.h"
#include "CHDBPoco/DOM/DocumentType.h"
#include "CHDBPoco/DOM/DOMException.h"
#include "CHDBPoco/DOM/DOMSerializer.h"
#include "CHDBPoco/SAX/LexicalHandler.h"
#include "CHDBPoco/XML/XMLException.h"
#include "CHDBPoco/Path.h"
#include "CHDBPoco/FileStream.h"


namespace CHDBPoco {
namespace XML {


DOMWriter::DOMWriter():
	_pTextEncoding(0),
	_options(0),
	_indent("\t")
{
}


DOMWriter::~DOMWriter()
{
}


void DOMWriter::setEncoding(const std::string& encodingName, CHDBPoco::TextEncoding& textEncoding)
{
	_encodingName  = encodingName;
	_pTextEncoding = &textEncoding;
}


void DOMWriter::setOptions(int options)
{
	_options = options;
}


void DOMWriter::setNewLine(const std::string& newLine)
{
	_newLine = newLine;
}


void DOMWriter::setIndent(const std::string& indent)
{
	_indent = indent;
}


void DOMWriter::writeNode(XMLByteOutputStream& ostr, const Node* pNode)
{
	CHDB_poco_check_ptr (pNode);

	bool isFragment = pNode->nodeType() != Node::DOCUMENT_NODE;

	XMLWriter writer(ostr, _options, _encodingName, _pTextEncoding);
	writer.setNewLine(_newLine);
	writer.setIndent(_indent);
	
	DOMSerializer serializer;
	serializer.setContentHandler(&writer);
	serializer.setDTDHandler(&writer);
	serializer.setProperty(XMLReader::PROPERTY_LEXICAL_HANDLER, static_cast<LexicalHandler*>(&writer));
	if (isFragment) writer.startFragment();
	serializer.serialize(pNode);
	if (isFragment) writer.endFragment();
}


void DOMWriter::writeNode(const std::string& systemId, const Node* pNode)
{
	CHDBPoco::FileOutputStream ostr(systemId);
	if (ostr.good())
		writeNode(ostr, pNode);
	else 
		throw CHDBPoco::CreateFileException(systemId);
}


} } // namespace CHDBPoco::XML

