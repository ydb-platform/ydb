//
// XMLConfiguration.cpp
//
// Library: Util
// Package: Configuration
// Module:  XMLConfiguration
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Util/XMLConfiguration.h"


#ifndef DB_POCO_UTIL_NO_XMLCONFIGURATION


#include "DBPoco/String.h"
#include "DBPoco/SAX/InputSource.h"
#include "DBPoco/DOM/DOMParser.h"
#include "DBPoco/DOM/Element.h"
#include "DBPoco/DOM/Attr.h"
#include "DBPoco/DOM/Text.h"
#include "DBPoco/XML/XMLWriter.h"
#include "DBPoco/Exception.h"
#include "DBPoco/NumberParser.h"
#include "DBPoco/NumberFormatter.h"
#include <unordered_map>
#include <algorithm>
#include <iterator>


namespace DBPoco {
namespace Util {


XMLConfiguration::XMLConfiguration():
	_delim('.')
{
	loadEmpty("config");
}


XMLConfiguration::XMLConfiguration(char delim):
	_delim(delim)
{
	loadEmpty("config");
}


XMLConfiguration::XMLConfiguration(DBPoco::XML::InputSource* pInputSource):
	_delim('.')
{
	load(pInputSource);
}


XMLConfiguration::XMLConfiguration(DBPoco::XML::InputSource* pInputSource, char delim):
	_delim(delim)
{
	load(pInputSource);
}


XMLConfiguration::XMLConfiguration(std::istream& istr):
	_delim('.')
{
	load(istr);
}


XMLConfiguration::XMLConfiguration(std::istream& istr, char delim):
	_delim(delim)
{
	load(istr);
}


XMLConfiguration::XMLConfiguration(const std::string& path):
	_delim('.')
{
	load(path);
}


XMLConfiguration::XMLConfiguration(const std::string& path, char delim):
	_delim(delim)
{
	load(path);
}


XMLConfiguration::XMLConfiguration(const DBPoco::XML::Document* pDocument):
	_delim('.')
{
	load(pDocument);
}


XMLConfiguration::XMLConfiguration(const DBPoco::XML::Document* pDocument, char delim):
	_delim(delim)
{
	load(pDocument);
}

	
XMLConfiguration::XMLConfiguration(const DBPoco::XML::Node* pNode):
	_delim('.')
{
	load(pNode);
}


XMLConfiguration::XMLConfiguration(const DBPoco::XML::Node* pNode, char delim):
	_delim(delim)
{
	load(pNode);
}


XMLConfiguration::~XMLConfiguration()
{
}


void XMLConfiguration::load(DBPoco::XML::InputSource* pInputSource, unsigned long namePoolSize)
{
	DB_poco_check_ptr (pInputSource);
	
	DBPoco::XML::DOMParser parser(namePoolSize);
	parser.setFeature(DBPoco::XML::XMLReader::FEATURE_NAMESPACES, false);
	parser.setFeature(DBPoco::XML::DOMParser::FEATURE_FILTER_WHITESPACE, true);
	DBPoco::XML::AutoPtr<DBPoco::XML::Document> pDoc = parser.parse(pInputSource);
	load(pDoc);
}


void XMLConfiguration::load(DBPoco::XML::InputSource* pInputSource)
{
	load(pInputSource, DB_POCO_XML_NAMEPOOL_DEFAULT_SIZE);
}


void XMLConfiguration::load(std::istream& istr)
{
	DBPoco::XML::InputSource src(istr);
	load(&src);	
}


void XMLConfiguration::load(const std::string& path)
{
	DBPoco::XML::InputSource src(path);
	load(&src);	
}

	
void XMLConfiguration::load(const DBPoco::XML::Document* pDocument)
{
	DB_poco_check_ptr (pDocument);
	
	_pDocument = DBPoco::XML::AutoPtr<DBPoco::XML::Document>(const_cast<DBPoco::XML::Document*>(pDocument), true);
	_pRoot     = DBPoco::XML::AutoPtr<DBPoco::XML::Node>(pDocument->documentElement(), true);
}


void XMLConfiguration::load(const DBPoco::XML::Node* pNode)
{
	DB_poco_check_ptr (pNode);

	if (pNode->nodeType() == DBPoco::XML::Node::DOCUMENT_NODE)
	{
		load(static_cast<const DBPoco::XML::Document*>(pNode));
	}
	else
	{
		_pDocument = DBPoco::XML::AutoPtr<DBPoco::XML::Document>(pNode->ownerDocument(), true);
		_pRoot     = DBPoco::XML::AutoPtr<DBPoco::XML::Node>(const_cast<DBPoco::XML::Node*>(pNode), true);
	}
}


void XMLConfiguration::loadEmpty(const std::string& rootElementName)
{
	_pDocument = new DBPoco::XML::Document;
	_pRoot     = _pDocument->createElement(rootElementName);
	_pDocument->appendChild(_pRoot);
}


void XMLConfiguration::save(const std::string& path) const
{
	DBPoco::XML::DOMWriter writer;
	writer.setNewLine("\n");
	writer.setOptions(DBPoco::XML::XMLWriter::PRETTY_PRINT);
	writer.writeNode(path, _pDocument);
}


void XMLConfiguration::save(std::ostream& ostr) const
{
	DBPoco::XML::DOMWriter writer;
	writer.setNewLine("\n");
	writer.setOptions(DBPoco::XML::XMLWriter::PRETTY_PRINT);
	writer.writeNode(ostr, _pDocument);
}


void XMLConfiguration::save(DBPoco::XML::DOMWriter& writer, const std::string& path) const
{
	writer.writeNode(path, _pDocument);
}


void XMLConfiguration::save(DBPoco::XML::DOMWriter& writer, std::ostream& ostr) const
{
	writer.writeNode(ostr, _pDocument);
}


bool XMLConfiguration::getRaw(const std::string& key, std::string& value) const
{
	const DBPoco::XML::Node* pNode = findNode(key);
	if (pNode)
	{
		value = pNode->innerText();
		return true;
	}
	else return false;
}


void XMLConfiguration::setRaw(const std::string& key, const std::string& value)
{
	std::string::const_iterator it = key.begin();
	DBPoco::XML::Node* pNode = findNode(it, key.end(), _pRoot, true);
	if (pNode)
	{
        unsigned short nodeType = pNode->nodeType();
        if (DBPoco::XML::Node::ATTRIBUTE_NODE == nodeType)
        {
            pNode->setNodeValue(value);
        }
        else if (DBPoco::XML::Node::ELEMENT_NODE == nodeType)
        {
            DBPoco::XML::Node* pChildNode = pNode->firstChild();
            if (pChildNode)
            {
                if (DBPoco::XML::Node::TEXT_NODE == pChildNode->nodeType())
                {
                    pChildNode->setNodeValue(value);
                }
            }
            else
            {
				DBPoco::AutoPtr<DBPoco::XML::Node> pText = _pDocument->createTextNode(value);
				pNode->appendChild(pText);
            }
        }
	}
    else throw NotFoundException("Node not found in XMLConfiguration", key);
}


void XMLConfiguration::enumerate(const std::string& key, Keys& range) const
{
	using DBPoco::NumberFormatter;

	std::unordered_map<std::string, size_t> keys;
	const DBPoco::XML::Node* pNode = findNode(key);
	if (pNode)
	{
		const DBPoco::XML::Node* pChild = pNode->firstChild();
		while (pChild)
		{
			if (pChild->nodeType() == DBPoco::XML::Node::ELEMENT_NODE)
			{
				std::string nodeName = pChild->nodeName();
				size_t& count = keys[nodeName];
				replaceInPlace(nodeName, ".", "\\.");
				if (count)
					range.push_back(nodeName + "[" + NumberFormatter::format(count) + "]");
				else
					range.push_back(nodeName);
				++count;
			}
			pChild = pChild->nextSibling();
		}
	}
}


void XMLConfiguration::removeRaw(const std::string& key)
{
	DBPoco::XML::Node* pNode = findNode(key);

	if (pNode)
	{
		if (pNode->nodeType() == DBPoco::XML::Node::ELEMENT_NODE)
		{
			DBPoco::XML::Node* pParent = pNode->parentNode();
			if (pParent)
			{
				pParent->removeChild(pNode);
			}
		}
		else if (pNode->nodeType() == DBPoco::XML::Node::ATTRIBUTE_NODE)
		{
			DBPoco::XML::Attr* pAttr = dynamic_cast<DBPoco::XML::Attr*>(pNode);
			DBPoco::XML::Element* pOwner = pAttr->ownerElement();
			if (pOwner)
			{
				pOwner->removeAttributeNode(pAttr);
			}
		}
	}
}


const DBPoco::XML::Node* XMLConfiguration::findNode(const std::string& key) const
{
	std::string::const_iterator it = key.begin();
	DBPoco::XML::Node* pRoot = const_cast<DBPoco::XML::Node*>(_pRoot.get());
	return findNode(it, key.end(), pRoot);
}


DBPoco::XML::Node* XMLConfiguration::findNode(const std::string& key)
{
	std::string::const_iterator it = key.begin();
	DBPoco::XML::Node* pRoot = const_cast<DBPoco::XML::Node*>(_pRoot.get());
	return findNode(it, key.end(), pRoot);
}


DBPoco::XML::Node* XMLConfiguration::findNode(std::string::const_iterator& it, const std::string::const_iterator& end, DBPoco::XML::Node* pNode, bool create) const
{
	if (pNode && it != end)
	{
		if (*it == '[')
		{
			++it;
			if (it != end && *it == '@')
			{
				++it;
				std::string attr;
				while (it != end && *it != ']' && *it != '=') attr += *it++;
				if (it != end && *it == '=')
				{
					++it;
					std::string value;
					if (it != end && *it == '\'')
					{
						++it;
						while (it != end && *it != '\'') value += *it++;
						if (it != end) ++it;
					}
					else
					{
						while (it != end && *it != ']') value += *it++;
					}
					if (it != end) ++it;
					return findNode(it, end, findElement(attr, value, pNode), create);
				}
				else
				{
					if (it != end) ++it;
					return findAttribute(attr, pNode, create);
				}
			}
			else
			{
				std::string index;
				while (it != end && *it != ']') index += *it++;
				if (it != end) ++it;
				return findNode(it, end, findElement(DBPoco::NumberParser::parse(index), pNode, create), create);
			}
		}
		else
		{
			while (it != end && *it == _delim) ++it;
			std::string key;
			while (it != end)
			{
				if (*it == '\\' && std::distance(it, end) > 1)
				{
					// Skip backslash, copy only the char after it
					std::advance(it, 1);
					key += *it++;
					continue;
				}
				if (*it == _delim)
					break;
				if (*it == '[')
					break;
				key += *it++;
			}
			return findNode(it, end, findElement(key, pNode, create), create);
		}
	}
	else return pNode;
}


DBPoco::XML::Node* XMLConfiguration::findElement(const std::string& name, DBPoco::XML::Node* pNode, bool create)
{
	DBPoco::XML::Node* pChild = pNode->firstChild();
	while (pChild)
	{
		if (pChild->nodeType() == DBPoco::XML::Node::ELEMENT_NODE && pChild->nodeName() == name)
			return pChild;
		pChild = pChild->nextSibling();
	}
	if (create)
	{
		DBPoco::AutoPtr<DBPoco::XML::Element> pElem = pNode->ownerDocument()->createElement(name);
		pNode->appendChild(pElem);
		return pElem;
	}
	else return 0;
}


DBPoco::XML::Node* XMLConfiguration::findElement(int index, DBPoco::XML::Node* pNode, bool create)
{
	DBPoco::XML::Node* pRefNode = pNode;
	if (index > 0)
	{
		pNode = pNode->nextSibling();
		while (pNode)
		{
			if (pNode->nodeName() == pRefNode->nodeName())
			{
				if (--index == 0) break;
			}
			pNode = pNode->nextSibling();
		}
	}
	if (!pNode && create)
	{
		if (index == 1)
		{
			DBPoco::AutoPtr<DBPoco::XML::Element> pElem = pRefNode->ownerDocument()->createElement(pRefNode->nodeName());
			pRefNode->parentNode()->appendChild(pElem);
			return pElem;
		}
		else throw DBPoco::InvalidArgumentException("Element index out of range.");
	}
	return pNode;
}


DBPoco::XML::Node* XMLConfiguration::findElement(const std::string& attr, const std::string& value, DBPoco::XML::Node* pNode)
{
	DBPoco::XML::Node* pRefNode = pNode;
	DBPoco::XML::Element* pElem = dynamic_cast<DBPoco::XML::Element*>(pNode);
	if (!(pElem && pElem->getAttribute(attr) == value))
	{
		pNode = pNode->nextSibling();
		while (pNode)
		{
			if (pNode->nodeName() == pRefNode->nodeName())
			{
				pElem = dynamic_cast<DBPoco::XML::Element*>(pNode);
				if (pElem && pElem->getAttribute(attr) == value) break;
			}
			pNode = pNode->nextSibling();
		}
	}
	return pNode;
}


DBPoco::XML::Node* XMLConfiguration::findAttribute(const std::string& name, DBPoco::XML::Node* pNode, bool create)
{
	DBPoco::XML::Node* pResult(0);
	DBPoco::XML::Element* pElem = dynamic_cast<DBPoco::XML::Element*>(pNode);
	if (pElem)
	{
		pResult = pElem->getAttributeNode(name);
		if (!pResult && create)
		{
			DBPoco::AutoPtr<DBPoco::XML::Attr> pAttr = pNode->ownerDocument()->createAttribute(name);
			pElem->setAttributeNode(pAttr);
			return pAttr;
		}
	}
	return pResult;
}


} } // namespace DBPoco::Util

#endif // DB_POCO_UTIL_NO_XMLCONFIGURATION
