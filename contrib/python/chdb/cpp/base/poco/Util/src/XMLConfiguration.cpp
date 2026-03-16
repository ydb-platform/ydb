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


#include "CHDBPoco/Util/XMLConfiguration.h"


#ifndef CHDB_POCO_UTIL_NO_XMLCONFIGURATION


#include "CHDBPoco/String.h"
#include "CHDBPoco/SAX/InputSource.h"
#include "CHDBPoco/DOM/DOMParser.h"
#include "CHDBPoco/DOM/Element.h"
#include "CHDBPoco/DOM/Attr.h"
#include "CHDBPoco/DOM/Text.h"
#include "CHDBPoco/XML/XMLWriter.h"
#include "CHDBPoco/Exception.h"
#include "CHDBPoco/NumberParser.h"
#include "CHDBPoco/NumberFormatter.h"
#include <unordered_map>
#include <algorithm>
#include <iterator>


namespace CHDBPoco {
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


XMLConfiguration::XMLConfiguration(CHDBPoco::XML::InputSource* pInputSource):
	_delim('.')
{
	load(pInputSource);
}


XMLConfiguration::XMLConfiguration(CHDBPoco::XML::InputSource* pInputSource, char delim):
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


XMLConfiguration::XMLConfiguration(const CHDBPoco::XML::Document* pDocument):
	_delim('.')
{
	load(pDocument);
}


XMLConfiguration::XMLConfiguration(const CHDBPoco::XML::Document* pDocument, char delim):
	_delim(delim)
{
	load(pDocument);
}

	
XMLConfiguration::XMLConfiguration(const CHDBPoco::XML::Node* pNode):
	_delim('.')
{
	load(pNode);
}


XMLConfiguration::XMLConfiguration(const CHDBPoco::XML::Node* pNode, char delim):
	_delim(delim)
{
	load(pNode);
}


XMLConfiguration::~XMLConfiguration()
{
}


void XMLConfiguration::load(CHDBPoco::XML::InputSource* pInputSource, unsigned long namePoolSize)
{
	CHDB_poco_check_ptr (pInputSource);
	
	CHDBPoco::XML::DOMParser parser(namePoolSize);
	parser.setFeature(CHDBPoco::XML::XMLReader::FEATURE_NAMESPACES, false);
	parser.setFeature(CHDBPoco::XML::DOMParser::FEATURE_FILTER_WHITESPACE, true);
	CHDBPoco::XML::AutoPtr<CHDBPoco::XML::Document> pDoc = parser.parse(pInputSource);
	load(pDoc);
}


void XMLConfiguration::load(CHDBPoco::XML::InputSource* pInputSource)
{
	load(pInputSource, CHDB_POCO_XML_NAMEPOOL_DEFAULT_SIZE);
}


void XMLConfiguration::load(std::istream& istr)
{
	CHDBPoco::XML::InputSource src(istr);
	load(&src);	
}


void XMLConfiguration::load(const std::string& path)
{
	CHDBPoco::XML::InputSource src(path);
	load(&src);	
}

	
void XMLConfiguration::load(const CHDBPoco::XML::Document* pDocument)
{
	CHDB_poco_check_ptr (pDocument);
	
	_pDocument = CHDBPoco::XML::AutoPtr<CHDBPoco::XML::Document>(const_cast<CHDBPoco::XML::Document*>(pDocument), true);
	_pRoot     = CHDBPoco::XML::AutoPtr<CHDBPoco::XML::Node>(pDocument->documentElement(), true);
}


void XMLConfiguration::load(const CHDBPoco::XML::Node* pNode)
{
	CHDB_poco_check_ptr (pNode);

	if (pNode->nodeType() == CHDBPoco::XML::Node::DOCUMENT_NODE)
	{
		load(static_cast<const CHDBPoco::XML::Document*>(pNode));
	}
	else
	{
		_pDocument = CHDBPoco::XML::AutoPtr<CHDBPoco::XML::Document>(pNode->ownerDocument(), true);
		_pRoot     = CHDBPoco::XML::AutoPtr<CHDBPoco::XML::Node>(const_cast<CHDBPoco::XML::Node*>(pNode), true);
	}
}


void XMLConfiguration::loadEmpty(const std::string& rootElementName)
{
	_pDocument = new CHDBPoco::XML::Document;
	_pRoot     = _pDocument->createElement(rootElementName);
	_pDocument->appendChild(_pRoot);
}


void XMLConfiguration::save(const std::string& path) const
{
	CHDBPoco::XML::DOMWriter writer;
	writer.setNewLine("\n");
	writer.setOptions(CHDBPoco::XML::XMLWriter::PRETTY_PRINT);
	writer.writeNode(path, _pDocument);
}


void XMLConfiguration::save(std::ostream& ostr) const
{
	CHDBPoco::XML::DOMWriter writer;
	writer.setNewLine("\n");
	writer.setOptions(CHDBPoco::XML::XMLWriter::PRETTY_PRINT);
	writer.writeNode(ostr, _pDocument);
}


void XMLConfiguration::save(CHDBPoco::XML::DOMWriter& writer, const std::string& path) const
{
	writer.writeNode(path, _pDocument);
}


void XMLConfiguration::save(CHDBPoco::XML::DOMWriter& writer, std::ostream& ostr) const
{
	writer.writeNode(ostr, _pDocument);
}


bool XMLConfiguration::getRaw(const std::string& key, std::string& value) const
{
	const CHDBPoco::XML::Node* pNode = findNode(key);
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
	CHDBPoco::XML::Node* pNode = findNode(it, key.end(), _pRoot, true);
	if (pNode)
	{
        unsigned short nodeType = pNode->nodeType();
        if (CHDBPoco::XML::Node::ATTRIBUTE_NODE == nodeType)
        {
            pNode->setNodeValue(value);
        }
        else if (CHDBPoco::XML::Node::ELEMENT_NODE == nodeType)
        {
            CHDBPoco::XML::Node* pChildNode = pNode->firstChild();
            if (pChildNode)
            {
                if (CHDBPoco::XML::Node::TEXT_NODE == pChildNode->nodeType())
                {
                    pChildNode->setNodeValue(value);
                }
            }
            else
            {
				CHDBPoco::AutoPtr<CHDBPoco::XML::Node> pText = _pDocument->createTextNode(value);
				pNode->appendChild(pText);
            }
        }
	}
    else throw NotFoundException("Node not found in XMLConfiguration", key);
}


void XMLConfiguration::enumerate(const std::string& key, Keys& range) const
{
	using CHDBPoco::NumberFormatter;

	std::unordered_map<std::string, size_t> keys;
	const CHDBPoco::XML::Node* pNode = findNode(key);
	if (pNode)
	{
		const CHDBPoco::XML::Node* pChild = pNode->firstChild();
		while (pChild)
		{
			if (pChild->nodeType() == CHDBPoco::XML::Node::ELEMENT_NODE)
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
	CHDBPoco::XML::Node* pNode = findNode(key);

	if (pNode)
	{
		if (pNode->nodeType() == CHDBPoco::XML::Node::ELEMENT_NODE)
		{
			CHDBPoco::XML::Node* pParent = pNode->parentNode();
			if (pParent)
			{
				pParent->removeChild(pNode);
			}
		}
		else if (pNode->nodeType() == CHDBPoco::XML::Node::ATTRIBUTE_NODE)
		{
			CHDBPoco::XML::Attr* pAttr = dynamic_cast<CHDBPoco::XML::Attr*>(pNode);
			CHDBPoco::XML::Element* pOwner = pAttr->ownerElement();
			if (pOwner)
			{
				pOwner->removeAttributeNode(pAttr);
			}
		}
	}
}


const CHDBPoco::XML::Node* XMLConfiguration::findNode(const std::string& key) const
{
	std::string::const_iterator it = key.begin();
	CHDBPoco::XML::Node* pRoot = const_cast<CHDBPoco::XML::Node*>(_pRoot.get());
	return findNode(it, key.end(), pRoot);
}


CHDBPoco::XML::Node* XMLConfiguration::findNode(const std::string& key)
{
	std::string::const_iterator it = key.begin();
	CHDBPoco::XML::Node* pRoot = const_cast<CHDBPoco::XML::Node*>(_pRoot.get());
	return findNode(it, key.end(), pRoot);
}


CHDBPoco::XML::Node* XMLConfiguration::findNode(std::string::const_iterator& it, const std::string::const_iterator& end, CHDBPoco::XML::Node* pNode, bool create) const
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
				return findNode(it, end, findElement(CHDBPoco::NumberParser::parse(index), pNode, create), create);
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


CHDBPoco::XML::Node* XMLConfiguration::findElement(const std::string& name, CHDBPoco::XML::Node* pNode, bool create)
{
	CHDBPoco::XML::Node* pChild = pNode->firstChild();
	while (pChild)
	{
		if (pChild->nodeType() == CHDBPoco::XML::Node::ELEMENT_NODE && pChild->nodeName() == name)
			return pChild;
		pChild = pChild->nextSibling();
	}
	if (create)
	{
		CHDBPoco::AutoPtr<CHDBPoco::XML::Element> pElem = pNode->ownerDocument()->createElement(name);
		pNode->appendChild(pElem);
		return pElem;
	}
	else return 0;
}


CHDBPoco::XML::Node* XMLConfiguration::findElement(int index, CHDBPoco::XML::Node* pNode, bool create)
{
	CHDBPoco::XML::Node* pRefNode = pNode;
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
			CHDBPoco::AutoPtr<CHDBPoco::XML::Element> pElem = pRefNode->ownerDocument()->createElement(pRefNode->nodeName());
			pRefNode->parentNode()->appendChild(pElem);
			return pElem;
		}
		else throw CHDBPoco::InvalidArgumentException("Element index out of range.");
	}
	return pNode;
}


CHDBPoco::XML::Node* XMLConfiguration::findElement(const std::string& attr, const std::string& value, CHDBPoco::XML::Node* pNode)
{
	CHDBPoco::XML::Node* pRefNode = pNode;
	CHDBPoco::XML::Element* pElem = dynamic_cast<CHDBPoco::XML::Element*>(pNode);
	if (!(pElem && pElem->getAttribute(attr) == value))
	{
		pNode = pNode->nextSibling();
		while (pNode)
		{
			if (pNode->nodeName() == pRefNode->nodeName())
			{
				pElem = dynamic_cast<CHDBPoco::XML::Element*>(pNode);
				if (pElem && pElem->getAttribute(attr) == value) break;
			}
			pNode = pNode->nextSibling();
		}
	}
	return pNode;
}


CHDBPoco::XML::Node* XMLConfiguration::findAttribute(const std::string& name, CHDBPoco::XML::Node* pNode, bool create)
{
	CHDBPoco::XML::Node* pResult(0);
	CHDBPoco::XML::Element* pElem = dynamic_cast<CHDBPoco::XML::Element*>(pNode);
	if (pElem)
	{
		pResult = pElem->getAttributeNode(name);
		if (!pResult && create)
		{
			CHDBPoco::AutoPtr<CHDBPoco::XML::Attr> pAttr = pNode->ownerDocument()->createAttribute(name);
			pElem->setAttributeNode(pAttr);
			return pAttr;
		}
	}
	return pResult;
}


} } // namespace CHDBPoco::Util

#endif // CHDB_POCO_UTIL_NO_XMLCONFIGURATION
