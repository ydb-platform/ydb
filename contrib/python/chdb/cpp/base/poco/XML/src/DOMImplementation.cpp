//
// DOMImplementation.cpp
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/DOM/DOMImplementation.h"
#include "CHDBPoco/DOM/DocumentType.h"
#include "CHDBPoco/DOM/Document.h"
#include "CHDBPoco/DOM/Element.h"
#include "CHDBPoco/String.h"
#include "CHDBPoco/SingletonHolder.h"


namespace CHDBPoco {
namespace XML {


const XMLString DOMImplementation::FEATURE_XML            = toXMLString("xml");
const XMLString DOMImplementation::FEATURE_CORE           = toXMLString("core");
const XMLString DOMImplementation::FEATURE_EVENTS         = toXMLString("events");
const XMLString DOMImplementation::FEATURE_MUTATIONEVENTS = toXMLString("mutationevents");
const XMLString DOMImplementation::FEATURE_TRAVERSAL      = toXMLString("traversal");
const XMLString DOMImplementation::VERSION_1_0            = toXMLString("1.0");
const XMLString DOMImplementation::VERSION_2_0            = toXMLString("2.0");


DOMImplementation::DOMImplementation()
{
}


DOMImplementation::~DOMImplementation()
{
}


bool DOMImplementation::hasFeature(const XMLString& feature, const XMLString& version) const
{
	XMLString lcFeature = CHDBPoco::toLower(feature);
	return (lcFeature == FEATURE_XML && version == VERSION_1_0) ||
	       (lcFeature == FEATURE_CORE && version == VERSION_2_0) ||
	       (lcFeature == FEATURE_EVENTS && version == VERSION_2_0) ||
	       (lcFeature == FEATURE_MUTATIONEVENTS && version == VERSION_2_0) ||
	       (lcFeature == FEATURE_TRAVERSAL && version == VERSION_2_0);
}

		
DocumentType* DOMImplementation::createDocumentType(const XMLString& name, const XMLString& publicId, const XMLString& systemId) const
{
	return new DocumentType(0, name, publicId, systemId);
}


Document* DOMImplementation::createDocument(const XMLString& namespaceURI, const XMLString& qualifiedName, DocumentType* doctype) const
{
	Document* pDoc = new Document(doctype);
	if (namespaceURI.empty())
		pDoc->appendChild(pDoc->createElement(qualifiedName))->release();
	else
		pDoc->appendChild(pDoc->createElementNS(namespaceURI, qualifiedName))->release();
	return pDoc;
}


namespace
{
	static CHDBPoco::SingletonHolder<DOMImplementation> sh;
}


const DOMImplementation& DOMImplementation::instance()
{
	return *sh.get();
}


} } // namespace CHDBPoco::XML
