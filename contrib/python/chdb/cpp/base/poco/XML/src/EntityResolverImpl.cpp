//
// EntityResolverImpl.cpp
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/SAX/EntityResolverImpl.h"
#include "CHDBPoco/SAX/InputSource.h"
#include "CHDBPoco/XML/XMLString.h"
#include "CHDBPoco/URI.h"
#include "CHDBPoco/Path.h"
#include "CHDBPoco/Exception.h"


using CHDBPoco::URIStreamOpener;
using CHDBPoco::URI;
using CHDBPoco::Path;
using CHDBPoco::Exception;
using CHDBPoco::IOException;
using CHDBPoco::OpenFileException;


namespace CHDBPoco {
namespace XML {


EntityResolverImpl::EntityResolverImpl():
	_opener(URIStreamOpener::defaultOpener())
{
}


EntityResolverImpl::EntityResolverImpl(const URIStreamOpener& opener):
	_opener(opener)
{
}


EntityResolverImpl::~EntityResolverImpl()
{
}


InputSource* EntityResolverImpl::resolveEntity(const XMLString* publicId, const XMLString& systemId)
{
	std::istream* pIstr = resolveSystemId(systemId);
	InputSource* pInputSource = new InputSource(systemId);
	if (publicId) pInputSource->setPublicId(*publicId);
	pInputSource->setByteStream(*pIstr);
	return pInputSource;
}

		
void EntityResolverImpl::releaseInputSource(InputSource* pSource)
{
	CHDB_poco_check_ptr (pSource);

	delete pSource->getByteStream();
	delete pSource;
}


std::istream* EntityResolverImpl::resolveSystemId(const XMLString& systemId)
{
	std::string sid = fromXMLString(systemId);
	return _opener.open(sid);
}


} } // namespace CHDBPoco::XML
