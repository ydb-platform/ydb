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


#include "DBPoco/SAX/EntityResolverImpl.h"
#include "DBPoco/SAX/InputSource.h"
#include "DBPoco/XML/XMLString.h"
#include "DBPoco/URI.h"
#include "DBPoco/Path.h"
#include "DBPoco/Exception.h"


using DBPoco::URIStreamOpener;
using DBPoco::URI;
using DBPoco::Path;
using DBPoco::Exception;
using DBPoco::IOException;
using DBPoco::OpenFileException;


namespace DBPoco {
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
	DB_poco_check_ptr (pSource);

	delete pSource->getByteStream();
	delete pSource;
}


std::istream* EntityResolverImpl::resolveSystemId(const XMLString& systemId)
{
	std::string sid = fromXMLString(systemId);
	return _opener.open(sid);
}


} } // namespace DBPoco::XML
