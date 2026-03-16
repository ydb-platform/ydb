//
// XMLException.cpp
//
// Library: XML
// Package: XML
// Module:  XMLException
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/XML/XMLException.h"
#include <typeinfo>


using DBPoco::RuntimeException;


namespace DBPoco {
namespace XML {


DB_POCO_IMPLEMENT_EXCEPTION(XMLException, RuntimeException, "XML Exception")


} } // namespace DBPoco::XML
