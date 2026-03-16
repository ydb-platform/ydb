//
// Formatter.cpp
//
// Library: Foundation
// Package: Logging
// Module:  Formatter
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Formatter.h"
#include "DBPoco/Exception.h"


namespace DBPoco {


Formatter::Formatter()
{
}


Formatter::~Formatter()
{
}


void Formatter::setProperty(const std::string& name, const std::string& value)
{
	throw PropertyNotSupportedException();
}


std::string Formatter::getProperty(const std::string& name) const
{
	throw PropertyNotSupportedException();
}


} // namespace DBPoco
