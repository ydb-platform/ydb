//
// Handler.cpp
//
// Library: JSON
// Package: JSON
// Module:  Handler
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/JSON/Handler.h"
#include "DBPoco/JSON/Object.h"


namespace DBPoco {
namespace JSON {


Handler::Handler()
{
}


Handler::~Handler()
{
}


Dynamic::Var Handler::asVar() const
{
	return Dynamic::Var();
}


DBPoco::DynamicStruct Handler::asStruct() const
{
	return DBPoco::DynamicStruct();
}


} } // namespace DBPoco::JSON
