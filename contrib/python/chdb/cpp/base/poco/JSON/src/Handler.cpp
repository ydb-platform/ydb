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


#include "CHDBPoco/JSON/Handler.h"
#include "CHDBPoco/JSON/Object.h"


namespace CHDBPoco {
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


CHDBPoco::DynamicStruct Handler::asStruct() const
{
	return CHDBPoco::DynamicStruct();
}


} } // namespace CHDBPoco::JSON
