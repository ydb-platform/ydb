//
// ThreadTarget.cpp
//
// Library: Foundation
// Package: Threading
// Module:  ThreadTarget
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/ThreadTarget.h"


namespace CHDBPoco {


ThreadTarget::ThreadTarget(Callback method): _method(method)
{
}


ThreadTarget::ThreadTarget(const ThreadTarget& te): _method(te._method)
{
}


ThreadTarget& ThreadTarget::operator = (const ThreadTarget& te)
{
	_method  = te._method;
	return *this;
}


ThreadTarget::~ThreadTarget()
{
}


} // namespace CHDBPoco
