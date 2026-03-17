//
// RWLock.cpp
//
// Library: Foundation
// Package: Threading
// Module:  RWLock
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/RWLock.h"


#if   DB_POCO_OS == DB_POCO_OS_ANDROID
#include "RWLock_Android.cpp"
#else
#include "RWLock_POSIX.cpp"
#endif


namespace DBPoco {


RWLock::RWLock()
{
}

	
RWLock::~RWLock()
{
}


} // namespace DBPoco
