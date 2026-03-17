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


#include "CHDBPoco/RWLock.h"


#if   CHDB_POCO_OS == CHDB_POCO_OS_ANDROID
#include "RWLock_Android.cpp"
#else
#include "RWLock_POSIX.cpp"
#endif


namespace CHDBPoco {


RWLock::RWLock()
{
}

	
RWLock::~RWLock()
{
}


} // namespace CHDBPoco
