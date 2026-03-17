//
// NamedMutex.cpp
//
// Library: Foundation
// Package: Processes
// Module:  NamedMutex
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/NamedMutex.h"


#if   CHDB_POCO_OS == CHDB_POCO_OS_ANDROID
#error #include "NamedMutex_Android.cpp"
#elif defined(CHDB_POCO_OS_FAMILY_UNIX)
#include "NamedMutex_UNIX.cpp"
#endif


namespace CHDBPoco {


NamedMutex::NamedMutex(const std::string& name):
	NamedMutexImpl(name)
{
}


NamedMutex::~NamedMutex()
{
}


} // namespace CHDBPoco
