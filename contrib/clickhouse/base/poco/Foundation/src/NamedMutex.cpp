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


#include "DBPoco/NamedMutex.h"


#if   DB_POCO_OS == DB_POCO_OS_ANDROID
#error #include "NamedMutex_Android.cpp"
#elif defined(DB_POCO_OS_FAMILY_UNIX)
#include "NamedMutex_UNIX.cpp"
#endif


namespace DBPoco {


NamedMutex::NamedMutex(const std::string& name):
	NamedMutexImpl(name)
{
}


NamedMutex::~NamedMutex()
{
}


} // namespace DBPoco
