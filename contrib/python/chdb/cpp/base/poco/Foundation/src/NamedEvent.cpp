//
// NamedEvent.cpp
//
// Library: Foundation
// Package: Processes
// Module:  NamedEvent
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/NamedEvent.h"


#if   CHDB_POCO_OS == CHDB_POCO_OS_ANDROID
#error #include "NamedEvent_Android.cpp"
#elif defined(CHDB_POCO_OS_FAMILY_UNIX)
#include "NamedEvent_UNIX.cpp"
#endif


namespace CHDBPoco {


NamedEvent::NamedEvent(const std::string& name):
	NamedEventImpl(name)
{
}


NamedEvent::~NamedEvent()
{
}


} // namespace CHDBPoco
