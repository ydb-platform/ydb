//
// Event.cpp
//
// Library: Foundation
// Package: Threading
// Module:  Event
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Event.h"


#include "Event_POSIX.cpp"


namespace DBPoco {


Event::Event(bool autoReset): EventImpl(autoReset)
{
}


Event::~Event()
{
}


} // namespace DBPoco
