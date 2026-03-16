//
// AbstractObserver.cpp
//
// Library: Foundation
// Package: Notifications
// Module:  NotificationCenter
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/AbstractObserver.h"


namespace DBPoco {


AbstractObserver::AbstractObserver()
{
}


AbstractObserver::AbstractObserver(const AbstractObserver& observer)
{
}


AbstractObserver::~AbstractObserver()
{
}

	
AbstractObserver& AbstractObserver::operator = (const AbstractObserver& observer)
{
	return *this;
}


} // namespace DBPoco
