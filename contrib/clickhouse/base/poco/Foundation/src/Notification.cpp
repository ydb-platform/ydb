//
// Notification.cpp
//
// Library: Foundation
// Package: Notifications
// Module:  Notification
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Notification.h"
#include <typeinfo>


namespace DBPoco {


Notification::Notification()
{
}


Notification::~Notification()
{
}


std::string Notification::name() const
{
	return typeid(*this).name();
}


} // namespace DBPoco
