//
// EventChannel.cpp
//
// Library: Foundation
// Package: Logging
// Module:  EventChannel
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/EventChannel.h"


namespace DBPoco {


EventChannel::EventChannel()
{
}


EventChannel::~EventChannel()
{
}


void EventChannel::log(const Message& msg)
{
	messageLogged(this, msg);
}


} // namespace DBPoco
