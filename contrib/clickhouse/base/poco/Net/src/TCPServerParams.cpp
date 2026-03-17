//
// TCPServerParams.cpp
//
// Library: Net
// Package: TCPServer
// Module:  TCPServerParams
//
// Copyright (c) 2005-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/TCPServerParams.h"


namespace DBPoco {
namespace Net {


TCPServerParams::TCPServerParams():
	_threadIdleTime(10000000),
	_maxThreads(0),
	_maxQueued(64),
	_threadPriority(DBPoco::Thread::PRIO_NORMAL)
{
}


TCPServerParams::~TCPServerParams()
{
}


void TCPServerParams::setThreadIdleTime(const DBPoco::Timespan& milliseconds)
{
	_threadIdleTime = milliseconds;
}


void TCPServerParams::setMaxThreads(int count)
{
	DB_poco_assert (count > 0);

	_maxThreads = count;
}


void TCPServerParams::setMaxQueued(int count)
{
	DB_poco_assert (count >= 0);

	_maxQueued = count;
}


void TCPServerParams::setThreadPriority(DBPoco::Thread::Priority prio)
{
	_threadPriority = prio;
}


} } // namespace DBPoco::Net
