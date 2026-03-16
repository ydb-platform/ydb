//
// AtomicCounter.cpp
//
// Library: Foundation
// Package: Core
// Module:  AtomicCounter
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/AtomicCounter.h"


namespace CHDBPoco {


#if defined(CHDB_POCO_HAVE_STD_ATOMICS)
//
// C++11 atomics
//
AtomicCounter::AtomicCounter():
	_counter(0)
{
}

	
AtomicCounter::AtomicCounter(AtomicCounter::ValueType initialValue):
	_counter(initialValue)
{
}


AtomicCounter::AtomicCounter(const AtomicCounter& counter):
	_counter(counter.value())
{
}


AtomicCounter::~AtomicCounter()
{
}


AtomicCounter& AtomicCounter::operator = (const AtomicCounter& counter)
{
	_counter.store(counter._counter.load());
	return *this;
}

	
AtomicCounter& AtomicCounter::operator = (AtomicCounter::ValueType value)
{
	_counter.store(value);
	return *this;
}


#elif CHDB_POCO_OS == CHDB_POCO_OS_WINDOWS_NT
//
// Windows
//
AtomicCounter::AtomicCounter():
	_counter(0)
{
}

	
AtomicCounter::AtomicCounter(AtomicCounter::ValueType initialValue):
	_counter(initialValue)
{
}


AtomicCounter::AtomicCounter(const AtomicCounter& counter):
	_counter(counter.value())
{
}


AtomicCounter::~AtomicCounter()
{
}


AtomicCounter& AtomicCounter::operator = (const AtomicCounter& counter)
{
	InterlockedExchange(&_counter, counter.value());
	return *this;
}

	
AtomicCounter& AtomicCounter::operator = (AtomicCounter::ValueType value)
{
	InterlockedExchange(&_counter, value);
	return *this;
}


#elif CHDB_POCO_OS == CHDB_POCO_OS_MAC_OS_X
//
// Mac OS X
//
AtomicCounter::AtomicCounter():
	_counter(0)
{
}

	
AtomicCounter::AtomicCounter(AtomicCounter::ValueType initialValue):
	_counter(initialValue)
{
}


AtomicCounter::AtomicCounter(const AtomicCounter& counter):
	_counter(counter.value())
{
}


AtomicCounter::~AtomicCounter()
{
}


AtomicCounter& AtomicCounter::operator = (const AtomicCounter& counter)
{
	_counter = counter.value();
	return *this;
}

	
AtomicCounter& AtomicCounter::operator = (AtomicCounter::ValueType value)
{
	_counter = value;
	return *this;
}


#elif defined(CHDB_POCO_HAVE_GCC_ATOMICS)
//
// GCC 4.1+ atomic builtins.
//
AtomicCounter::AtomicCounter():
	_counter(0)
{
}

	
AtomicCounter::AtomicCounter(AtomicCounter::ValueType initialValue):
	_counter(initialValue)
{
}


AtomicCounter::AtomicCounter(const AtomicCounter& counter):
	_counter(counter.value())
{
}


AtomicCounter::~AtomicCounter()
{
}


AtomicCounter& AtomicCounter::operator = (const AtomicCounter& counter)
{
	__sync_lock_test_and_set(&_counter, counter.value());
	return *this;
}

	
AtomicCounter& AtomicCounter::operator = (AtomicCounter::ValueType value)
{
	__sync_lock_test_and_set(&_counter, value);
	return *this;
}


#else
//
// Generic implementation based on FastMutex
//
AtomicCounter::AtomicCounter()
{
	_counter.value = 0;
}

	
AtomicCounter::AtomicCounter(AtomicCounter::ValueType initialValue)
{
	_counter.value = initialValue;
}


AtomicCounter::AtomicCounter(const AtomicCounter& counter)
{
	_counter.value = counter.value();
}


AtomicCounter::~AtomicCounter()
{
}


AtomicCounter& AtomicCounter::operator = (const AtomicCounter& counter)
{
	FastMutex::ScopedLock lock(_counter.mutex);
	_counter.value = counter.value();
	return *this;
}

	
AtomicCounter& AtomicCounter::operator = (AtomicCounter::ValueType value)
{
	FastMutex::ScopedLock lock(_counter.mutex);
	_counter.value = value;
	return *this;
}


#endif // CHDB_POCO_OS


} // namespace CHDBPoco
