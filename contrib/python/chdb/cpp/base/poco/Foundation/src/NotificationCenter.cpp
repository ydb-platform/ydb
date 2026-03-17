//
// NotificationCenter.cpp
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


#include "CHDBPoco/NotificationCenter.h"
#include "CHDBPoco/Notification.h"
#include "CHDBPoco/Observer.h"
#include "CHDBPoco/AutoPtr.h"
#include "CHDBPoco/SingletonHolder.h"


namespace CHDBPoco {


NotificationCenter::NotificationCenter()
{
}


NotificationCenter::~NotificationCenter()
{
}


void NotificationCenter::addObserver(const AbstractObserver& observer)
{
	Mutex::ScopedLock lock(_mutex);
	_observers.push_back(observer.clone());
}


void NotificationCenter::removeObserver(const AbstractObserver& observer)
{
	Mutex::ScopedLock lock(_mutex);
	for (ObserverList::iterator it = _observers.begin(); it != _observers.end(); ++it)
	{
		if (observer.equals(**it))
		{
			(*it)->disable();
			_observers.erase(it);
			return;
		}
	}
}


bool NotificationCenter::hasObserver(const AbstractObserver& observer) const
{
	Mutex::ScopedLock lock(_mutex);
	for (ObserverList::const_iterator it = _observers.begin(); it != _observers.end(); ++it)
		if (observer.equals(**it)) return true;

	return false;
}


void NotificationCenter::postNotification(Notification::Ptr pNotification)
{
	CHDB_poco_check_ptr (pNotification);

	ScopedLockWithUnlock<Mutex> lock(_mutex);
	ObserverList observersToNotify(_observers);
	lock.unlock();
	for (ObserverList::iterator it = observersToNotify.begin(); it != observersToNotify.end(); ++it)
	{
		(*it)->notify(pNotification);
	}
}


bool NotificationCenter::hasObservers() const
{
	Mutex::ScopedLock lock(_mutex);

	return !_observers.empty();
}


std::size_t NotificationCenter::countObservers() const
{
	Mutex::ScopedLock lock(_mutex);

	return _observers.size();
}


namespace
{
	static SingletonHolder<NotificationCenter> sh;
}


NotificationCenter& NotificationCenter::defaultCenter()
{
	return *sh.get();
}


} // namespace CHDBPoco
