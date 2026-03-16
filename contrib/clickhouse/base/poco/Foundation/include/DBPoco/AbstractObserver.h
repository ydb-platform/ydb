//
// AbstractObserver.h
//
// Library: Foundation
// Package: Notifications
// Module:  NotificationCenter
//
// Definition of the AbstractObserver class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_AbstractObserver_INCLUDED
#define DB_Foundation_AbstractObserver_INCLUDED


#include "DBPoco/Foundation.h"
#include "DBPoco/Notification.h"


namespace DBPoco
{


class Foundation_API AbstractObserver
/// The base class for all instantiations of
/// the Observer and NObserver template classes.
{
public:
    AbstractObserver();
    AbstractObserver(const AbstractObserver & observer);
    virtual ~AbstractObserver();

    AbstractObserver & operator=(const AbstractObserver & observer);

    virtual void notify(Notification * pNf) const = 0;
    virtual bool equals(const AbstractObserver & observer) const = 0;
    virtual bool accepts(Notification * pNf) const = 0;
    virtual AbstractObserver * clone() const = 0;
    virtual void disable() = 0;
};


} // namespace DBPoco


#endif // DB_Foundation_AbstractObserver_INCLUDED
