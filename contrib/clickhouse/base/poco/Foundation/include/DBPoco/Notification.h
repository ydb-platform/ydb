//
// Notification.h
//
// Library: Foundation
// Package: Notifications
// Module:  Notification
//
// Definition of the Notification class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_Notification_INCLUDED
#define DB_Foundation_Notification_INCLUDED


#include "DBPoco/AutoPtr.h"
#include "DBPoco/Foundation.h"
#include "DBPoco/Mutex.h"
#include "DBPoco/RefCountedObject.h"


namespace DBPoco
{


class Foundation_API Notification : public RefCountedObject
/// The base class for all notification classes used
/// with the NotificationCenter and the NotificationQueue
/// classes.
/// The Notification class can be used with the AutoPtr
/// template class.
{
public:
    typedef AutoPtr<Notification> Ptr;

    Notification();
    /// Creates the notification.

    virtual std::string name() const;
    /// Returns the name of the notification.
    /// The default implementation returns the class name.

protected:
    virtual ~Notification();
};


} // namespace DBPoco


#endif // DB_Foundation_Notification_INCLUDED
