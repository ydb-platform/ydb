//
// BasicEvent.h
//
// Library: Foundation
// Package: Events
// Module:  BasicEvent
//
// Implementation of the BasicEvent template.
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_BasicEvent_INCLUDED
#define DB_Foundation_BasicEvent_INCLUDED


#include "DBPoco/AbstractDelegate.h"
#include "DBPoco/AbstractEvent.h"
#include "DBPoco/DefaultStrategy.h"
#include "DBPoco/Mutex.h"


namespace DBPoco
{


template <class TArgs, class TMutex = FastMutex>
class BasicEvent : public AbstractEvent<TArgs, DefaultStrategy<TArgs, AbstractDelegate<TArgs>>, AbstractDelegate<TArgs>, TMutex>
/// A BasicEvent uses the DefaultStrategy which
/// invokes delegates in the order they have been registered.
///
/// Please see the AbstractEvent class template documentation
/// for more information.
{
public:
    BasicEvent() { }

    ~BasicEvent() { }

private:
    BasicEvent(const BasicEvent & e);
    BasicEvent & operator=(const BasicEvent & e);
};


} // namespace DBPoco


#endif // DB_Foundation_BasicEvent_INCLUDED
