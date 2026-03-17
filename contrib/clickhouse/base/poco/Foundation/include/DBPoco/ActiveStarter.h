//
// ActiveStarter.h
//
// Library: Foundation
// Package: Threading
// Module:  ActiveObjects
//
// Definition of the ActiveStarter class.
//
// Copyright (c) 2006-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_ActiveStarter_INCLUDED
#define DB_Foundation_ActiveStarter_INCLUDED


#include "DBPoco/ActiveRunnable.h"
#include "DBPoco/Foundation.h"
#include "DBPoco/ThreadPool.h"


namespace DBPoco
{


template <class OwnerType>
class ActiveStarter
/// The default implementation of the StarterType
/// policy for ActiveMethod. It starts the method
/// in its own thread, obtained from the default
/// thread pool.
{
public:
    static void start(OwnerType * /*pOwner*/, ActiveRunnableBase::Ptr pRunnable)
    {
        ThreadPool::defaultPool().start(*pRunnable);
        pRunnable->duplicate(); // The runnable will release itself.
    }
};


} // namespace DBPoco


#endif // DB_Foundation_ActiveStarter_INCLUDED
