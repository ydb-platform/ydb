//
// NamedEvent_UNIX.h
//
// Library: Foundation
// Package: Processes
// Module:  NamedEvent
//
// Definition of the NamedEventImpl class for Unix.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_NamedEvent_UNIX_INCLUDED
#define DB_Foundation_NamedEvent_UNIX_INCLUDED


#include "DBPoco/Foundation.h"
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
#    include <semaphore.h>
#endif


namespace DBPoco
{


class Foundation_API NamedEventImpl
{
protected:
    NamedEventImpl(const std::string & name);
    ~NamedEventImpl();
    void setImpl();
    void waitImpl();

private:
    std::string getFileName();

    std::string _name;
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
    sem_t * _sem;
#else
    int _semid; // semaphore id
#endif
};


} // namespace DBPoco


#endif // DB_Foundation_NamedEvent_UNIX_INCLUDED
