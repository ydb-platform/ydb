//
// Semaphore_POSIX.h
//
// Library: Foundation
// Package: Threading
// Module:  Semaphore
//
// Definition of the SemaphoreImpl class for POSIX Threads.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_Semaphore_POSIX_INCLUDED
#define DB_Foundation_Semaphore_POSIX_INCLUDED


#include <errno.h>
#include <pthread.h>
#include "DBPoco/Exception.h"
#include "DBPoco/Foundation.h"


namespace DBPoco
{


class Foundation_API SemaphoreImpl
{
protected:
    SemaphoreImpl(int n, int max);
    ~SemaphoreImpl();
    void setImpl();
    void waitImpl();
    bool waitImpl(long milliseconds);

private:
    volatile int _n;
    int _max;
    pthread_mutex_t _mutex;
    pthread_cond_t _cond;
};


//
// inlines
//
inline void SemaphoreImpl::setImpl()
{
    if (pthread_mutex_lock(&_mutex))
        throw SystemException("cannot signal semaphore (lock)");
    if (_n < _max)
    {
        ++_n;
    }
    else
    {
        pthread_mutex_unlock(&_mutex);
        throw SystemException("cannot signal semaphore: count would exceed maximum");
    }
    if (pthread_cond_signal(&_cond))
    {
        pthread_mutex_unlock(&_mutex);
        throw SystemException("cannot signal semaphore");
    }
    pthread_mutex_unlock(&_mutex);
}


} // namespace DBPoco


#endif // DB_Foundation_Semaphore_POSIX_INCLUDED
