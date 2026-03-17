//
// NamedMutex.h
//
// Library: Foundation
// Package: Processes
// Module:  NamedMutex
//
// Definition of the NamedMutex class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_NamedMutex_INCLUDED
#define DB_Foundation_NamedMutex_INCLUDED


#include "DBPoco/Foundation.h"
#include "DBPoco/ScopedLock.h"


#if   DB_POCO_OS == DB_POCO_OS_ANDROID
#    error #include "DBPoco/NamedMutex_Android.h"
#elif defined(DB_POCO_OS_FAMILY_UNIX)
#    include "DBPoco/NamedMutex_UNIX.h"
#endif


namespace DBPoco
{


class Foundation_API NamedMutex : private NamedMutexImpl
/// A NamedMutex (mutual exclusion) is a global synchronization
/// mechanism used to control access to a shared resource
/// in a concurrent (multi process) scenario.
/// Using the ScopedLock class is the preferred way to automatically
/// lock and unlock a mutex.
///
/// Unlike a Mutex or a FastMutex, which itself is the unit of synchronization,
/// a NamedMutex refers to a named operating system resource being the
/// unit of synchronization.
/// In other words, there can be multiple instances of NamedMutex referring
/// to the same actual synchronization object.
///
///
/// There should not be more than one instance of NamedMutex for
/// a given name in a process. Otherwise, the instances may
/// interfere with each other.
{
public:
    typedef DBPoco::ScopedLock<NamedMutex> ScopedLock;

    NamedMutex(const std::string & name);
    /// creates the Mutex.

    ~NamedMutex();
    /// destroys the Mutex.

    void lock();
    /// Locks the mutex. Blocks if the mutex
    /// is held by another process or thread.

    bool tryLock();
    /// Tries to lock the mutex. Returns false immediately
    /// if the mutex is already held by another process or thread.
    /// Returns true if the mutex was successfully locked.

    void unlock();
    /// Unlocks the mutex so that it can be acquired by
    /// other threads.

private:
    NamedMutex();
    NamedMutex(const NamedMutex &);
    NamedMutex & operator=(const NamedMutex &);
};


//
// inlines
//
inline void NamedMutex::lock()
{
    lockImpl();
}


inline bool NamedMutex::tryLock()
{
    return tryLockImpl();
}


inline void NamedMutex::unlock()
{
    unlockImpl();
}


} // namespace DBPoco


#endif // DB_Foundation_NamedMutex_INCLUDED
