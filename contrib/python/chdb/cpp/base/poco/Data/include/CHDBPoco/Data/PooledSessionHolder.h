//
// PooledSessionHolder.h
//
// Library: Data
// Package: SessionPooling
// Module:  PooledSessionHolder
//
// Definition of the PooledSessionHolder class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Data_PooledSessionHolder_INCLUDED
#define CHDB_Data_PooledSessionHolder_INCLUDED


#include "CHDBPoco/AutoPtr.h"
#include "CHDBPoco/Data/Data.h"
#include "CHDBPoco/Data/SessionImpl.h"
#include "CHDBPoco/Mutex.h"
#include "CHDBPoco/Timestamp.h"


namespace CHDBPoco
{
namespace Data
{


    class SessionPool;


    class Data_API PooledSessionHolder : public CHDBPoco::RefCountedObject
    /// This class is used by SessionPool to manage SessionImpl objects.
    {
    public:
        PooledSessionHolder(SessionPool & owner, SessionImpl * pSessionImpl);
        /// Creates the PooledSessionHolder.

        ~PooledSessionHolder();
        /// Destroys the PooledSessionHolder.

        SessionImpl * session();
        /// Returns a pointer to the SessionImpl.

        SessionPool & owner();
        /// Returns a reference to the SessionHolder's owner.

        void access();
        /// Updates the last access timestamp.

        int idle() const;
        /// Returns the number of seconds the session has not been used.

    private:
        SessionPool & _owner;
        CHDBPoco::AutoPtr<SessionImpl> _pImpl;
        CHDBPoco::Timestamp _lastUsed;
        mutable CHDBPoco::FastMutex _mutex;
    };


    //
    // inlines
    //
    inline SessionImpl * PooledSessionHolder::session()
    {
        return _pImpl;
    }


    inline SessionPool & PooledSessionHolder::owner()
    {
        return _owner;
    }


    inline void PooledSessionHolder::access()
    {
        CHDBPoco::FastMutex::ScopedLock lock(_mutex);

        _lastUsed.update();
    }


    inline int PooledSessionHolder::idle() const
    {
        CHDBPoco::FastMutex::ScopedLock lock(_mutex);

        return (int)(_lastUsed.elapsed() / CHDBPoco::Timestamp::resolution());
    }


}
} // namespace CHDBPoco::Data


#endif // CHDB_Data_PooledSessionHolder_INCLUDED
