//
// TCPServerParams.h
//
// Library: Net
// Package: TCPServer
// Module:  TCPServerParams
//
// Definition of the TCPServerParams class.
//
// Copyright (c) 2005-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Net_TCPServerParams_INCLUDED
#define CHDB_Net_TCPServerParams_INCLUDED


#include "CHDBPoco/AutoPtr.h"
#include "CHDBPoco/Net/Net.h"
#include "CHDBPoco/RefCountedObject.h"
#include "CHDBPoco/Thread.h"
#include "CHDBPoco/Timespan.h"


namespace CHDBPoco
{
namespace Net
{


    class Net_API TCPServerParams : public CHDBPoco::RefCountedObject
    /// This class is used to specify parameters to both the
    /// TCPServer, as well as to TCPServerDispatcher objects.
    ///
    /// Subclasses may add new parameters to the class.
    {
    public:
        typedef CHDBPoco::AutoPtr<TCPServerParams> Ptr;

        TCPServerParams();
        /// Creates the TCPServerParams.
        ///
        /// Sets the following default values:
        ///   - threadIdleTime:       10 seconds
        ///   - maxThreads:           0
        ///   - maxQueued:            64

        void setThreadIdleTime(const CHDBPoco::Timespan & idleTime);
        /// Sets the maximum idle time for a thread before
        /// it is terminated.
        ///
        /// The default idle time is 10 seconds;

        const CHDBPoco::Timespan & getThreadIdleTime() const;
        /// Returns the maximum thread idle time.

        void setMaxQueued(int count);
        /// Sets the maximum number of queued connections.
        /// Must be greater than 0.
        ///
        /// If there are already the maximum number of connections
        /// in the queue, new connections will be silently discarded.
        ///
        /// The default number is 64.

        int getMaxQueued() const;
        /// Returns the maximum number of queued connections.

        void setMaxThreads(int count);
        /// Sets the maximum number of simultaneous threads
        /// available for this TCPServerDispatcher.
        ///
        /// Must be greater than or equal to 0.
        /// If 0 is specified, the TCPServerDispatcher will
        /// set this parameter to the number of available threads
        /// in its thread pool.
        ///
        /// The thread pool used by the TCPServerDispatcher
        /// must at least have the capacity for the given
        /// number of threads.

        int getMaxThreads() const;
        /// Returns the maximum number of simultaneous threads
        /// available for this TCPServerDispatcher.

        void setThreadPriority(CHDBPoco::Thread::Priority prio);
        /// Sets the priority of TCP server threads
        /// created by TCPServer.

        CHDBPoco::Thread::Priority getThreadPriority() const;
        /// Returns the priority of TCP server threads
        /// created by TCPServer.

    protected:
        virtual ~TCPServerParams();
        /// Destroys the TCPServerParams.

    private:
        CHDBPoco::Timespan _threadIdleTime;
        int _maxThreads;
        int _maxQueued;
        CHDBPoco::Thread::Priority _threadPriority;
    };


    //
    // inlines
    //
    inline const CHDBPoco::Timespan & TCPServerParams::getThreadIdleTime() const
    {
        return _threadIdleTime;
    }


    inline int TCPServerParams::getMaxThreads() const
    {
        return _maxThreads;
    }


    inline int TCPServerParams::getMaxQueued() const
    {
        return _maxQueued;
    }


    inline CHDBPoco::Thread::Priority TCPServerParams::getThreadPriority() const
    {
        return _threadPriority;
    }


}
} // namespace CHDBPoco::Net


#endif // CHDB_Net_TCPServerParams_INCLUDED
