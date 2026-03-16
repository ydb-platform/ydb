//
// TCPServerDispatcher.h
//
// Library: Net
// Package: TCPServer
// Module:  TCPServerDispatcher
//
// Definition of the TCPServerDispatcher class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Net_TCPServerDispatcher_INCLUDED
#define CHDB_Net_TCPServerDispatcher_INCLUDED

#include <atomic>

#include "CHDBPoco/Mutex.h"
#include "CHDBPoco/Net/Net.h"
#include "CHDBPoco/Net/StreamSocket.h"
#include "CHDBPoco/Net/TCPServerConnectionFactory.h"
#include "CHDBPoco/Net/TCPServerParams.h"
#include "CHDBPoco/NotificationQueue.h"
#include "CHDBPoco/Runnable.h"
#include "CHDBPoco/ThreadPool.h"


namespace CHDBPoco
{
namespace Net
{


    class Net_API TCPServerDispatcher : public CHDBPoco::Runnable
    /// A helper class for TCPServer that dispatches
    /// connections to server connection threads.
    {
    public:
        TCPServerDispatcher(TCPServerConnectionFactory::Ptr pFactory, CHDBPoco::ThreadPool & threadPool, TCPServerParams::Ptr pParams);
        /// Creates the TCPServerDispatcher.
        ///
        /// The dispatcher takes ownership of the TCPServerParams object.
        /// If no TCPServerParams object is supplied, the TCPServerDispatcher
        /// creates one.

        void duplicate();
        /// Increments the object's reference count.

        void release();
        /// Decrements the object's reference count
        /// and deletes the object if the count
        /// reaches zero.

        void run();
        /// Runs the dispatcher.

        void enqueue(const StreamSocket & socket);
        /// Queues the given socket connection.

        void stop();
        /// Stops the dispatcher.

        int currentThreads() const;
        /// Returns the number of currently used threads.

        int maxThreads() const;
        /// Returns the maximum number of threads available.

        int totalConnections() const;
        /// Returns the total number of handled connections.

        int currentConnections() const;
        /// Returns the number of currently handled connections.

        int maxConcurrentConnections() const;
        /// Returns the maximum number of concurrently handled connections.

        int queuedConnections() const;
        /// Returns the number of queued connections.

        int refusedConnections() const;
        /// Returns the number of refused connections.

        const TCPServerParams & params() const;
        /// Returns a const reference to the TCPServerParam object.

    protected:
        ~TCPServerDispatcher();
        /// Destroys the TCPServerDispatcher.

        void beginConnection();
        /// Updates the performance counters.

        void endConnection();
        /// Updates the performance counters.

    private:
        TCPServerDispatcher();
        TCPServerDispatcher(const TCPServerDispatcher &);
        TCPServerDispatcher & operator=(const TCPServerDispatcher &);

        std::atomic<int> _rc;
        TCPServerParams::Ptr _pParams;
        std::atomic<int> _currentThreads;
        std::atomic<int> _totalConnections;
        std::atomic<int> _currentConnections;
        std::atomic<int> _maxConcurrentConnections;
        std::atomic<int> _refusedConnections;
        std::atomic<bool> _stopped;
        CHDBPoco::NotificationQueue _queue;
        TCPServerConnectionFactory::Ptr _pConnectionFactory;
        CHDBPoco::ThreadPool & _threadPool;
        mutable CHDBPoco::FastMutex _mutex;
    };


    //
    // inlines
    //
    inline const TCPServerParams & TCPServerDispatcher::params() const
    {
        return *_pParams;
    }


}
} // namespace CHDBPoco::Net


#endif // CHDB_Net_TCPServerDispatcher_INCLUDED
