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


#ifndef DB_Net_TCPServerDispatcher_INCLUDED
#define DB_Net_TCPServerDispatcher_INCLUDED

#include <atomic>

#include "DBPoco/Mutex.h"
#include "DBPoco/Net/Net.h"
#include "DBPoco/Net/StreamSocket.h"
#include "DBPoco/Net/TCPServerConnectionFactory.h"
#include "DBPoco/Net/TCPServerParams.h"
#include "DBPoco/NotificationQueue.h"
#include "DBPoco/Runnable.h"
#include "DBPoco/ThreadPool.h"


namespace DBPoco
{
namespace Net
{


    class Net_API TCPServerDispatcher : public DBPoco::Runnable
    /// A helper class for TCPServer that dispatches
    /// connections to server connection threads.
    {
    public:
        TCPServerDispatcher(TCPServerConnectionFactory::Ptr pFactory, DBPoco::ThreadPool & threadPool, TCPServerParams::Ptr pParams);
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
        DBPoco::NotificationQueue _queue;
        TCPServerConnectionFactory::Ptr _pConnectionFactory;
        DBPoco::ThreadPool & _threadPool;
        mutable DBPoco::FastMutex _mutex;
    };


    //
    // inlines
    //
    inline const TCPServerParams & TCPServerDispatcher::params() const
    {
        return *_pParams;
    }


}
} // namespace DBPoco::Net


#endif // DB_Net_TCPServerDispatcher_INCLUDED
