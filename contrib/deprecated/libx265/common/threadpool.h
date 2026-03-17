/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02111, USA.
 *
 * This program is also available under a commercial proprietary license.
 * For more information, contact us at license @ x265.com
 *****************************************************************************/

#ifndef X265_THREADPOOL_H
#define X265_THREADPOOL_H

#include "common.h"
#include "threading.h"

namespace X265_NS {
// x265 private namespace

class ThreadPool;
class WorkerThread;
class BondedTaskGroup;

#if X86_64
typedef uint64_t sleepbitmap_t;
#else
typedef uint32_t sleepbitmap_t;
#endif

static const sleepbitmap_t ALL_POOL_THREADS = (sleepbitmap_t)-1;
enum { MAX_POOL_THREADS = sizeof(sleepbitmap_t) * 8 };
enum { INVALID_SLICE_PRIORITY = 10 }; // a value larger than any X265_TYPE_* macro

// Frame level job providers. FrameEncoder and Lookahead derive from
// this class and implement findJob()
class JobProvider
{
public:

    ThreadPool*   m_pool;
    sleepbitmap_t m_ownerBitmap;
    int           m_jpId;
    int           m_sliceType;
    bool          m_helpWanted;
    bool          m_isFrameEncoder; /* rather ugly hack, but nothing better presents itself */

    JobProvider()
        : m_pool(NULL)
        , m_ownerBitmap(0)
        , m_jpId(-1)
        , m_sliceType(INVALID_SLICE_PRIORITY)
        , m_helpWanted(false)
        , m_isFrameEncoder(false)
    {}

    virtual ~JobProvider() {}

    // Worker threads will call this method to perform work
    virtual void findJob(int workerThreadId) = 0;

    // Will awaken one idle thread, preferring a thread which most recently
    // performed work for this provider.
    void tryWakeOne();
};

class ThreadPool
{
public:

    sleepbitmap_t m_sleepBitmap;
    int           m_numProviders;
    int           m_numWorkers;
    void*         m_numaMask; // node mask in linux, cpu mask in windows
#if defined(_WIN32_WINNT) && _WIN32_WINNT >= _WIN32_WINNT_WIN7 
    GROUP_AFFINITY m_groupAffinity;
#endif
    bool          m_isActive;

    JobProvider** m_jpTable;
    WorkerThread* m_workers;

    ThreadPool();
    ~ThreadPool();

    bool create(int numThreads, int maxProviders, uint64_t nodeMask);
    bool start();
    void stopWorkers();
    void setCurrentThreadAffinity();
    void setThreadNodeAffinity(void *numaMask);
    int  tryAcquireSleepingThread(sleepbitmap_t firstTryBitmap, sleepbitmap_t secondTryBitmap);
    int  tryBondPeers(int maxPeers, sleepbitmap_t peerBitmap, BondedTaskGroup& master);
    static ThreadPool* allocThreadPools(x265_param* p, int& numPools, bool isThreadsReserved);
    static int  getCpuCount();
    static int  getNumaNodeCount();
    static void getFrameThreadsCount(x265_param* p,int cpuCount);
};

/* Any worker thread may enlist the help of idle worker threads from the same
 * job provider. They must derive from this class and implement the
 * processTasks() method.  To use, an instance must be instantiated by a worker
 * thread (referred to as the master thread) and then tryBondPeers() must be
 * called. If it returns non-zero then some number of slave worker threads are
 * already in the process of calling your processTasks() function. The master
 * thread should participate and call processTasks() itself. When
 * waitForExit() returns, all bonded peer threads are guaranteed to have
 * exitied processTasks(). Since the thread count is small, it uses explicit
 * locking instead of atomic counters and bitmasks */
class BondedTaskGroup
{
public:

    Lock              m_lock;
    ThreadSafeInteger m_exitedPeerCount;
    int               m_bondedPeerCount;
    int               m_jobTotal;
    int               m_jobAcquired;

    BondedTaskGroup()  { m_bondedPeerCount = m_jobTotal = m_jobAcquired = 0; }

    /* Do not allow the instance to be destroyed before all bonded peers have
     * exited processTasks() */
    ~BondedTaskGroup() { waitForExit(); }

    /* Try to enlist the help of idle worker threads on most recently associated
     * with the given job provider and "bond" them to work on your tasks. Up to
     * maxPeers worker threads will call your processTasks() method. */
    int tryBondPeers(JobProvider& jp, int maxPeers)
    {
        int count = jp.m_pool->tryBondPeers(maxPeers, jp.m_ownerBitmap, *this);
        m_bondedPeerCount += count;
        return count;
    }

    /* Try to enlist the help of any idle worker threads and "bond" them to work
     * on your tasks. Up to maxPeers worker threads will call your
     * processTasks() method. */
    int tryBondPeers(ThreadPool& pool, int maxPeers)
    {
        int count = pool.tryBondPeers(maxPeers, ALL_POOL_THREADS, *this);
        m_bondedPeerCount += count;
        return count;
    }

    /* Returns when all bonded peers have exited processTasks(). It does *NOT*
     * ensure all tasks are completed (but this is generally implied). */
    void waitForExit()
    {
        int exited = m_exitedPeerCount.get();
        while (m_bondedPeerCount != exited)
            exited = m_exitedPeerCount.waitForChange(exited);
    }

    /* Derived classes must define this method. The worker thread ID may be
     * used to index into thread local data, or ignored.  The ID will be between
     * 0 and jp.m_numWorkers - 1 */
    virtual void processTasks(int workerThreadId) = 0;
};

} // end namespace X265_NS

#endif // ifndef X265_THREADPOOL_H
