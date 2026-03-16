/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
 *          Min Chen <chenm003@163.com>
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

#include "common.h"
#include "threadpool.h"
#include "threading.h"

#include <new>

#if defined(_WIN32_WINNT) && _WIN32_WINNT >= _WIN32_WINNT_WIN7
#include <winnt.h>
#endif

#if X86_64

#ifdef __GNUC__

#define SLEEPBITMAP_CTZ(id, x)     id = (unsigned long)__builtin_ctzll(x)
#define SLEEPBITMAP_OR(ptr, mask)  __sync_fetch_and_or(ptr, mask)
#define SLEEPBITMAP_AND(ptr, mask) __sync_fetch_and_and(ptr, mask)

#elif defined(_MSC_VER)

#define SLEEPBITMAP_CTZ(id, x)     _BitScanForward64(&id, x)
#define SLEEPBITMAP_OR(ptr, mask)  InterlockedOr64((volatile LONG64*)ptr, (LONG)mask)
#define SLEEPBITMAP_AND(ptr, mask) InterlockedAnd64((volatile LONG64*)ptr, (LONG)mask)

#endif // ifdef __GNUC__

#else

/* use 32-bit primitives defined in threading.h */
#define SLEEPBITMAP_CTZ CTZ
#define SLEEPBITMAP_OR  ATOMIC_OR
#define SLEEPBITMAP_AND ATOMIC_AND

#endif

/* TODO FIX: Macro __MACH__ ideally should be part of MacOS definition, but adding to Cmake
   behaving is not as expected, need to fix this. */

#if MACOS && __MACH__
#include <sys/param.h>
#include <sys/sysctl.h>
#endif
#if HAVE_LIBNUMA
#include <numa.h> // Y_IGNORE
#endif
#if defined(_MSC_VER)
# define strcasecmp _stricmp
#endif

#if defined(_WIN32_WINNT) && _WIN32_WINNT >= _WIN32_WINNT_WIN7
const uint64_t m1 = 0x5555555555555555; //binary: 0101...
const uint64_t m2 = 0x3333333333333333; //binary: 00110011..
const uint64_t m3 = 0x0f0f0f0f0f0f0f0f; //binary:  4 zeros,  4 ones ...
const uint64_t h01 = 0x0101010101010101; //the sum of 256 to the power of 0,1,2,3...

static int popCount(uint64_t x)
{
    x -= (x >> 1) & m1;
    x = (x & m2) + ((x >> 2) & m2);
    x = (x + (x >> 4)) & m3;
    return (x * h01) >> 56;
}
#endif

namespace X265_NS {
// x265 private namespace

class WorkerThread : public Thread
{
private:

    ThreadPool&  m_pool;
    int          m_id;
    Event        m_wakeEvent;

    WorkerThread& operator =(const WorkerThread&);

public:

    JobProvider*     m_curJobProvider;
    BondedTaskGroup* m_bondMaster;

    WorkerThread(ThreadPool& pool, int id) : m_pool(pool), m_id(id) {}
    virtual ~WorkerThread() {}

    void threadMain();
    void awaken()           { m_wakeEvent.trigger(); }
};

void WorkerThread::threadMain()
{
    THREAD_NAME("Worker", m_id);

#if _WIN32
    SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_BELOW_NORMAL);
#else
    __attribute__((unused)) int val = nice(10);
#endif

    m_pool.setCurrentThreadAffinity();

    sleepbitmap_t idBit = (sleepbitmap_t)1 << m_id;
    m_curJobProvider = m_pool.m_jpTable[0];
    m_bondMaster = NULL;

    SLEEPBITMAP_OR(&m_curJobProvider->m_ownerBitmap, idBit);
    SLEEPBITMAP_OR(&m_pool.m_sleepBitmap, idBit);
    m_wakeEvent.wait();

    while (m_pool.m_isActive)
    {
        if (m_bondMaster)
        {
            m_bondMaster->processTasks(m_id);
            m_bondMaster->m_exitedPeerCount.incr();
            m_bondMaster = NULL;
        }

        do
        {
            /* do pending work for current job provider */
            m_curJobProvider->findJob(m_id);

            /* if the current job provider still wants help, only switch to a
             * higher priority provider (lower slice type). Else take the first
             * available job provider with the highest priority */
            int curPriority = (m_curJobProvider->m_helpWanted) ? m_curJobProvider->m_sliceType :
                                                                 INVALID_SLICE_PRIORITY + 1;
            int nextProvider = -1;
            for (int i = 0; i < m_pool.m_numProviders; i++)
            {
                if (m_pool.m_jpTable[i]->m_helpWanted &&
                    m_pool.m_jpTable[i]->m_sliceType < curPriority)
                {
                    nextProvider = i;
                    curPriority = m_pool.m_jpTable[i]->m_sliceType;
                }
            }
            if (nextProvider != -1 && m_curJobProvider != m_pool.m_jpTable[nextProvider])
            {
                SLEEPBITMAP_AND(&m_curJobProvider->m_ownerBitmap, ~idBit);
                m_curJobProvider = m_pool.m_jpTable[nextProvider];
                SLEEPBITMAP_OR(&m_curJobProvider->m_ownerBitmap, idBit);
            }
        }
        while (m_curJobProvider->m_helpWanted);

        /* While the worker sleeps, a job-provider or bond-group may acquire this
         * worker's sleep bitmap bit. Once acquired, that thread may modify 
         * m_bondMaster or m_curJobProvider, then waken the thread */
        SLEEPBITMAP_OR(&m_pool.m_sleepBitmap, idBit);
        m_wakeEvent.wait();
    }

    SLEEPBITMAP_OR(&m_pool.m_sleepBitmap, idBit);
}

void JobProvider::tryWakeOne()
{
    int id = m_pool->tryAcquireSleepingThread(m_ownerBitmap, ALL_POOL_THREADS);
    if (id < 0)
    {
        m_helpWanted = true;
        return;
    }

    WorkerThread& worker = m_pool->m_workers[id];
    if (worker.m_curJobProvider != this) /* poaching */
    {
        sleepbitmap_t bit = (sleepbitmap_t)1 << id;
        SLEEPBITMAP_AND(&worker.m_curJobProvider->m_ownerBitmap, ~bit);
        worker.m_curJobProvider = this;
        SLEEPBITMAP_OR(&worker.m_curJobProvider->m_ownerBitmap, bit);
    }
    worker.awaken();
}

int ThreadPool::tryAcquireSleepingThread(sleepbitmap_t firstTryBitmap, sleepbitmap_t secondTryBitmap)
{
    unsigned long id;

    sleepbitmap_t masked = m_sleepBitmap & firstTryBitmap;
    while (masked)
    {
        SLEEPBITMAP_CTZ(id, masked);

        sleepbitmap_t bit = (sleepbitmap_t)1 << id;
        if (SLEEPBITMAP_AND(&m_sleepBitmap, ~bit) & bit)
            return (int)id;

        masked = m_sleepBitmap & firstTryBitmap;
    }

    masked = m_sleepBitmap & secondTryBitmap;
    while (masked)
    {
        SLEEPBITMAP_CTZ(id, masked);

        sleepbitmap_t bit = (sleepbitmap_t)1 << id;
        if (SLEEPBITMAP_AND(&m_sleepBitmap, ~bit) & bit)
            return (int)id;

        masked = m_sleepBitmap & secondTryBitmap;
    }

    return -1;
}

int ThreadPool::tryBondPeers(int maxPeers, sleepbitmap_t peerBitmap, BondedTaskGroup& master)
{
    int bondCount = 0;
    do
    {
        int id = tryAcquireSleepingThread(peerBitmap, 0);
        if (id < 0)
            return bondCount;

        m_workers[id].m_bondMaster = &master;
        m_workers[id].awaken();
        bondCount++;
    }
    while (bondCount < maxPeers);

    return bondCount;
}
ThreadPool* ThreadPool::allocThreadPools(x265_param* p, int& numPools, bool isThreadsReserved)
{
    enum { MAX_NODE_NUM = 127 };
    int cpusPerNode[MAX_NODE_NUM + 1];
    int threadsPerPool[MAX_NODE_NUM + 2];
    uint64_t nodeMaskPerPool[MAX_NODE_NUM + 2];
    int totalNumThreads = 0;

    memset(cpusPerNode, 0, sizeof(cpusPerNode));
    memset(threadsPerPool, 0, sizeof(threadsPerPool));
    memset(nodeMaskPerPool, 0, sizeof(nodeMaskPerPool));

    int numNumaNodes = X265_MIN(getNumaNodeCount(), MAX_NODE_NUM);
    bool bNumaSupport = false;

#if defined(_WIN32_WINNT) && _WIN32_WINNT >= _WIN32_WINNT_WIN7 
    bNumaSupport = true;
#elif HAVE_LIBNUMA
    bNumaSupport = numa_available() >= 0;
#endif


#if defined(_WIN32_WINNT) && _WIN32_WINNT >= _WIN32_WINNT_WIN7
    PGROUP_AFFINITY groupAffinityPointer = new GROUP_AFFINITY;
    for (int i = 0; i < numNumaNodes; i++)
    {
        GetNumaNodeProcessorMaskEx((UCHAR)i, groupAffinityPointer);
        cpusPerNode[i] = popCount(groupAffinityPointer->Mask);
    }
    delete groupAffinityPointer;
#elif HAVE_LIBNUMA
    if (bNumaSupport)
    {
        struct bitmask* bitMask = numa_allocate_cpumask();
        for (int i = 0; i < numNumaNodes; i++)
        {
            int ret = numa_node_to_cpus(i, bitMask);
            if (!ret)
                cpusPerNode[i] = numa_bitmask_weight(bitMask);
            else
                x265_log(p, X265_LOG_ERROR, "Failed to genrate CPU mask\n");
        }
        numa_free_cpumask(bitMask);
    }
#else // NUMA not supported
    cpusPerNode[0] = getCpuCount();
#endif

    if (bNumaSupport && p->logLevel >= X265_LOG_DEBUG)
    for (int i = 0; i < numNumaNodes; i++)
        x265_log(p, X265_LOG_DEBUG, "detected NUMA node %d with %d logical cores\n", i, cpusPerNode[i]);
    /* limit threads based on param->numaPools
     * For windows because threads can't be allocated to live across sockets
     * changing the default behavior to be per-socket pools -- FIXME */
#if defined(_WIN32_WINNT) && _WIN32_WINNT >= _WIN32_WINNT_WIN7
    if (!p->numaPools || (strcmp(p->numaPools, "NULL") == 0 || strcmp(p->numaPools, "*") == 0 || strcmp(p->numaPools, "") == 0))
    {
         char poolString[50] = "";
         for (int i = 0; i < numNumaNodes; i++)
         {
             char nextCount[10] = "";
             if (i)
                 sprintf(nextCount, ",%d", cpusPerNode[i]);
             else
                   sprintf(nextCount, "%d", cpusPerNode[i]);
             strcat(poolString, nextCount);
         }
         x265_param_parse(p, "pools", poolString);
     }
#endif
    if (p->numaPools && *p->numaPools)
    {
        const char *nodeStr = p->numaPools;
        for (int i = 0; i < numNumaNodes; i++)
        {
            if (!*nodeStr)
            {
                threadsPerPool[i] = 0;
                continue;
            }
            else if (*nodeStr == '-')
                threadsPerPool[i] = 0;
            else if (*nodeStr == '*' || !strcasecmp(nodeStr, "NULL"))
            {
                for (int j = i; j < numNumaNodes; j++)
                {
                    threadsPerPool[numNumaNodes] += cpusPerNode[j];
                    nodeMaskPerPool[numNumaNodes] |= ((uint64_t)1 << j);
                }
                break;
            }
            else if (*nodeStr == '+')
            {
                threadsPerPool[numNumaNodes] += cpusPerNode[i];
                nodeMaskPerPool[numNumaNodes] |= ((uint64_t)1 << i);
            }
            else
            {
                int count = atoi(nodeStr);
                if (i > 0 || strchr(nodeStr, ','))   // it is comma -> old logic
                {
                    threadsPerPool[i] = X265_MIN(count, cpusPerNode[i]);
                    nodeMaskPerPool[i] = ((uint64_t)1 << i);
                }
                else                                 // new logic: exactly 'count' threads on all NUMAs
                {
                    threadsPerPool[numNumaNodes] = X265_MIN(count, numNumaNodes * MAX_POOL_THREADS);
                    nodeMaskPerPool[numNumaNodes] = ((uint64_t)-1 >> (64 - numNumaNodes));
                }
            }

            /* consume current node string, comma, and white-space */
            while (*nodeStr && *nodeStr != ',')
               ++nodeStr;
            if (*nodeStr == ',' || *nodeStr == ' ')
               ++nodeStr;
        }
    }
    else
    {
        for (int i = 0; i < numNumaNodes; i++)
        {
            threadsPerPool[numNumaNodes]  += cpusPerNode[i];
            nodeMaskPerPool[numNumaNodes] |= ((uint64_t)1 << i);
        }
    }
 
    // If the last pool size is > MAX_POOL_THREADS, clip it to spawn thread pools only of size >= 1/2 max (heuristic)
    if ((threadsPerPool[numNumaNodes] > MAX_POOL_THREADS) &&
        ((threadsPerPool[numNumaNodes] % MAX_POOL_THREADS) < (MAX_POOL_THREADS / 2)))
    {
        threadsPerPool[numNumaNodes] -= (threadsPerPool[numNumaNodes] % MAX_POOL_THREADS);
        x265_log(p, X265_LOG_DEBUG,
                 "Creating only %d worker threads beyond specified numbers with --pools (if specified) to prevent asymmetry in pools; may not use all HW contexts\n", threadsPerPool[numNumaNodes]);
    }

    numPools = 0;
    for (int i = 0; i < numNumaNodes + 1; i++)
    {
        if (bNumaSupport)
            x265_log(p, X265_LOG_DEBUG, "NUMA node %d may use %d logical cores\n", i, cpusPerNode[i]);
        if (threadsPerPool[i])
        {
            numPools += (threadsPerPool[i] + MAX_POOL_THREADS - 1) / MAX_POOL_THREADS;
            totalNumThreads += threadsPerPool[i];
        }
    }
    if (!isThreadsReserved)
    {
        if (!numPools)
        {
            x265_log(p, X265_LOG_DEBUG, "No pool thread available. Deciding frame-threads based on detected CPU threads\n");
            totalNumThreads = ThreadPool::getCpuCount(); // auto-detect frame threads
        }

        if (!p->frameNumThreads)
            ThreadPool::getFrameThreadsCount(p, totalNumThreads);
    }
    
    if (!numPools)
        return NULL;

    if (numPools > p->frameNumThreads)
    {
        x265_log(p, X265_LOG_DEBUG, "Reducing number of thread pools for frame thread count\n");
        numPools = X265_MAX(p->frameNumThreads / 2, 1);
    }
    if (isThreadsReserved)
        numPools = 1;
    ThreadPool *pools = new ThreadPool[numPools];
    if (pools)
    {
        int maxProviders = (p->frameNumThreads + numPools - 1) / numPools + !isThreadsReserved; /* +1 is Lookahead, always assigned to threadpool 0 */
        int node = 0;
        for (int i = 0; i < numPools; i++)
        {
            while (!threadsPerPool[node])
                node++;
            int numThreads = X265_MIN(MAX_POOL_THREADS, threadsPerPool[node]);
            int origNumThreads = numThreads;
            if (i == 0 && p->lookaheadThreads > numThreads / 2)
            {
                p->lookaheadThreads = numThreads / 2;
                x265_log(p, X265_LOG_DEBUG, "Setting lookahead threads to a maximum of half the total number of threads\n");
            }
            if (isThreadsReserved)
            {
                numThreads = p->lookaheadThreads;
                maxProviders = 1;
            }

            else if (i == 0)
                numThreads -= p->lookaheadThreads;
            if (!pools[i].create(numThreads, maxProviders, nodeMaskPerPool[node]))
            {
                X265_FREE(pools);
                numPools = 0;
                return NULL;
            }
            if (numNumaNodes > 1)
            {
                char *nodesstr = new char[64 * strlen(",63") + 1];
                int len = 0;
                for (int j = 0; j < 64; j++)
                    if ((nodeMaskPerPool[node] >> j) & 1)
                        len += sprintf(nodesstr + len, ",%d", j);
                x265_log(p, X265_LOG_INFO, "Thread pool %d using %d threads on numa nodes %s\n", i, numThreads, nodesstr + 1);
                delete[] nodesstr;
            }
            else
                x265_log(p, X265_LOG_INFO, "Thread pool created using %d threads\n", numThreads);
            threadsPerPool[node] -= origNumThreads;
        }
    }
    else
        numPools = 0;
    return pools;
}

ThreadPool::ThreadPool()
{
    memset(this, 0, sizeof(*this));
}

bool ThreadPool::create(int numThreads, int maxProviders, uint64_t nodeMask)
{
    X265_CHECK(numThreads <= MAX_POOL_THREADS, "a single thread pool cannot have more than MAX_POOL_THREADS threads\n");

#if defined(_WIN32_WINNT) && _WIN32_WINNT >= _WIN32_WINNT_WIN7 
    memset(&m_groupAffinity, 0, sizeof(GROUP_AFFINITY));
    for (int i = 0; i < getNumaNodeCount(); i++)
    {
        int numaNode = ((nodeMask >> i) & 0x1U) ? i : -1;
        if (numaNode != -1)
        if (GetNumaNodeProcessorMaskEx((USHORT)numaNode, &m_groupAffinity))
            break;
    }
    m_numaMask = &m_groupAffinity.Mask;
#elif HAVE_LIBNUMA
    if (numa_available() >= 0)
    {
        struct bitmask* nodemask = numa_allocate_nodemask();
        if (nodemask)
        {
            *(nodemask->maskp) = nodeMask;
            m_numaMask = nodemask;
        }
        else
            x265_log(NULL, X265_LOG_ERROR, "unable to get NUMA node mask for %lx\n", nodeMask);
    }
#else
    (void)nodeMask;
#endif

    m_numWorkers = numThreads;

    m_workers = X265_MALLOC(WorkerThread, numThreads);
    /* placement new initialization */
    if (m_workers)
        for (int i = 0; i < numThreads; i++)
            new (m_workers + i)WorkerThread(*this, i);

    m_jpTable = X265_MALLOC(JobProvider*, maxProviders);
    m_numProviders = 0;

    return m_workers && m_jpTable;
}

bool ThreadPool::start()
{
    m_isActive = true;
    for (int i = 0; i < m_numWorkers; i++)
    {
        if (!m_workers[i].start())
        {
            m_isActive = false;
            return false;
        }
    }
    return true;
}

void ThreadPool::stopWorkers()
{
    if (m_workers)
    {
        m_isActive = false;
        for (int i = 0; i < m_numWorkers; i++)
        {
            while (!(m_sleepBitmap & ((sleepbitmap_t)1 << i)))
                GIVE_UP_TIME();
            m_workers[i].awaken();
            m_workers[i].stop();
        }
    }
}

ThreadPool::~ThreadPool()
{
    if (m_workers)
    {
        for (int i = 0; i < m_numWorkers; i++)
            m_workers[i].~WorkerThread();
    }

    X265_FREE(m_workers);
    X265_FREE(m_jpTable);

#if HAVE_LIBNUMA
    if(m_numaMask)
        numa_free_nodemask((struct bitmask*)m_numaMask);
#endif
}

void ThreadPool::setCurrentThreadAffinity()
{
    setThreadNodeAffinity(m_numaMask);
}

void ThreadPool::setThreadNodeAffinity(void *numaMask)
{
#if defined(_WIN32_WINNT) && _WIN32_WINNT >= _WIN32_WINNT_WIN7 
    UNREFERENCED_PARAMETER(numaMask);
    GROUP_AFFINITY groupAffinity;
    memset(&groupAffinity, 0, sizeof(GROUP_AFFINITY));
    groupAffinity.Group = m_groupAffinity.Group;
    groupAffinity.Mask = m_groupAffinity.Mask;
    const PGROUP_AFFINITY affinityPointer = &groupAffinity;
    if (SetThreadGroupAffinity(GetCurrentThread(), affinityPointer, NULL))
        return;
    else
        x265_log(NULL, X265_LOG_ERROR, "unable to set thread affinity for NUMA node mask\n");
#elif HAVE_LIBNUMA
    if (numa_available() >= 0)
    {
        numa_run_on_node_mask((struct bitmask*)numaMask);
        numa_set_interleave_mask((struct bitmask*)numaMask);
        numa_set_localalloc();
        return;
    }
    x265_log(NULL, X265_LOG_ERROR, "unable to set thread affinity for NUMA node mask\n");
#else
    (void)numaMask;
#endif
    return;
}

/* static */
int ThreadPool::getNumaNodeCount()
{
#if defined(_WIN32_WINNT) && _WIN32_WINNT >= _WIN32_WINNT_WIN7 
    ULONG num = 1;
    if (GetNumaHighestNodeNumber(&num))
        num++;
    return (int)num;
#elif HAVE_LIBNUMA
    if (numa_available() >= 0)
        return numa_max_node() + 1;
    else
        return 1;
#else
    return 1;
#endif
}

/* static */
int ThreadPool::getCpuCount()
{
#if defined(_WIN32_WINNT) && _WIN32_WINNT >= _WIN32_WINNT_WIN7
    enum { MAX_NODE_NUM = 127 };
    int cpus = 0;
    int numNumaNodes = X265_MIN(getNumaNodeCount(), MAX_NODE_NUM);
    GROUP_AFFINITY groupAffinity;
    for (int i = 0; i < numNumaNodes; i++)
    {
        GetNumaNodeProcessorMaskEx((UCHAR)i, &groupAffinity);
        cpus += popCount(groupAffinity.Mask);
    }
    return cpus;
#elif _WIN32
    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    return sysinfo.dwNumberOfProcessors;
#elif __unix__ && X265_ARCH_ARM
    /* Return the number of processors configured by OS. Because, most embedded linux distributions
     * uses only one processor as the scheduler doesn't have enough work to utilize all processors */
    return sysconf(_SC_NPROCESSORS_CONF);
#elif __unix__
    return sysconf(_SC_NPROCESSORS_ONLN);
#elif MACOS && __MACH__
    int nm[2];
    size_t len = 4;
    uint32_t count;

    nm[0] = CTL_HW;
    nm[1] = HW_AVAILCPU;
    sysctl(nm, 2, &count, &len, NULL, 0);

    if (count < 1)
    {
        nm[1] = HW_NCPU;
        sysctl(nm, 2, &count, &len, NULL, 0);
        if (count < 1)
            count = 1;
    }

    return count;
#else
    return 2; // default to 2 threads, everywhere else
#endif
}

void ThreadPool::getFrameThreadsCount(x265_param* p, int cpuCount)
{
    int rows = (p->sourceHeight + p->maxCUSize - 1) >> g_log2Size[p->maxCUSize];
    if (!p->bEnableWavefront)
        p->frameNumThreads = X265_MIN3(cpuCount, (rows + 1) / 2, X265_MAX_FRAME_THREADS);
    else if (cpuCount >= 32)
        p->frameNumThreads = (p->sourceHeight > 2000) ? 6 : 5; 
    else if (cpuCount >= 16)
        p->frameNumThreads = 4; 
    else if (cpuCount >= 8)
        p->frameNumThreads = 3;
    else if (cpuCount >= 4)
        p->frameNumThreads = 2;
    else
        p->frameNumThreads = 1;
}

} // end namespace X265_NS
