#include "interface/spilling.h"
#include "storage/storage.h"
#include "namespace_cache.h"

#include <map>
#include <deque>
#include <semaphore>
#include <set>


namespace NYql {
namespace NSpilling {

const ui32 DefaultThreadPoolSize = 10;
const ui32 MaxThreadPoolSize = 30;

const ui32 GarbageCollectionPeriodMs = 30000;  // Default timeout for garbage collection activity (milliseconds)
const ui32 MaxGarbageCollectionThreads = 3;    // Maximum number of threads to perform garbage collection

class TNamespacesList {
public:
    TAtomicSharedPtr<TNamespaceCache> GetNamespaceCache(const TString& name);
    void PushNamespaceToProcess(const TString& name, ui32 sessionId);  // Adds particular namespace for async processing
    bool PopNamespaceToProcess(TString& name); // Returns true if there is next namespace for processing. Namespace is returned in TString& name argument.
    bool WaitForTask(); // Waits for the next task for worker thread
    void PerformGarbageCollection(); // Performs garbage collection with GarbageCollectionPeriodMs
    void RemoveNamespaceCache(const TString& name);
    bool CheckForStop();  // Checks if worker thread need to be stopped
    void Stop();  // Signal to stop workers threads
    TSessionDataStat GetSessionDataStat(ui32 SessionId); // Returns statistics for data spilled and loaded during session
    void CloseSession(ui32 SessionId); // Closes session and clears all resources
    TNamespacesList(ui32 threadPoolSize, TAtomicSharedPtr<ISpillStorage> storageI);
private:
    TAdaptiveLock NamespacesMapLock_; // Lock to serialize access to internal map data structures
    TAdaptiveLock QueueLock_; // Lock for NamespacesQueue
    TAdaptiveLock SessionsLock_; // Lock for SessionNamespaces lock
    TAdaptiveLock GarbageCollectionLock_; // Lock for Garbage collection activity
    std::atomic<ui32> ActiveWorkerThreads_ = 0; // Number of current active working threads
    std::atomic<ui32> GarbageCollectionThreads_ = 0; // Number of current active working threads
    std::counting_semaphore<> WorkingSemaphore_{MaxThreadPoolSize}; // Semaphore to wait for the worker threads activation
    ui32 CurrNamespaceId_ = 1; // Current namespace id to assign to new namespace
    std::map<TString, TAtomicSharedPtr<TNamespaceCache>> NamespacesMap_; // Map to find required namespace cache by name
    std::map<ui32, std::set<TString> > SessionNamespaces_; // Set of all namespace names for particular session
    std::deque<TString> NamespacesQueue_; // Queue to process namespaces with pending tasks by thread pool
    bool StopFlag_ = false;
    TAtomicSharedPtr<ISpillStorage> StorageI_; // Interface to storage to work with namespace
    std::chrono::steady_clock::time_point LastGarbageCollection_; // Last time when namespace garbage collection was performed
    std::vector<TString> NamespacesForGarbageCollection_; // List of namespaces to perform garbage collection

};
    

}
}
