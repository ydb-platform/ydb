#pragma once 
#include <util/generic/hash.h> 
#include <util/generic/hash_set.h> 
#include <util/generic/string.h> 
#include <util/system/spinlock.h> 
 
namespace NActors { 
class IActor; 
} // namespace NActors 
 
namespace NKikimr::NSQS { 
namespace NDebug { 
 
// Multimap for per request objects. 
// Provides simple deletion by key and value functionality. 
template <class TKey, class T> 
class TMultimapWithKeyValueErase : public THashMultiMap<TKey, T> { 
public: 
    TMultimapWithKeyValueErase() { 
        this->reserve(1000); // reserve to not reallocate at runtime 
    } 
 
    using THashMultiMap<TKey, T>::THashMultiMap; 
 
    bool EraseKeyValue(const TKey& key, const T& value) { 
        auto [i, end] = this->equal_range(key); 
        for (; i != end; ++i) { 
            if (i->second == value) { 
                this->erase(i); 
                return true; 
            } 
        } 
        return false; 
    } 
}; 
 
} // namespace NDebug 
 
// Structure for using in debug purpuses with gdb. 
// Can show all main SQS objects in coredump. 
struct TDebugInfo { 
    TDebugInfo(); 
 
    // Actors 
    class TSqsService* SqsServiceActorPtr = nullptr; 
    class TSqsProxyService* SqsProxyServiceActorPtr = nullptr; 
 
    // Requests by request id 
    NDebug::TMultimapWithKeyValueErase<TString, class TProxyActor*> ProxyActors; 
    NDebug::TMultimapWithKeyValueErase<TString, NActors::IActor*> ActionActors; 
    NDebug::TMultimapWithKeyValueErase<TString, class TMiniKqlExecutionActor*> ExecutorActors; 
 
    // Queue activities by [username/queuename] 
    NDebug::TMultimapWithKeyValueErase<TString, class TQueueLeader*> QueueLeaders;
    NDebug::TMultimapWithKeyValueErase<TString, class TPurgeActor*> QueuePurgeActors; 
    NDebug::TMultimapWithKeyValueErase<TString, class TRetentionActor*> QueueRetentionActors; 
    NDebug::TMultimapWithKeyValueErase<TString, class TCleanupActor*> QueueCleanupActors; 
    NDebug::TMultimapWithKeyValueErase<TString, class TQueueMigrationActor*> QueueMigrationActors; 
 
    // Http 
    class TAsyncHttpServer* HttpServer = nullptr; 
    THashSet<class THttpRequest*> UnparsedHttpRequests; // requests without assigned request id 
    NDebug::TMultimapWithKeyValueErase<TString, class THttpRequest*> ParsedHttpRequests; // http requests with request id 
    void MoveToParsedHttpRequests(const TString& requestId, class THttpRequest* request); 
    void EraseHttpRequest(const TString& requestId, class THttpRequest* request); 
}; 
 
// Helper for safe access to debug info. 
class TDebugInfoHolder { 
public: 
    class TAutoGuarder { 
        friend class TDebugInfoHolder; 
 
        TAutoGuarder(TDebugInfoHolder& parent) 
            : Parent(parent) 
            , Guard(Parent.Lock) 
        { 
        } 
 
    public: 
        TDebugInfo* operator->() { 
            return &Parent.DebugInfo; 
        } 
 
    private: 
        TDebugInfoHolder& Parent; 
        TGuard<TAdaptiveLock> Guard; 
    }; 
 
    // Returns safe (guarded) debug info to write to. 
    TAutoGuarder operator->() { 
        return { *this }; 
    } 
 
private: 
    TDebugInfo DebugInfo; 
    TAdaptiveLock Lock; 
}; 
 
extern TDebugInfoHolder DebugInfo; 
 
} // namespace NKikimr::NSQS 
