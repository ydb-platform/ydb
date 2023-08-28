#include "namespaces_list.h"
#include "interface/spilling.h"
#include "storage/storage.h"


#include <map>


namespace NYql {
namespace NSpilling {


TAtomicSharedPtr<TNamespaceCache> TNamespacesList::GetNamespaceCache(const TString& name){
    TAtomicSharedPtr<TNamespaceCache> res;
    with_lock(NamespacesMapLock_) {
        if ( auto found = NamespacesMap_.find(name); found != NamespacesMap_.end() ) {
            res = found->second;
        } else {
            CurrNamespaceId_++;
            res = MakeAtomicShared<TNamespaceCache>(name, CurrNamespaceId_, StorageI_);
            NamespacesMap_.emplace(name, res);
        } 
    }
    return res;
}


void TNamespacesList::PushNamespaceToProcess(const TString& name, ui32 sessionId) {
    with_lock(QueueLock_) {
        NamespacesQueue_.push_back(name);
        if (ActiveWorkerThreads_ < NamespacesQueue_.size()) {
            WorkingSemaphore_.release();
        }
    }
    with_lock(SessionsLock_) {
        SessionNamespaces_[sessionId].emplace(name);
    }
}
    
bool TNamespacesList::PopNamespaceToProcess(TString& name) {
    bool res = false;
    with_lock(QueueLock_) {
        if ( NamespacesQueue_.size() > 0) {
            name = NamespacesQueue_.front();
            NamespacesQueue_.pop_front();
            res = true;
        } else {
            res = false;
        }
    } 
    return res;
}

bool TNamespacesList::WaitForTask() {
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    auto diff = std::chrono::milliseconds(1000);
    ActiveWorkerThreads_--;
    bool task = WorkingSemaphore_.try_acquire_for(diff);
    ActiveWorkerThreads_++;
    return task;
}

void TNamespacesList::PerformGarbageCollection() {

    with_lock(GarbageCollectionLock_) {
        if ( NamespacesForGarbageCollection_.size() == 0 ) {
            NamespacesForGarbageCollection_ = StorageI_->GetNamespaces();
        }
    }

    bool needGarbageCollection = true;
    
    if (GarbageCollectionThreads_ <= MaxGarbageCollectionThreads ) {
        GarbageCollectionThreads_++;
    } else {
        return;
    }

    while (needGarbageCollection) {
        TString ns;
        with_lock( GarbageCollectionLock_ ) {
            if ( NamespacesForGarbageCollection_.size() > 0 ) {
                ns = NamespacesForGarbageCollection_.back();
                NamespacesForGarbageCollection_.pop_back();
            } else {
                needGarbageCollection = false;
                break;
            }
        }

        TAtomicSharedPtr<TNamespaceCache> nsc;
        nsc = GetNamespaceCache(ns);
        nsc->GarbageCollection();

    }

    GarbageCollectionThreads_--;
        

}

void TNamespacesList::RemoveNamespaceCache(const TString& name){
    with_lock(NamespacesMapLock_) {
        NamespacesMap_.erase(name);
    }
}

bool TNamespacesList::CheckForStop() {
    bool res = false;
    with_lock(NamespacesMapLock_) {
        res = StopFlag_;
    }
    return res;
}

void TNamespacesList::Stop() {
    with_lock(NamespacesMapLock_) {
        StopFlag_ = true;
        for (ui32 i = 0; i < MaxThreadPoolSize; i++ ) {
            WorkingSemaphore_.release();        
        }
    }
}

TSessionDataStat TNamespacesList::GetSessionDataStat(ui32 sessionId) {
    TSessionDataStat res;
    with_lock(SessionsLock_) {
        auto found = SessionNamespaces_.find(sessionId);
        if (found != SessionNamespaces_.end()) {
            for (auto& it: found->second) {
                TSessionDataStat ns_res;
                auto nsc = GetNamespaceCache(it);
                ns_res = nsc->GetSessionStat(sessionId);
                res.Provided += ns_res.Provided;
                res.InMemory += ns_res.InMemory;
                res.Spilled += ns_res.Spilled;
                res.LoadedFromStorage += ns_res.LoadedFromStorage;
                res.LoadedFromMemory += ns_res.LoadedFromMemory;
             }
        }
    } 
    return res;
}

void TNamespacesList::CloseSession(ui32 sessionId) {
       with_lock(SessionsLock_) {
        auto found = SessionNamespaces_.find(sessionId);
        if (found != SessionNamespaces_.end()) {
            for (auto& it: found->second) {
                TSessionDataStat ns_res;
                auto nsc = GetNamespaceCache(it);
                nsc->CloseSession(sessionId);
             }
        }
    } 


}

TNamespacesList::TNamespacesList(ui32 threadPoolSize, TAtomicSharedPtr<ISpillStorage> storageI) : 
    ActiveWorkerThreads_(threadPoolSize),
    StorageI_(storageI) {
        LastGarbageCollection_ = std::chrono::steady_clock::now();
}

}
}
