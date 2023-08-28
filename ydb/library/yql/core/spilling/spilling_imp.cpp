#include "spilling_imp.h"
#include "interface/spilling.h"
#include "storage/storage.h"

#include <ydb/library/yql/utils/log/log.h>

#include <random>
#include <util/folder/path.h>
#include <filesystem>
#include <iostream>
#include <limits>
#include <unordered_map>
#include <chrono>

#include <util/string/split.h>

#include <util/folder/dirut.h>
#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/generic/ptr.h>
#include <util/generic/utility.h>
#include <util/system/file.h>
#include <util/system/file_lock.h>
#include <util/system/fs.h>
#include <util/system/maxlen.h>
#include <util/system/mutex.h>
#include <util/system/utime.h>
#include <util/system/thread.h>



namespace NYql {
namespace NSpilling {

THolder<ISession>  TTempStorageProxyImp::CreateSession() {
    if (CurrSessId_ == std::numeric_limits<ui32>::max() ) {
        CurrSessId_ = 1;
    }
    ui32 sessId = CurrSessId_.fetch_add(1);
    return MakeHolder<TSessionImp>(sessId, NsList_, StorageI_);

}

THolder<IObjectsIterator> TTempStorageProxyImp::CreateIterator( 
                                                                const TMaybe<TString>& objNamespace,
                                                                const TMaybe<TString>& objName, 
                                                                bool onlyValid) {

    return MakeHolder<TStorageIteratorImp>(objNamespace, objName, onlyValid);
}

TTempStorageExecutionPolicy TTempStorageProxyImp::ExecutionPolicy() {
    return Policy_;
}

TOperationResults TTempStorageProxyImp::SetExecutionPolicy(const TTempStorageExecutionPolicy & policy) {
    Policy_ = policy;
}

NThreading::TFuture<TOperationResults> TTempStorageProxyImp::Delete(const TString & objNamespace, const TMaybe<TString> & name) {
    NThreading::TFuture<TOperationResults> res;
    return res;
}


TOperationResults TTempStorageProxyImp::LastOperationResults(){
    return OperationResults_;
}

ui32 Ui32Rand(const ui32 & min, const ui32 & max) {
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<ui32> distribution(min,max);
    return distribution(generator);
}

// Worker function in threads pool
void ProcessThreadPoolTasks(TAtomicSharedPtr<TNamespacesList> nsl, TAtomicSharedPtr<ISpillStorage> sti){
    bool stopFlag = false;
    bool haveNamespaceToProcess = false;
    TString namespaceToProcess;
    ui32 periodShift = Ui32Rand(0, GarbageCollectionPeriodMs / 8); 

    std::chrono::steady_clock::time_point lastGarbageCollection = std::chrono::steady_clock::now();

    TThread::TId this_id = TThread::CurrentThreadNumericId();
    
    while( !stopFlag ) {
        try {
            bool task = nsl->WaitForTask();

            std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
            ui64 period = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastGarbageCollection).count();
            if ( period > GarbageCollectionPeriodMs  + periodShift ) {

                if ( this_id % DefaultThreadPoolSize <= MaxGarbageCollectionThreads)  {
                    lastGarbageCollection = now;
                    nsl->PerformGarbageCollection();
                    now = std::chrono::steady_clock::now();
                    ui64 execTime = std::chrono::duration_cast<std::chrono::microseconds>(now - lastGarbageCollection).count();
                    YQL_LOG(INFO) << "Thread " << this_id << " garbage collection (microseconds): " << execTime << Endl;
                }
            }
       
            haveNamespaceToProcess = nsl->PopNamespaceToProcess(namespaceToProcess);
            if (haveNamespaceToProcess) {
                TAtomicSharedPtr<TNamespaceCache> nsc = nsl->GetNamespaceCache(namespaceToProcess);
                nsc->Dispatch();
            } else {
            }
            stopFlag = nsl->CheckForStop();
        } catch(...) {

            YQL_LOG(ERROR) << "Error happened..." << FormatCurrentException() << Endl;
        }
    }
    YQL_LOG(INFO) << "Stopping spilling thread " << this_id << " ..." << Endl;
}


TTempStorageProxyImp::TTempStorageProxyImp(const TFileStorageConfig & config, const TTempStorageExecutionPolicy & policy, THolder<ISpillStorage>&& storage) :
    StorageConfig_(config),
    Policy_(policy),
    StorageI_(std::move(storage))
{
    OperationResults_.Status = EOperationStatus::Success;
    NsList_ = MakeAtomicShared<TNamespacesList>(DefaultThreadPoolSize, StorageI_);
    for (ui32 i = 0; i < DefaultThreadPoolSize; i++) {
        ThreadPool_.push_back(std::thread(ProcessThreadPoolTasks, NsList_, StorageI_));
    }
}


TTempStorageProxyImp::~TTempStorageProxyImp(){
    YQL_LOG(INFO) << "Stopping nslist ..." << Endl;
    NsList_->Stop();
    YQL_LOG(INFO) << "Stopping threads ..." << Endl;
    for(auto & t: ThreadPool_) {
        t.join();
    }
}



NThreading::TFuture<TOperationResults> TSessionImp::Save(const TString & objNamespace, const TString & name,  TBuffer && buf){
    TAtomicSharedPtr<TNamespaceCache> nsc = NsList_->GetNamespaceCache(objNamespace);
    NsList_->PushNamespaceToProcess(objNamespace, SessionId_);
    return nsc->Save(name, std::move(buf), SessionId_);
}

NThreading::TFuture<TLoadOperationResults> TSessionImp::Load(const TString & objNamespace, const TString & name, EObjectsLifetime objLifetime ){
    TAtomicSharedPtr<TNamespaceCache> nsc = NsList_->GetNamespaceCache(objNamespace);
    NsList_->PushNamespaceToProcess(objNamespace, SessionId_);
    return nsc->Load(name, SessionId_, objLifetime);
}


TSessionDataStat TSessionImp::GetSessionDataStat(){
    return NsList_->GetSessionDataStat(SessionId_);
}

TSessionExecutionPolicy TSessionImp::ExecutionPolicy(){ 
    return Policy_;
} 


TOperationResults TSessionImp::SetExecutionPolicy(const TSessionExecutionPolicy& policy){
    TOperationResults res;
    Policy_ = policy;
    return res;
}

std::pair<THolder<IStream>, TOperationResults> TSessionImp::OpenStream(const TString& objNamespace, const TString& streamName ) {
    THolder<TStreamImp> sti = MakeHolder<TStreamImp>(objNamespace, streamName, NsList_, SessionId_);
    TOperationResults res;
    return std::make_pair<THolder<IStream>, TOperationResults> (std::move(sti), std::move(res)); 
}


TSessionImp::TSessionImp(ui32 sessionId, TAtomicSharedPtr<TNamespacesList> nsList, TAtomicSharedPtr<ISpillStorage> storage) : 
    SessionId_(sessionId),
    NsList_(nsList),
    StorageI_(storage) {

}

TStreamImp::TStreamImp(const TString& objNamespace, const TString& streamName, TAtomicSharedPtr<TNamespacesList> nsList, ui32 sessionId ) : 
    Namespace_(objNamespace), 
    Name_(streamName),
    NsList_(nsList),
    SessionId_(sessionId) {};

TSessionImp::~TSessionImp(){
    NsList_->CloseSession(SessionId_);
}



bool TStorageIteratorImp::Next(TTempObjectDesc & object) {
    return false;
}

TStorageIteratorImp::TStorageIteratorImp(const TMaybe<TString>& objNamespace, const TMaybe<TString>& objName, bool onlyValid) {
}


NThreading::TFuture<TOperationResults> TStreamImp::Save(TBuffer&& buf){
    TAtomicSharedPtr<TNamespaceCache> nsc = NsList_->GetNamespaceCache(Namespace_);
    NsList_->PushNamespaceToProcess(Namespace_, SessionId_);
    return nsc->Save(Name_, std::move(buf), SessionId_, true);
}

ui64 TStreamImp::Size() {
    TAtomicSharedPtr<TNamespaceCache> nsc = NsList_->GetNamespaceCache(Namespace_);
    return nsc->StreamSize(Name_);

}

NThreading::TFuture<TLoadOperationResults> TStreamImp::Load(ui64 bufferId, EObjectsLifetime objLifetime ) {
    TAtomicSharedPtr<TNamespaceCache> nsc = NsList_->GetNamespaceCache(Namespace_);
    NsList_->PushNamespaceToProcess(Namespace_, SessionId_);
    return nsc->Load(Name_, SessionId_, objLifetime, true, bufferId);


}

NThreading::TFuture<TOperationResults> TStreamImp::Close() {
    NThreading::TFuture<TOperationResults> res;
    return res;
}




std::pair< THolder<ITempStorageProxy>, TOperationResults >  CreateFileStorageProxy(const TFileStorageConfig & config, const TTempStorageExecutionPolicy & policy ) {

    std::pair< THolder<ISpillStorage>, TOperationResults > sps = OpenFileStorageForSpilling(config);
    THolder<TTempStorageProxyImp> sp = MakeHolder<TTempStorageProxyImp>(config, policy, std::move(sps.first));
    TOperationResults res = sps.second;
    return std::make_pair< THolder<ITempStorageProxy>, TOperationResults >( std::move(sp), std::move(res) );    
}


}
}