#include "interface/spilling.h"
#include "storage/storage.h"
#include "namespaces_list.h"

#include <thread>
#include <set>

namespace NYql {
namespace NSpilling {

const ui64 MagicForFileRecord =     0xA957248FEED9E4CE;





class TTempStorageProxyImp : public ITempStorageProxy {
public:
    THolder<ISession> CreateSession();  
    THolder<IObjectsIterator> CreateIterator(   const TMaybe<TString>& objNamespace = TMaybe<TString>(),
                                                const TMaybe<TString>& objName = TMaybe<TString>(), 
                                                bool onlyValid = true);    
    TTempStorageExecutionPolicy ExecutionPolicy();
    TOperationResults SetExecutionPolicy(const TTempStorageExecutionPolicy& policy); 
    NThreading::TFuture<TOperationResults> Delete(const TString& objNamespace, const TMaybe<TString>& name);
    TOperationResults LastOperationResults();
    TTempStorageProxyImp(const TFileStorageConfig & config, const TTempStorageExecutionPolicy & policy, THolder<ISpillStorage>&& storage);
    ~TTempStorageProxyImp();

private:
    TFileStorageConfig StorageConfig_;
    TTempStorageExecutionPolicy Policy_;
    TAtomicSharedPtr<ISpillStorage> StorageI_;
    TOperationResults OperationResults_; // Last operation results
    std::atomic<ui32> CurrSessId_ = 1; // Current session id to assign new session
    std::vector<std::thread> ThreadPool_; // Thread pool to process spilling tasks
    TAtomicSharedPtr<TNamespacesList> NsList_; // List of namespaces to forward requests 

};

// Worker function in threads pool
void ProcessThreadPoolTasks(TAtomicSharedPtr<TNamespacesList> nsl, TAtomicSharedPtr<ISpillStorage> sti);


// Class to store session object ids
class TSessionIds {
    ui32 NsId_ = 0;
    ui32 ObjId_ = 0; 
};

class TSessionImp: public ISession {
public: 
    NThreading::TFuture<TOperationResults> Save(const TString & objNamespace, const TString & name,  TBuffer && buf);
    NThreading::TFuture<TLoadOperationResults> Load(const TString & objNamespace, const TString & name, EObjectsLifetime objLifetime = EObjectsLifetime::DeleteAfterLoad );
    TSessionDataStat GetSessionDataStat();
    TSessionExecutionPolicy ExecutionPolicy(); 
    TOperationResults SetExecutionPolicy(const TSessionExecutionPolicy& policy);
    std::pair<THolder<IStream>, TOperationResults> OpenStream(const TString& objNamespace, const TString& streamName );
    TSessionImp(ui32 sessionId, TAtomicSharedPtr<TNamespacesList> nsList, TAtomicSharedPtr<ISpillStorage> storage);
    ~TSessionImp();

private:
    ui32 SessionId_;
    TSessionExecutionPolicy Policy_;
    TAtomicSharedPtr<TNamespacesList> NsList_; // List of namespaces to forward requests 
    TAtomicSharedPtr<ISpillStorage> StorageI_; // Storage interface to spill session data
};

class TStorageIteratorImp: public IObjectsIterator {
public:
    bool Next(TTempObjectDesc & object);
    TStorageIteratorImp(const TMaybe<TString>& objNamespace = TMaybe<TString>(),
                                                const TMaybe<TString>& objName = TMaybe<TString>(), 
                                                bool onlyValid = true);
private:
    bool NameSpacesIterator_ = false;
    TVector<TString> ObjNamespaces_;
    TVector<TString>::const_iterator CurrNamespace_;
};


class TStreamImp: public IStream {
public:
    NThreading::TFuture<TOperationResults> Save(TBuffer&& buf);
    ui64 Size();
    NThreading::TFuture<TLoadOperationResults> Load(ui64 bufferId = 0, EObjectsLifetime objLifetime = EObjectsLifetime::DeleteAfterLoad);
    NThreading::TFuture<TOperationResults> Close();
    TStreamImp(const TString& objNamespace, const TString& streamName, TAtomicSharedPtr<TNamespacesList> nsList, ui32 sessionId );
private:
    TString Namespace_;
    TString Name_;
    TAtomicSharedPtr<TNamespacesList> NsList_;
    ui32 SessionId_;    
};


}
}