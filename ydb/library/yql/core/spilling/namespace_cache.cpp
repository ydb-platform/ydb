#include "namespaces_list.h"
#include "interface/spilling.h"
#include "storage/storage.h"

#include <ydb/library/yql/utils/log/log.h>

#include <map>

#include <util/string/split.h>
#include <contrib/libs/xxhash/xxhash.h>


namespace NYql {
namespace NSpilling {

// Finds approximate position of val in array of N increasing values. Values cycle after max(ui32)
// Returns true if val can be between start and end taking into account cycling.
inline bool FindPos(ui32 start, ui32 end, ui32 val, ui32 nElements, ui32 & pos) {
    if (!nElements)
        return false;
    
    if (start == end) {
        if ( val == start ) {
            pos = 0;
            return true;
        } else {
            return false;
        }
    }

    if (start > end) {
        ui32 len = (std::numeric_limits<ui32>::max() - start) + end;
        if ((val >= start) || (val <= end)) {
            pos = len / nElements;
            return true;
        } else {
            return false;
        }
    }

    if ((val >= start) && (val <= end)) {
        pos = ( (val - start) * (nElements - 1 ) ) / (end - start);
        return true;
    } else {
        return false;
    }

    return false;

};

// Returns file index for particular object id
inline ui32 FindFileInd(ui32 objId) {

    ui32 fileId = (objId) / (1<<16);
    return fileId;

}

inline void TNamespaceCache::UpdateMinSessionId(ui32 sessionId, ui32 objId) {
    auto sessionIt = Sessions_.find(sessionId);
    if ( sessionIt == Sessions_.end() ) {
            Sessions_[sessionId].MinObjId = objId;
    } else {
        if (sessionIt->second.MinObjId > objId) {
            sessionIt->second.MinObjId = objId;
        }
    }
}


NThreading::TFuture<TOperationResults> TNamespaceCache::Save(const TString & objName,  TBuffer && buf, ui32 sessionId, bool isStream ) {
    NThreading::TFuture<TOperationResults> res;
    TSaveTask bt;
    bt.Name = objName;
    bt.SessionId = sessionId;
    bt.Buf = MakeAtomicShared<TBuffer>(std::move(buf));
   
    bt.Promise = NThreading::NewPromise<TOperationResults>();



    with_lock(ToSaveLock_) {
        AdvanceObjId(bt.ObjId);
        if ( !isStream ) {
            ui32 lastObjId = 0;
            auto search = LastObjs_.find(objName);
            if (search != LastObjs_.end() ) {
                lastObjId = search->second;
            }
            if ( lastObjId ) {
                ui32 bytesDeleted = DeleteTaskFromSaveQueue(lastObjId);
                SessionDataSpilled_[sessionId] += bytesDeleted;
            }
            LastObjs_[objName] = bt.ObjId;
        } else {
            auto search = LastStreamIds_.find(objName);
            ui32 streamInd = 0;
            if (search != LastStreamIds_.end() ) {
                streamInd = search->second.size();
                search->second.push_back(bt.ObjId);
            } else {
                streamInd = 0;
                LastStreamIds_[objName].push_back(bt.ObjId);
            }
            bt.StreamBufId = streamInd;
            bt.OpType = EOperationType::StreamBufAdd;
        }
        UpdateMinSessionId(sessionId, bt.ObjId);
        SessionDataProvided_[sessionId] += bt.Buf->Size();
        res = bt.Promise.GetFuture();
        ToSave_.emplace_back(std::move(bt));
    } 
    return res;
}


NThreading::TFuture<TLoadOperationResults> TNamespaceCache::Load(const TString& name, ui32 sessionId, EObjectsLifetime objLifetime, bool isStream, ui32 streamId ) {
    NThreading::TFuture<TLoadOperationResults> res;
    TLoadTask lt;
    TLoadOperationResults lr;
    lt.Name = name;
    lt.Promise = NThreading::NewPromise<TLoadOperationResults>(); 
    lt.Name = name;
    lt.SessionId = sessionId;
    lt.StreamBufId = streamId;
    bool foundInLastObjs = false;
    bool foundInSaveQueue = false;
    with_lock(ToSaveLock_) {
        if (!isStream) {
            auto search = LastObjs_.find(name);
            foundInLastObjs = (search != LastObjs_.end());
            lt.ObjId = search->second;
        } else {
            auto search = LastStreamIds_.find(name);
            if (search != LastStreamIds_.end()) {
                if ( search->second.size() >= streamId ) {
                    foundInLastObjs = true;
                    lt.ObjId = search->second[streamId];
                }
            }
        }
        if ( foundInLastObjs ) {
            UpdateMinSessionId(sessionId, lt.ObjId);
            ui32 objPos = 0;
            foundInSaveQueue = FindObjInSaveQueue(lt.ObjId, objPos);
            if (foundInSaveQueue) {
                TSaveTask & st = ToSave_[objPos];
                if ( st.ProcessingStatus != EProcessingStatus::Deleted ) {
                    lr.Status = EOperationStatus::Success;
                    if ( st.ProcessingStatus == EProcessingStatus::Processing ) {
                       lr.Buf = st.Buf; 
                    } else {
                        if (objLifetime == EObjectsLifetime::DeleteAfterLoad ) {
                            lr.Buf = st.Buf;
                            st.Buf = nullptr;
                            SessionDataSpilled_[st.SessionId] += lr.Buf->Size(); 
                            st.ProcessingStatus = EProcessingStatus::Deleted;
                        } else {
                            lr.Buf = st.Buf; 
                        }
                        
                    }
                    SessionDataLoadedFromMemory_[st.SessionId] += lr.Buf->Size();
                    lt.Promise.SetValue(std::move(lr));
                } else {
                    lr.Status = EOperationStatus::NoObjectName;
                    lt.Promise.SetValue(std::move(lr));                     
                }
            }
        } else {
           lr.Status = EOperationStatus::NoObjectName;
           lt.Promise.SetValue(std::move(lr)); 
        }
    }

    res = lt.Promise.GetFuture();
    if (!foundInSaveQueue && foundInLastObjs) {
        with_lock(ToLoadLock_) {
            ToLoad_.emplace_back(std::move(lt));
        }
    }

    return res;

}


void TNamespaceCache::Dispatch() {

    ProcessSaveQueue();
    ProcessLoadQueue();
    ProcessDeleteQueue();

}


TSessionDataStat TNamespaceCache::GetSessionStat(ui32 sessionId) {
    TSessionDataStat res;
    ui64 provided = 0;
    ui64 spilled =  0;
    ui64 loadedFromMemory = 0;
    ui64 loadedFromStorage = 0;

    with_lock(ToSaveLock_) {
        auto it = SessionDataProvided_.find(sessionId);
        if ( it != SessionDataProvided_.end()) {
            provided = it->second;
            res.Provided = provided / (1024 * 1024);
        };
        it = SessionDataSpilled_.find(sessionId);
        if (it != SessionDataProvided_.end()) {
            spilled = it->second;
            res.Spilled = spilled / (1024 * 1024);
        };
    }

    with_lock(ToLoadLock_) {
        auto it = SessionDataLoadedFromMemory_.find(sessionId);
        if (it != SessionDataLoadedFromMemory_.end()) {
            loadedFromMemory = it->second;
            res.LoadedFromMemory = loadedFromMemory / (1024 * 1024);
        };

        it = SessionDataLoadedFromStorage_.find(sessionId);
        if (it != SessionDataLoadedFromStorage_.end()) {
            loadedFromStorage = it->second;
            res.LoadedFromStorage = loadedFromStorage / (1024 * 1024);
        };


    }

    ui64 totalProcessed =  spilled;
    if ( totalProcessed > provided ) {
        res.InMemory = 0;
    } else {
        res.InMemory = (provided - totalProcessed) / (1024 * 1024);
    }

    return res;
}


void TNamespaceCache::CloseSession(ui32 sessionId){
    TBaseTask dt;
    dt.OpType = EOperationType::SessionDelete;
    dt.SessionId = sessionId;

    with_lock(ToDeleteLock_) {
        ToDelete_.emplace_back(dt);
    }

}

void TNamespaceCache::GarbageCollection(){
    std::set<ui32> filesIdToDelete;

    ui32 minObjIdForSession = 0;
    with_lock(ToSaveLock_) {
        if (Sessions_.size() > 0) {
            minObjIdForSession = Sessions_.begin()->second.MinObjId;
        }
        for (auto & s: Sessions_ ) {
            if (s.second.MinObjId < minObjIdForSession ) {
                minObjIdForSession = s.second.MinObjId;
            }
        }

        for ( auto it = LastObjs_.begin(); it != LastObjs_.end();  ) {
            if (it->second < minObjIdForSession ) {
                it = LastObjs_.erase(it);
            } else {
                it++;
            }
        }

        for ( auto it = LastStreamIds_.begin(); it != LastStreamIds_.end(); ) {
            if ( it->second.size() > 0) {
                if (it->second.front() < minObjIdForSession && it->second.back() < minObjIdForSession) {
                    it = LastStreamIds_.erase(it);
                    continue;
                }
            }
            it++;
        }


    }


    ui32 minFileId = FindFileInd(minObjIdForSession);
    with_lock(FilesLock_) {
        for (auto it = SpillMetaFiles_.begin(); it != SpillMetaFiles_.lower_bound(minFileId); it++ ) {
            SpillFilesIdToDelete_.insert(it->first);
        }
    } 
    


    with_lock(FilesLock_) {
        filesIdToDelete = SpillFilesIdToDelete_;

    }

    for (auto id: filesIdToDelete) {
        TAtomicSharedPtr<TMetaFileAttributes> met;
        with_lock(FilesLock_) {
            InitMetaFile(id);
            met = SpillMetaFiles_[id];
        }
        if ( met->DataFile->IsLocked()) {
            met->DataFile->Delete();
        }

        if ( met->MetaFile->IsLocked()) {
            met->MetaFile->Delete();
        }
    }

    with_lock(FilesLock_) {
        for (auto id: filesIdToDelete) {
            SpillFilesIdToDelete_.erase(id);
            SpillMetaFiles_.erase(id);

        }

    }

}



ui64 TNamespaceCache::StreamSize(const TString& name){
    with_lock(ToSaveLock_) {
        auto search = LastStreamIds_.find(name);
        if (search != LastStreamIds_.end()) {
            return search->second.size();
        } else {
            return 0;
        }
    }

} 


void TNamespaceCache::NextNamespaceFile(bool openExisting) {
    with_lock(FilesLock_) {
        TVector<TString> files = StorageI_->GetNamespaceFiles(Name_);
        ui32 maxInd = 0;
        for (const auto & p: files) {
            TVector<TString> splitted;
            size_t tokenCount = Split(p, ".", splitted);
            if (tokenCount == 4 && splitted[0] == "ydbspl" ) {
                ui32 ind = 0;
                if (TryFromString(splitted[1], ind)) {
                    if (SpillMetaFiles_.find(ind) == SpillMetaFiles_.end() ) {
                        SpillFilesIdToDelete_.insert(ind);
                    }
                    if (ind > maxInd) {
                        maxInd = ind;
                    }
                }
            }
        }


        if (maxInd > std::numeric_limits<ui32>::max() / 2 ) {
            maxInd = 1;
        }

        SpillFilesIdToDelete_.erase(maxInd);

        TTempStorageExecutionPolicy pol;
        ui32 retryCount = 0;
        for (; retryCount < pol.MaxNumberOfRetries; retryCount++ ) {
            THolder<ISpillFile> metFile = StorageI_->CreateSpillFile(Name_, TString("ydbspl.") + std::to_string(maxInd) + TString(".0.met"), MetaFileIncreaseStep);
            if (metFile->IsLocked()) {
                CurrSpillFileId_ = maxInd;
                CurrSpillMetaFile_ = std::move(metFile);
                THolder<ISpillFile> datFile = StorageI_->CreateSpillFile(Name_, TString("ydbspl.") + std::to_string(maxInd) + TString(".0.dat"), DataFileIncreaseStep);
                CurrSpillDataFile_ = std::move(datFile);
                NextObjId_ = CurrSpillFileId_ * (1<<16) + 1;
                TAtomicSharedPtr<TMetaFileAttributes> res = MakeAtomicShared<TMetaFileAttributes>();
                res->MetaFile = CurrSpillMetaFile_;
                res->DataFile = CurrSpillDataFile_;
                res->Id = CurrSpillFileId_;
                res->FirstObjId = NextObjId_;
                res->MetaRecords = MakeAtomicShared<std::map<ui32, TSpillMetaRecord>>();
                SpillMetaFiles_[CurrSpillFileId_] = res;
                break;
            } else {
                maxInd++;
            }
        }

    }


}

inline void TNamespaceCache::AdvanceObjId(ui32 & objId) {
    if (NextObjId_ == std::numeric_limits<ui32>::max() ) {
        NextObjId_ = 1;
    }
    objId = ++NextObjId_;
}


inline bool TNamespaceCache::FindObjInSaveQueue(ui32 objId, ui32& pos){
    ui32 size = ToSave_.size();
    if ( size == 0)
        return false;
    ui32 firstId = ToSave_.front().ObjId;
    ui32 lastId = ToSave_.back().ObjId;
    ui32 approximatePos = 0;
    bool found = FindPos(firstId, lastId, objId, size, approximatePos );
    if (!found)
        return false;
        
    pos = approximatePos;
    auto it = ToSave_.begin() + pos;
    ui32 toSaveId = it->ObjId;

    if (toSaveId == objId) {
        return true;
    }

    if (toSaveId < objId) {
        for( ; it != ToSave_.end(); it++) {
            toSaveId = it->ObjId;
            if (toSaveId == objId) {
                pos = (it - ToSave_.begin());
                return true;
            }
        }
        return false;
    }

    if (toSaveId > objId) {
        while(pos > 0) {
            --pos;
            if (ToSave_[pos].ObjId == objId) {
                return true;
            }
        }
        return false;
    }

    return false;
}


ui32 TNamespaceCache::DeleteTaskFromSaveQueue(ui32 objId, EProcessingStatus reason) {
    ui32 pos;
    ui32 bytesDeleted = 0;
    bool found = FindObjInSaveQueue(objId, pos);
    if ( found && ToSave_[pos].ProcessingStatus != EProcessingStatus::Processing) {
        ToSave_[pos].ProcessingStatus = reason;
        bytesDeleted = ToSave_[pos].Buf->Size();
        ToSave_[pos].Buf = nullptr;
    }
    return bytesDeleted;

}

bool TNamespaceCache::FindNextTaskInSaveQueue(ui32& taskPos) {

    bool res = false;
    for ( taskPos = 0; taskPos < ToSave_.size(); taskPos++) {
        TSaveTask& saveTask = ToSave_[taskPos];
        if ( saveTask.ProcessingStatus == EProcessingStatus::Added ) {
            if (saveTask.OpType == EOperationType::Add ) {
                TString& name = saveTask.Name;
                auto search = LastObjs_.find(name);
                ui32 foundObjId = 0;
                if ( search != LastObjs_.end() ) {
                 foundObjId = search->second;
                }

                if (foundObjId <= saveTask.ObjId ) {
                    saveTask.ProcessingStatus = EProcessingStatus::Processing;
                    res = true;
                    break;
                } else {
                    saveTask.ProcessingStatus = EProcessingStatus::Deleted;
                }
            }

            if (saveTask.OpType == EOperationType::StreamBufAdd ) {
                saveTask.ProcessingStatus = EProcessingStatus::Processing;
                res = true;
                break;

            }
    
        }

    }
    return res;
}

bool TNamespaceCache::FindNextTaskInLoadQueue(ui32& taskPos) {

    bool res = false;
    for ( taskPos = 0; taskPos < ToLoad_.size(); taskPos++) {
        TLoadTask& loadTask = ToLoad_[taskPos];
        if ( loadTask.ProcessingStatus == EProcessingStatus::Added ) {
            if (loadTask.OpType == EOperationType::Add ) {
                TString& name = loadTask.Name;
                auto search = LastObjs_.find(name);
                ui32 foundObjId = 0;
                if ( search != LastObjs_.end() ) {
                    foundObjId = search->second;
                }

                if (foundObjId > loadTask.ObjId ) {
                    loadTask.ObjId = foundObjId;
                }
                loadTask.ProcessingStatus = EProcessingStatus::Processing;
                res = true;
                break;
            }
        }
    }
    return res;
}

bool TNamespaceCache::FindNextTaskInDeleteQueue(ui32& taskPos) {

    bool res = false;
    for ( taskPos = 0; taskPos < ToDelete_.size(); taskPos++) {
        TBaseTask& deleteTask = ToDelete_[taskPos];
        if ( deleteTask.ProcessingStatus == EProcessingStatus::Added ) {
            deleteTask.ProcessingStatus = EProcessingStatus::Processing;
            res = true;
            break;
        } 
    }
    return res;
}

void TNamespaceCache::ChangeLastObjId(TString& objName, ui32 prevObjId, ui32 nextObjId) {
    auto search = LastObjs_.find(objName);
    if ( search != LastObjs_.end() ) {
        if (search->second <= prevObjId) {
            search->second = nextObjId;
        }
    }

}

void TNamespaceCache::ProcessSaveQueue() {

    ui32 taskPos = 0;
    bool saveTaskFound = false;
    bool enoughSpaceToWrite = false;

    try {
        ToSaveLock_.lock();

        RemoveCompletedFromSaveQueue();
        
        saveTaskFound = FindNextTaskInSaveQueue(taskPos);

        if ( saveTaskFound ) {

            TAtomicSharedPtr<ISpillFile> currSpillMetaFile = CurrSpillMetaFile_;
            TAtomicSharedPtr<ISpillFile> currSpillDataFile = CurrSpillDataFile_; 

            TSaveTask& saveTask = ToSave_[taskPos];
            ui32 size = saveTask.Buf->Size();
            ui64 offset = currSpillDataFile->Reserve(size);
            ui64 total = offset + size;
            TAtomicSharedPtr<TBuffer> bufToSave = saveTask.Buf; 
            char * data = bufToSave->Data();
            ui32 taskObjId = saveTask.ObjId;
            ui32 prevObjId = taskObjId;
            TSpillMetaRecord mr{EOperationType::Add, saveTask.Name, offset, taskObjId, size, 0 };
            ui32 metaSize = mr.Size();
            ui64 metaOffset = currSpillMetaFile->Reserve(metaSize);

            if (total >= std::numeric_limits<ui32>::max() ) {
                saveTask.ProcessingStatus = EProcessingStatus::Added;
                NextNamespaceFile(false);
                ToSaveLock_.unlock();
            } else {
                enoughSpaceToWrite = true;
                saveTask.ProcessingStatus = EProcessingStatus::Processing;
                bool changeObjId = ( !(taskObjId > CurrSpillFileId_ * (1<<16) && (taskObjId < (CurrSpillFileId_ + 1) * (1<<16)) ));
                if (changeObjId) {
                    AdvanceObjId(taskObjId);
                    mr.SetObjId(taskObjId);
                }
                ToSaveLock_.unlock();
                XXH32_hash_t hash = XXH32( data, size, 0);
                mr.SetDataHash(hash);


                TBuffer mrBuf;
                mr.Pack(mrBuf);

                currSpillDataFile->Write(offset, data, size);
                currSpillMetaFile->Write(metaOffset, mrBuf.Data(), metaSize);


                ToSaveLock_.lock();
                saveTask.ProcessingStatus = EProcessingStatus::Deleted;
                if (changeObjId) {
                    ChangeLastObjId(saveTask.Name, prevObjId, taskObjId);
                }
                SessionDataSpilled_[saveTask.SessionId] += size;
                ToSaveLock_.unlock();

                with_lock(FilesLock_) {
                    auto & metaAttributes = SpillMetaFiles_[CurrSpillFileId_];
                    metaAttributes->MetaRecords->insert({taskObjId, mr});
                }
            }

        } else {
            ToSaveLock_.unlock();
        }


    } catch(...) {
        ToSaveLock_.unlock();
        throw;
    }

}

void TNamespaceCache::ProcessLoadQueue() {

    ui32 taskPos = 0;
    bool loadTaskFound = false;

    TLoadTask loadTask;
    with_lock(ToLoadLock_) {
        RemoveCompletedFromLoadQueue();
        loadTaskFound = FindNextTaskInLoadQueue(taskPos);
        if (!loadTaskFound)
            return;
        loadTask = ToLoad_[taskPos];
    }



    TLoadOperationResults lr;
    ui32 fileInd = FindFileInd(loadTask.ObjId);
    TSpillMetaRecord mr;
    TAtomicSharedPtr<TMetaFileAttributes> fileMet;
    bool found = false;

    with_lock(FilesLock_) {
        found = FindMetaRecordInFile(fileInd, loadTask.ObjId, mr, fileMet );
    }


    if (!found)
        return;

    TBuffer buf(mr.DataSize());
    fileMet->DataFile->Read(mr.Offset(), buf.Data(), mr.DataSize());
    XXH32_hash_t hash = XXH32( buf.Data(), mr.DataSize(), 0);
    lr.Buf = MakeAtomicShared<TBuffer>(std::move(buf));
    lr.Status = EOperationStatus::Success;

    if ( hash != mr.DataHash() ) {
        lr.Status = EOperationStatus::ChecksumInvalid;
        YQL_LOG(ERROR) << "Wrong hash!!!:  " << "Buf hash: " << hash << " Meta hash: " << mr.DataHash() << Endl;
    }

    with_lock(ToLoadLock_) {
        SessionDataLoadedFromStorage_[loadTask.SessionId] += mr.DataSize();
    }


    loadTask.Promise.SetValue(std::move(lr)); 


}

void TNamespaceCache::ProcessDeleteQueue() {

    ui32 taskPos = 0;
    bool deleteTaskFound = false;

    with_lock(ToDeleteLock_) {
        RemoveCompletedFromDeleteQueue();
        deleteTaskFound = FindNextTaskInDeleteQueue(taskPos);
    }

    if (!deleteTaskFound) 
        return;

    TBaseTask& deleteTask = ToDelete_[taskPos];
    ui32 sessionId = deleteTask.SessionId; 

    if (deleteTask.OpType == EOperationType::SessionDelete ) {
        with_lock(ToSaveLock_) {
            Sessions_.erase(sessionId);
            for (auto& t : ToSave_ ) {
                if ( t.SessionId == sessionId ) {
                    t.ProcessingStatus = EProcessingStatus::Deleted;
                    t.Buf = nullptr;
                }
            }
        }

        with_lock(ToLoadLock_) {
            for (auto& t : ToLoad_ ) {
                if ( t.SessionId == sessionId ) {
                    t.ProcessingStatus = EProcessingStatus::Deleted;
                    t.Buf = nullptr;
                }
            }
        }
    }



}


void TNamespaceCache::RemoveCompletedFromSaveQueue() {

    for (auto it = ToSave_.begin(); it != ToSave_.end(); ) {
        if (it->ProcessingStatus == EProcessingStatus::Deleted) {
            ui64 sessionId = it->SessionId;
            it = ToSave_.erase(it);
        } else {
            break;
        }
    }

}

void TNamespaceCache::RemoveCompletedFromLoadQueue() {

    for (auto it = ToLoad_.begin(); it != ToLoad_.end(); ) {
        if (it->ProcessingStatus == EProcessingStatus::Deleted) {
            ui64 sessionId = it->SessionId;
            it = ToLoad_.erase(it);
        } else {
            break;
        }
    }

}


void TNamespaceCache::RemoveCompletedFromDeleteQueue() {

    for (auto it = ToDelete_.begin(); it != ToDelete_.end(); ) {
        if (it->ProcessingStatus == EProcessingStatus::Deleted) {
            ui64 sessionId = it->SessionId;
            it = ToDelete_.erase(it);
        } else {
            break;
        }
    }

}


bool TNamespaceCache::FindMetaRecordInFile(ui32 fileId, ui32 objId, TSpillMetaRecord& mr,  TAtomicSharedPtr<TMetaFileAttributes>& fileMet) {
    bool res = false;
    TAtomicSharedPtr<TMetaFileAttributes> met = InitMetaFile(fileId);
    auto it = met->MetaRecords->find(objId);
    if (it != met->MetaRecords->end() ) {
        res = true;
        mr = it->second;
        fileMet = met;
    }   

    return res;
}

TAtomicSharedPtr< std::vector<TSpillMetaRecord> > ReadAllRecordsFromMetaFile( TAtomicSharedPtr<ISpillFile> file) {
    TAtomicSharedPtr< std::vector<TSpillMetaRecord> > res = MakeAtomicShared<std::vector<TSpillMetaRecord>>();
    file->Seek(0);
    TBuffer readbuf(ReadBufSize);
    i32 readRes = file->Read(0, readbuf.Data(), ReadBufSize);
    TSpillMetaRecord mr;
    mr.Unpack(readbuf);
    return res; 
}


 TAtomicSharedPtr<TMetaFileAttributes> TNamespaceCache::InitMetaFile(ui32 fileId){
     TAtomicSharedPtr<TMetaFileAttributes> res;

        auto found = SpillMetaFiles_.find(fileId);
        if (found != SpillMetaFiles_.end()) {
            res = found->second;
        } else {
            THolder<ISpillFile> metFile = StorageI_->CreateSpillFile(Name_, TString("ydbspl.") + std::to_string(fileId) + TString(".0.met"), MetaFileIncreaseStep);
            THolder<ISpillFile> datFile = StorageI_->CreateSpillFile(Name_, TString("ydbspl.") + std::to_string(fileId) + TString(".0.dat"), DataFileIncreaseStep);
            res = MakeAtomicShared<TMetaFileAttributes>();
            res->MetaFile = std::move( metFile );
            res->DataFile = std::move(datFile);
            SpillMetaFiles_[fileId] = res;
        }



    return res;
}


TAtomicSharedPtr<ISpillFile> FindSpillFile(ui32 objId, EFileType fileType = EFileType::Meta) {
    TAtomicSharedPtr<ISpillFile> res;
    return res;
}

TNamespaceCache::TNamespaceCache(const TString& name, ui32 id, TAtomicSharedPtr<ISpillStorage> storageI) : 
    Name_(name), 
    Id_(id),
    StorageI_(storageI)  {

    NextNamespaceFile(true);


    };


}
}
