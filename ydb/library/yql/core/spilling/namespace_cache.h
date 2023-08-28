#include "interface/spilling.h"
#include "storage/storage.h"

#include <map>
#include <deque>
#include <set>
#include <unordered_map>


namespace NYql {
namespace NSpilling {



const ui32 MetaFileIncreaseStep = 10000; // 10 KB increase in meta file size
const ui32 DataFileIncreaseStep = 100000000; // 100 MB increase in data file size
const ui32 ReadBufSize = 4 * 1024 * 1024; // 4 MB read buffer size

// Defines possible processing states of the task
enum class EProcessingStatus { 
    Added = 1,                  // Task added to queue
    Processing = 2,             // Task is processing
    Completed = 3,              // Task is completed
    NeedRetry = 4,              // Scheduled for retry
    Failed = 5,                 // Task is failed
    Deleted = 6,                // Task is deleted, no valid internal state
    NewVersion = 7              // New version of object is available
}; 


// Base task for namespace operations
struct TBaseTask {
    EOperationType OpType = EOperationType::Add; // Type of operation
    ui32 ObjId; // Identifier of the object
    ui32 SessionId; // Identifier of the session for this task
    TString Name; // Name of the object or stream
    ui32 StreamBufId; // Id of the object inside the stream (0, 1, 2, ...)
    TAtomicSharedPtr<TBuffer> Buf; // Data to save or load
    EProcessingStatus ProcessingStatus = EProcessingStatus::Added; // Contains current status of task processing stage
};


// Task for save operations
struct TSaveTask : public TBaseTask {
    NThreading::TPromise<TOperationResults> Promise; // Promise to report operation results    
};


// Task for load operations
struct TLoadTask : public TBaseTask {
    NThreading::TPromise<TLoadOperationResults> Promise; // Promise to report load operation results    
};

// Defines possible retention period of file.  All temporary files are deleted automacially on session close.
// Persistent files store objects as defined in object retention period. 
enum class EFileLifespan {
    Temporary = 0,
    Persistent = 1
};

// Defines possible types of spill files.  Meta files contain small index of objects stored in data files
enum class EFileType {
    Meta = 0,
    Data = 1
};

// Namespace file description
struct TNamespaceFileDesc {
    TString Name; // Full name of the file
    EFileType Type = EFileType::Meta; // Type of the file
    EFileLifespan Lifespan = EFileLifespan::Temporary;  // Lifespan of the spill file.  All unlocked temporary files are deleted automatically
    ui32 Id = 1; // Serial id of the file in the namespace.  Id of the file defines interval of Object Ids, stored in file
    ui32 Version = 1; // Version of the file in the namespace for the same id.

};

// Session info stored in the namespace cache
struct TSessionDesc {
    ui32 MinObjId; // Mininum object id associated with session   
};

// Attributes of meta file to be used
struct TMetaFileAttributes {
    TAdaptiveLock Lock; // Lock to serialize access to file attributes
    bool Valid; // True if namespace meta file exists and valid
    TString Name; // Name of the file
    ui32 Id = 0;  // Id of the file in the namespace
    TAtomicSharedPtr<ISpillFile> MetaFile; // File handle of the meta file
    TAtomicSharedPtr<ISpillFile> DataFile; // File handle of the data file
    ui32 StartPos = 0; // Start valid position in file
    ui32 EndPos = 0; // End of valid position in file
    ui32 FirstObjId = 0; // First object id in file
    ui32 LastObjId = 0;  // Last object id in file
    TAtomicSharedPtr< std::map<ui32, TSpillMetaRecord> >  MetaRecords; // Cached meta records of the file
};



// Class to represent internal cache for particular namespace
class TNamespaceCache {
public:
    // Adds required spilling data to save queue
    NThreading::TFuture<TOperationResults> Save(const TString& objName,  TBuffer && buf, ui32 sessionId, bool isStream = false );

    // Loads data from namespace cache or long term storage
    NThreading::TFuture<TLoadOperationResults> Load(const TString& name, ui32 sessionId, EObjectsLifetime objLifetime = EObjectsLifetime::DeleteAfterLoad, bool isStream = false, ui32 streamId = 0 );

    // Dispatches worker thread to process queues
    void Dispatch();

    // Return data statistics for particular session
    TSessionDataStat GetSessionStat(ui32 sessionId);

    // Closes session and clears all resources
    void CloseSession(ui32 sessionId);

    // Makes garbage collection for files stored in namespace
    void GarbageCollection();

    // Returns current number of objects stored in particular stream name
    ui64 StreamSize(const TString& name); 

    // name - name of the namespace, id - internal id, storageI - storage interface to save Namespace Cache state 
    TNamespaceCache(const TString& name, ui32 id, TAtomicSharedPtr<ISpillStorage> storageI);
private:

    // Creates next namespace file.  If openExisting = true, last namespace file is reused
    void NextNamespaceFile(bool openExisting = true);

    // Advances next object id, taking into account cyclic requirement
    inline void AdvanceObjId(ui32& objId);

    // Finds object with ObjId in Save queue. Returns true if found and object position in queue
    inline bool FindObjInSaveQueue(ui32 objId, ui32& pos);

    // Updates mininum object id for particular session if required
    inline void UpdateMinSessionId(ui32 sessionId, ui32 objId);

    // Deletes task from save queue. Returns bytes deleted in save task buffer.
    ui32 DeleteTaskFromSaveQueue(ui32 objId, EProcessingStatus reason = EProcessingStatus::Deleted);

     // Changes object id for the object 
    void ChangeLastObjId(TString& objName, ui32 prevObjId, ui32 nextObjId);

    // Processes save queue
    void ProcessSaveQueue();

    // Processes load queue
    void ProcessLoadQueue();

    // Processes delete queue
    void ProcessDeleteQueue();

    // Finds next valid save task in save queue and returns true is task found. Task position in queue is returned in taskPos.
    bool FindNextTaskInSaveQueue(ui32& taskPos);

    // Finds next valid  task in load queue and returns true is task found. Task position in queue is returned in taskPos.
    bool FindNextTaskInLoadQueue(ui32& taskPos);

    // Finds next valid  task in delete queue and returns true is task found. Task position in queue is returned in taskPos.
    bool FindNextTaskInDeleteQueue(ui32& taskPos);

    // Removes all completed tasks from beginning of ToSave_ queue
    void RemoveCompletedFromSaveQueue();

    // Removes all completed tasks from beginning of ToLoad_ queue
    void RemoveCompletedFromLoadQueue();

   // Removes all completed tasks from beginning of ToDelete_ queue
    void RemoveCompletedFromDeleteQueue();

    // Returns true if object is found in meta file fileId.  
    // Meta record for particular object is returned in mr argument.  File meta information is returned in fileMet record.
    bool FindMetaRecordInFile(ui32 fileId, ui32 objId, TSpillMetaRecord& mr, TAtomicSharedPtr<TMetaFileAttributes>& fileMet );

    // Inits meta file with file id
   TAtomicSharedPtr<TMetaFileAttributes> InitMetaFile(ui32 fileId);

    // Marks obj id as deleted
    inline void SetDeletedBit(ui32 objId);

    // Returns required file handle for particular object id
    TAtomicSharedPtr<ISpillFile> FindSpillFile(ui32 objId, EFileType fileType = EFileType::Meta);

    // Finds iterator of object with provided objId inside 

    TAdaptiveLock FilesLock_; // Lock to serialize access to internal data structures associated with files content
    TAdaptiveLock ToSaveLock_; // Lock to serialize access to save queue
    TAdaptiveLock ToLoadLock_; // Lock to serialize access to load queue
    TAdaptiveLock ToDeleteLock_; // Lock to serialize access to delete queue
    TString Name_; // Name of the namespace
    ui32 Id_ = 0; // Internal id of the namespace
    std::deque<TSaveTask> ToSave_;    // Queue to spill data
    std::deque<TLoadTask> ToLoad_;    // Queue to load spilled data 
    std::deque<TBaseTask> ToDelete_;  // Queue to delete spilled data
    std::unordered_map<TString, ui32> LastObjs_; // Latest version of objects with particular name
    std::unordered_map<TString, std::vector<ui32>> LastStreamIds_; // Latest buff id of particular stream 
    std::unordered_map<ui32, TSessionDesc> Sessions_; // Sessions descriptions by session id key
    std::unordered_map<ui32, std::atomic<ui64>> SessionDataProvided_; // Data provided to spill per session
    std::unordered_map<ui32, std::atomic<ui64>> SessionDataSpilled_; // Data spilled to storage from memory per session
    std::unordered_map<ui32, std::atomic<ui64>> SessionDataLoadedFromStorage_; // Data loaded from storage to memory per session
    std::unordered_map<ui32, std::atomic<ui64>> SessionDataLoadedFromMemory_; // Data loaded from memory to memory per session
    TAtomicSharedPtr<ISpillStorage> StorageI_; // Interface to storage to work with namespace
    TAtomicSharedPtr<ISpillFile> CurrSpillMetaFile_ = nullptr; // Current spill meta file to write data
    TAtomicSharedPtr<ISpillFile> CurrSpillDataFile_ = nullptr; // Current spill file to write data
    ui32 CurrSpillFileId_ = 0; // Current id of spill file in the namespace
    std::atomic<ui32> NextObjId_ = 0;  // Next object id in the namespace
    std::map<ui32, TAtomicSharedPtr<TMetaFileAttributes> > SpillMetaFiles_; // Current open spill meta files for particular namespace
    std::set<ui32> SpillFilesIdToDelete_;  // Id of spill files which need to be deleted

};


}
}
