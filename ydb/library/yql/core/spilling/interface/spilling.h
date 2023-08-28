#pragma once

#include <library/cpp/threading/future/async.h>
#include <util/generic/buffer.h>

namespace NYql {
namespace NSpilling {

struct TFileStorageConfig;  // Configuration attributes to instantiate TempStorageProxy stored in filesystem folder
struct TTempStorageExecutionPolicy; // Execution policy to work with temporary storage
struct TTempObjectDesc;  // Represents description of particular objects in temporary storage
class ITempStorageProxy;  // Proxy class to provide unified interface for all types of temporary storage
struct TOperationResults; // Contains information about particular operation results

// Factory method to create TempStorageProxy for usage.  Error reasons are returned in TOperatonResults 
std::pair<THolder<ITempStorageProxy>, TOperationResults> CreateFileStorageProxy(const TFileStorageConfig& config, const TTempStorageExecutionPolicy& policy );

struct TLoadOperationResults; // Contains information about load operation together with loaded data

class ISession; // Session to manage temp objects lifecycle for particular process
struct TSessionExecutionPolicy; // Execution policy for session to redefine default TTempStorageExecutionPolicy settings
class IStream;  // Class to save and load temp objects to storage in stream mode. Stream serializes objects order as it was saved, allows random
                // access to stream objects by object index and provides consistency guarantee for all objects in stream when Close call succeed.


// Iterator over temp storage objects
class IObjectsIterator {
public:
 
    virtual bool Next(TTempObjectDesc& object) = 0; // Fills next temporary object description. Returns true if there are more objects
    virtual ~IObjectsIterator() = default;
};


// Amount of data processed per session
struct TSessionDataStat {
    ui64 Provided = 0;                  // Total data provided by client to save in session (MB)
    ui64 InMemory = 0;                  // Amount on data for session in memory (MB)
    ui64 Spilled = 0;                   // Session data spilled to storage or deleted in memory (MB)
    ui64 LoadedFromStorage = 0;         // Amount of data loaded from storage (MB)
    ui64 LoadedFromMemory = 0;          // Session data processed in-memory without spilling (MB)
};


// Possible lifetime management options for objects stored in particular session
enum class EObjectsLifetime {
    DeleteAfterLoad = 0,    // Object is deleted automatically from spilling storage after load operation. Object ownership is on the caller side after load call.
    SharedForSession = 1,   // Object can be loaded many times during session and object memory is managed by shared pointer. Pointer is shared between spilling Session and all callers for the object.
    Persistent = 2,         // Object is persistent and stored in spilling storage until deleted 
 };


class ISession {
public:
    // Saves buf with size bytes to temporary storage. buf content should not be changed until operation completes.
    // TOperationResults contains final results of buffer saving. buf is deleted after saving.
    virtual NThreading::TFuture<TOperationResults> Save(const TString& objNamespace, const TString& name, TBuffer&& buf) = 0;

    // Loads data from temporary storage to Buf in TLoadOperationResults.
    virtual NThreading::TFuture<TLoadOperationResults> Load(const TString& objNamespace, const TString& name, EObjectsLifetime objLifetime = EObjectsLifetime::DeleteAfterLoad) = 0;

    // Return current size of all temporary buffers to save.  It should be used to control speed of Save operations from the client side
    virtual TSessionDataStat GetSessionDataStat() = 0;

    virtual TSessionExecutionPolicy ExecutionPolicy() = 0; // Returns current execution policy of ISession
    virtual TOperationResults SetExecutionPolicy(const TSessionExecutionPolicy& policy) = 0;  // Changes execution policy of ISession

    // Opens stream to save or load data.  Possible errors are returned in TOperationResults
    virtual std::pair<THolder<IStream>, TOperationResults> OpenStream(const TString& objNamespace, const TString& streamName ) = 0;

    virtual ~ISession() = default;
};

class IStream {
public:
    // Saves buf to stream.  Buffer id in stream is returned in TOperationResults. Buffer id is a 0,1,2,3..(Size of stream - 1)  sequence.
    // Stream guarantees buffer id order the same as it was Save calls order.
    virtual NThreading::TFuture<TOperationResults> Save(TBuffer&& buf) = 0;

    // Return current number of objects stored in stream.
    virtual ui64 Size() = 0;

    // Loads data from stream with provided buffer id
    virtual NThreading::TFuture<TLoadOperationResults> Load(ui64 bufferId = 0, EObjectsLifetime objLifetime = EObjectsLifetime::DeleteAfterLoad) = 0;

    // Closes stream to ensure consistensy of all stream data buffers during subsequent load operations.  
    virtual NThreading::TFuture<TOperationResults> Close() = 0;

    virtual ~IStream() = default;
};





class ITempStorageProxy {
public:

    // Creates new session to store and load temporary objects;  Session manages lifecycle of all resources associated with the session.
    // When session is deleted, all pending load operations are canceled, all resources associated with the session are freed
    virtual THolder<ISession> CreateSession() = 0;  

    // Creates iterator to enumerate stored objects of interest. It works both for namespaces and  objects enumeration.
    // Objects are identified by namespace and name.  If onlyValid = true, only valid objects are returned
    virtual THolder<IObjectsIterator> CreateIterator(   const TMaybe<TString>& objNamespace = TMaybe<TString>(),
                                                        const TMaybe<TString>& objName = TMaybe<TString>(), 
                                                        bool onlyValid = true) = 0;    
                                                                            

    virtual TTempStorageExecutionPolicy ExecutionPolicy() = 0; // Returns current execution policy of ITempStorageProxy
    virtual TOperationResults SetExecutionPolicy(const TTempStorageExecutionPolicy& policy) = 0;  // Changes execution policy of ITempStorageProxy

    // Deletes object with particular namespace and name.  If name is empty, all objects from particular namespace are deleted.  
    virtual NThreading::TFuture<TOperationResults> Delete(const TString& objNamespace, const TMaybe<TString>& name) = 0;
    
    virtual ~ITempStorageProxy() = default;

};


struct TTempStorageExecutionPolicy {
    ui64 MaxStorageSize = 100; // Maximim size of temporary storage in GB. We are trying to keep total size of temporary objects withing this limit
    ui64 RetentionPeriod = 24; // Default retention period in hours for temporary objects
    ui64 DeleteOnClose = true; // When true, all temporary objects are deleted automatically when Session object is closed
    bool CanClearNow = true;  // If true, background deletion of obsolete objects is performed
    ui64 MaxBandwidth = 10 * 1000; // ( Maximim bandwidth limitation for particular TempStorageProxy in MB/sec.  Default is 10 GB/sec )
    ui64 MaxCallsPerSecond = 100; // Maximim calls per second for particular TempStorageProxy
    ui64 OneCallTimeout = 10; // Default timeout for one call to storage in seconds.
    bool RetryCall = true; // If true, operation we are trying to complete operation using retry policy settings
    ui64 RetryPeriod = 2; // Default period for retry calls in seconds
    bool DoubleRetryPeriod = true;  // Doubles retry period util it reaches MaxRetryPeriod;
    ui64 MaxRetryPeriod = 60; // Maximim retry period in seconds
    ui64 MaxNumberOfRetries = 10; // Maximim number of retries to complete operation.
    ui64 MaxBuffersSize = 2000; // Total size of all process internals buffers in MB waiting either to load or store.  
                                // When this limit exceeded, Save and Load operations starts returning BuffersAreFull error
};

struct TSessionExecutionPolicy {
    ui64 DeleteOnClose = true; // When true, all temporary objects are deleted automatically when Session object is closed
    ui64 MaxBuffersSize = 100; // Total size of session internals buffers in MB waiting either to load or store.  
                                // When this limit exceeded, Save and Load operations starts returning  BuffersAreFull error
};

// Possible reasons why object is considered invalid
enum class EBadObjectReason {
    NoBadReason = 0,
    ChecksumInvalid = 1, 
    SizeInvalid = 2, 
    NoNamespace = 3, 
    NoObjectName = 4, 
    ReadError = 5 
 };

struct TTempObjectDesc {
    TString Namespace; // Namespace name. Empty for global namespace
    TString Name; // Object name. Empty for namespaces. For stream objects it is a name of the stream
    ui64 Size; // Size of the object in bytes.  Total size of the objects in namespace in case of namespaces
    ui64 Index; // Index of the object in the stream, starting from 0
    bool IsValid; // True if object seems to be correct.  Final consistency check is performed during full object load
    bool IsFinal; // True if object is final in the stream
    bool StreamValid; // True if stream is closed correctly
    EBadObjectReason BadObjectReason; // Contains reason why object is considered invalid
};

// Possible statuses of operation
enum class EOperationStatus {
    Success = 0,
    Failure = 1,
    CannotOpenStorageProxy = 2,
    WrongBufferId = 3,
    ProxyTerminated = 4,
    BuffersAreFull = 5,
    NoObjectName = 6, 
    NewVersionDefined = 7,
    ChecksumInvalid = 8 
};

// Results of operation
struct TOperationResults {
    EOperationStatus Status; // Final status of operation
    ui64 BufferId; // Buffer id for stream buffers sequence
    TString ErrorString; // Contains error description for logs and following diagnostics
};

// Results of load operation
struct TLoadOperationResults: public TOperationResults {
   TAtomicSharedPtr<TBuffer> Buf; //  Buffer with data after load operation.
};

struct TFileStorageConfig {
    TString Path; // Storage path location
};

}
}