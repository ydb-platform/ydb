#pragma once

#include <library/cpp/threading/future/async.h>
#include <ydb/library/yql/core/spilling/interface/spilling.h>
#include <util/system/datetime.h>

namespace NYql {
namespace NSpilling {

class ISpillStorage;  // Unified interface to implement spilling based on any supported storage type

// Factory method to create ISpillStorage based on file system for usage.  Error reasons are returned in TOperatonResults 
std::pair< THolder<ISpillStorage>, TOperationResults > OpenFileStorageForSpilling(const TFileStorageConfig& config);

const ui32 MagicForFileRecord32 =   0xD0F5F39F; // Magic header number to restore records in case of broken file

// Defines possible operation types
enum class EOperationType { 
    Add = 1,            // Add new temporary object
    Delete = 2,         // Delete temporary object
    TimeMark = 3,       // Add timemark to estimate timing of operations in meta file
    StreamAdd = 4,      // Add new stream sequence
    StreamDelete = 5,   // Deletes stream sequence
    StreamBufAdd = 6,   // Adding new stream buffer
    SessionDelete = 7,  // Deletes session and all objects associated with session
}; 


// Storage meta record is used to describe particular spill object operation
class TSpillMetaRecord {
public:
    EOperationType GetOpType();  // Returns operation type of the record
    ui32 GetNameSize(); // Returns size of the record name
    ui32 Size();  // Total size of record in bytes
    ui32 DataSize(); // Size of the record Data
    ui32 Offset(); // Offset of the data in data file
    ui32 DataHash(); // Returns data hash
    void Pack (TBuffer& buf ); // Serializes record to buf. After the call Buf is ready to be written to spill meta file
    void Unpack (TBuffer& buf ); // Restores internal state from buf.
    TString AsString();  // Returns string representation of SpillMetaRecord
    void SetDataHash(const ui32 hash) {DataHash_ = hash;} 
    void SetObjId(const ui32 objId) {RecordNumber_ = objId;}
    // Scans buf for all valid spill meta records and return all recods found in record argument.  lastValidOffset - last byte offset of last valid record in buf
    static void ScanForValidSpillRecords(TBuffer& buf, ui32& lastValidOffset, std::vector<TSpillMetaRecord>& records );
    TSpillMetaRecord(EOperationType opType, TString& name, ui32 offset, ui32 recordNum, ui32 dataSize, ui32 dataHash  );
    TSpillMetaRecord();
private:
    void SetOpType(EOperationType opType = EOperationType::Add); // Defines operation type of the record
    void SetNameSize(); // Sets str size in bytes in Meta1 record
    ui32 RecordHash_ = 0; // Hash number to check consistency of record
    ui32 Offset_ = 0; // Offset of the object in the data file
    ui32 RecordNumber_ = 0; // Global increasing record number for particular namespace
    ui32 Meta1_ = 0; // Meta information about the record.  See below description of bit masks to unpack info from this field
    ui32 DataSize_ = 0; // Data size of the buffer in dat file associated with record
    ui32 DataHash_ = 0; // Hash of buffer in dat file
    std::string Name_; // Name of the object
    ui32 Bytes_ = 0; // Total bytes of spill meta record in meta file (28 + size of Name rounded to 4)

};

struct TSpillStreamBufRecord : public TSpillMetaRecord {
    ui32 StreamId = 0; // Record id of the stream
    ui32 BufId = 0; // Id of the buffer inside the stream
};

const ui32 Meta1OperationBitMask    =   0xF0000000;  // Defines operation type stored in the record
const ui32 Meta1OperationStrLen     =   0x0000FFFF;  // Bit mask to extract length of spill object name (maximum 4095 bytes)
const ui32 Meta1OperationRetention  =   0x0FF00000; // Retention period in hours.  Value 0xFF (255) means unlimited retention period, it should be deleted manually


// Time mark record to estimate in between records addition time
struct TTimeMarkRecord : public TSpillMetaRecord {
    ui32 Microseconds1 = 0; // Current Microseconds time since epoch (minor 32 bits)
    ui32 Microseconds2 = 0; // Current Microseconds time since epoch (major 32 bits)
};

// Interface for spill file for particular namespace
class ISpillFile {
public:
    virtual TString GetName() = 0;  // Returns full name of the file
    virtual bool IsLocked() = 0;    // True when file is locked
    virtual ui64 Reserve(ui32 size) = 0; // Reserves size bytes for writing, returns file offset
    virtual void Write(ui32 offset, const char * data, ui32 bytes) = 0;
    virtual void Seek(ui32 offset) = 0;
    // Reads up to 1 GB without retrying, returns -1 on error
    virtual i32 Read(ui32 offset, char* buf, ui32 len) = 0;
    virtual void Delete() = 0; // Deletes spill file
    virtual ~ISpillFile() = default;
};
    

// Interface to work with particular 
class ISpillStorage {
public:

    virtual ui64 GetCurrSize() = 0; // Returns current size of spill storage

    // Returns full list of namespaces for current spill storage. 
    // All namespaces are on the same level and don't form hierarchy.
    // Number of namespaces should be reasonable (no more than 1000)
    // Namespace name should be alphanumerical only.
    virtual TVector<TString> GetNamespaces() = 0;

    // Returns list of file names for namespace
    virtual TVector<TString> GetNamespaceFiles(const TString& ns) = 0;

    // Creates file in namespace ns with name fn. File interface ISpillFile is ready for writing and locked in case of success.  
    // Caller should check success with LastOperationResults.  reserveStep is a step to increase allocated file storage.
    virtual THolder<ISpillFile> CreateSpillFile(const TString& ns, const TString& fn, ui32 reserveStep) = 0;

    // Returns last operation results
    virtual TOperationResults LastOperationResults() = 0;


    virtual ~ISpillStorage() = default;

};


}
}