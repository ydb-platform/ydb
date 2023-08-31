#include "storage.h"

#include <format>
#include <contrib/libs/xxhash/xxhash.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {
namespace NSpilling {

inline ui32 CalcUi32StrLen(ui32 bytes) {
    if (!bytes) return 0;
    return bytes / sizeof(ui32) + 1;
}

EOperationType TSpillMetaRecord::GetOpType() {
    ui32 opType = (Meta1_ & Meta1OperationBitMask) >> 28; // Calculating record type
    return  (EOperationType) opType;

}


ui32 TSpillMetaRecord::GetNameSize() {
    return (Meta1_ & (Meta1OperationStrLen) ); 
}


ui32 TSpillMetaRecord::Size() {
    ui32 res;

    res = 7 * sizeof(ui32); // Meta size + total record size
    res += (CalcUi32StrLen(Name_.size() ) ) * sizeof(ui32);

    EOperationType eOpType = GetOpType();

    switch (eOpType) {
        case EOperationType::TimeMark:
            res += 2 * sizeof(ui32);
            break;
        case EOperationType::StreamBufAdd:
            res += 2 * sizeof(ui32);
            break;
    }

    return res;
}

ui32 TSpillMetaRecord::DataSize() {
    return DataSize_;
}

ui32 TSpillMetaRecord::Offset() {
    return Offset_;
}

ui32 TSpillMetaRecord::DataHash() {
    return DataHash_;
} 

void TSpillMetaRecord::Pack (TBuffer &buf ) {
    ui32 size = Size();
    buf.Resize(size);

    ui32 * bufPtr = (ui32*) buf.Data();
    memset(bufPtr, 0, size);
    *(bufPtr) = RecordHash_;
    *(bufPtr+1) = Offset_;
    *(bufPtr+2) = RecordNumber_;
    *(bufPtr+3) = Meta1_;
    *(bufPtr+4) = DataSize_;
    *(bufPtr+5) = DataHash_;
    ui32 strUi32Size = CalcUi32StrLen(Name_.size());
    std::copy_n(Name_.data(), Name_.size(), buf.Data() + 6*sizeof(ui32) );
    *(bufPtr + 6 + strUi32Size ) = size;
    XXH32_hash_t hash = XXH32( buf.Data() + sizeof(ui32), size - sizeof(ui32), 0);
    *(bufPtr) = hash;
    RecordHash_ = hash;

}

void TSpillMetaRecord::Unpack (TBuffer &buf ){
    ui32 * bufPtr = (ui32*) buf.Data();
    RecordHash_ = *(bufPtr);
    Offset_ = *(bufPtr+1);
    RecordNumber_ = *(bufPtr+2);
    Meta1_ = *(bufPtr+3);
    DataSize_ = *(bufPtr+4);
    DataHash_ = *(bufPtr+5);
    ui32 nameSize = GetNameSize();
    Name_.clear();
    Name_.append( buf.Data() + 6*sizeof(ui32) , nameSize );
    XXH32_hash_t hash = XXH32( buf.Data() + sizeof(ui32), Size() - sizeof(ui32), 0);
    if (hash != RecordHash_) {
        YQL_LOG(ERROR) << "Invalid hash: " << hash << " Record hash: " << RecordHash_ << Endl;
    }
}

TString TSpillMetaRecord::AsString() {
    std::string res;
    res = "RecordHash: " + std::to_string(RecordHash_) + " |Offset: " + std::to_string(Offset_) + " |RecordNumber: " + std::to_string(RecordNumber_) + " |Meta1: " + std::format("{:#08x}", Meta1_) + 
            " |DataSize: " + std::to_string(DataSize_) + " |DataHash: " + std::to_string(DataHash_) + " |Name: " + Name_;

    return std::move(res);

}


void TSpillMetaRecord::SetOpType(EOperationType opType) {
    ui32 opTypeVal = ((ui32) opType) << 28;
//    Cout << "opTypeVal: " << opTypeVal << Endl;
    Meta1_ = (opTypeVal | (Meta1_ & (~Meta1OperationBitMask) )); 
}


void TSpillMetaRecord::SetNameSize() {
    ui32 size = Name_.size();
    Meta1_ = (size | (Meta1_ & (~Meta1OperationStrLen) )); 
 
}


void TSpillMetaRecord::ScanForValidSpillRecords(TBuffer& buf, ui32& lastValidOffset, std::vector<TSpillMetaRecord>& records ) {

}

TSpillMetaRecord::TSpillMetaRecord(EOperationType opType, TString& name, ui32 offset, ui32 recordNum, ui32 dataSize, ui32 dataHash  ) : 
    Name_(name), 
    Offset_(offset), 
    RecordNumber_(recordNum),
    DataSize_(dataSize),
    DataHash_(dataHash) 
{
    SetOpType(opType);
    SetNameSize();
    Bytes_ = Size();
}

TSpillMetaRecord::TSpillMetaRecord()  {};

}
}

