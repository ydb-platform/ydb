#pragma once
#include "blobstorage_pdisk_defs.h"

#include <ydb/library/pdisk_io/sector_map.h>

namespace NActors {
    struct TActorSetupCmd;
}

namespace NKikimr {

struct TPDiskInfo {
    ui64 Version;
    ui64 DiskSize;
    ui32 SectorSizeBytes;
    ui32 UserAccessibleChunkSizeBytes;
    ui64 DiskGuid;
    TString TextMessage;
    ui32 RawChunkSizeBytes;
    ui32 SysLogSectorCount;
    ui32 SystemChunkCount;
    TInstant Timestamp;
    TString FormatFlags;

    TString ErrorReason;  // Actually not a part of the format info, contains human-readable error description

    struct TSectorInfo {
        ui64 Nonce;
        ui64 Version;
        bool IsCrcOk;
        TSectorInfo(ui64 nonce, ui64 version, bool isCrcOk)
            : Nonce(nonce)
            , Version(version)
            , IsCrcOk(isCrcOk)
        {}
    };
    TVector<TSectorInfo> SectorInfo;
};

// Throws TFileError in case of errors
void ObliterateDisk(TString path);

void FormatPDisk(TString path, ui64 diskSizeBytes, ui32 sectorSizeBytes, ui32 userAccessibleChunkSizeBytes,
    const ui64 &diskGuid, const NPDisk::TKey &chunkKey, const NPDisk::TKey &logKey,
    const NPDisk::TKey &sysLogKey, const NPDisk::TKey &mainKey, TString textMessage,
    const bool isErasureEncodeUserLog = false, const bool trimEntireDevice = false,
    TIntrusivePtr<NPDisk::TSectorMap> sectorMap = nullptr);

bool ReadPDiskFormatInfo(const TString &path, const NPDisk::TMainKey &mainKey, TPDiskInfo &outInfo,
    const bool doLock = false, TIntrusivePtr<NPDisk::TSectorMap> sectorMap = nullptr);


// Size is better to be 2^n, to optimize mod operation
template <ui32 S>
struct TOperationLog {
    constexpr static ui32 Size = S;
    std::array<TString, S> Records;
    std::atomic<ui64> RecordIdx = std::atomic<ui64>(0);

    TString Print() const {
        TStringStream str;
        str << "[ " ;
        /* Print OperationLog records from newer to older */ 
        for (ui32 i = RecordIdx.load(), ctr = 0; ctr < Size; ++ctr, i = (i == 0) ? Size - 1 : i - 1) {
            str << "Record# " << ctr <<  ": { " << Records[i] << " }, ";
        }
        str << " ]";
        return str.Str();
    }
};

} // NKikimr

#define ADD_OPERATION_TO_LOG(log, record)                                                               \
    do {                                                                                                \
        ui32 idx = log.RecordIdx.fetch_add(1);                                                          \
        log.Records[idx % log.Size] = TStringBuilder() << TInstant::Now().ToString() << " " << record;  \
    } while (false)
