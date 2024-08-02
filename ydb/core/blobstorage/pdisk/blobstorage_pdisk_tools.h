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
    TIntrusivePtr<NPDisk::TSectorMap> sectorMap = nullptr, bool enableSmallDiskOptimization = true,
    std::optional<TRcBuf> metadata = std::nullopt);

bool ReadPDiskFormatInfo(const TString &path, const NPDisk::TMainKey &mainKey, TPDiskInfo &outInfo,
    const bool doLock = false, TIntrusivePtr<NPDisk::TSectorMap> sectorMap = nullptr);

// reads metadata from PDisk (if available)
NPDisk::EPDiskMetadataOutcome ReadPDiskMetadata(const TString& path, const NPDisk::TMainKey& key, TRcBuf& metadata,
    std::optional<ui64> *pdiskGuid);

// updated PDisk metadata (when PDisk is properly formatted and supports metadata vault); size of metadata should not
// exceed 15 MiB; when function fails (even many times), previusly stored metadata must be kept intact
NPDisk::EPDiskMetadataOutcome WritePDiskMetadata(const TString& path, const NPDisk::TMainKey& key, TRcBuf&& metadata,
    std::optional<ui64> *pdiskGuid);

} // NKikimr
