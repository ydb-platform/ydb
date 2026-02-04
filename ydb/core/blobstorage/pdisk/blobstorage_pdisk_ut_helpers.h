#pragma once
#include "defs.h"

#include "blobstorage_pdisk_ut_context.h"

#include <optional>
#include <ydb/library/pdisk_io/buffers.h>

#include <ydb/core/protos/base.pb.h>

namespace NKikimr {

TString EnsurePDiskExists(TTestContext *tc);
TString PrepareData(ui32 size, ui32 flavor = 0);
TString StatusToString(const NKikimrProto::EReplyStatus status);
TString MakeDatabasePath(const char *dir);
TString MakePDiskPath(const char *dir);
TString CreateFile(const char *baseDir, ui32 dataSize);
void FormatPDiskForTest(TString path, ui64 guid, ui32& chunkSize, ui64 diskSize, bool isErasureEncodeUserLog,
        TIntrusivePtr<NPDisk::TSectorMap> sectorMap, bool enableSmallDiskOptimization = false,
        bool plainDataChunks = false, bool enableFormatAndMetadataEncryption = true,
        std::optional<bool> enableSectorEncryption = std::nullopt,
        std::optional<bool> forceRandomizeMagic = std::nullopt);

void ReadPdiskFile(TTestContext *tc, ui32 dataSize, NPDisk::TAlignedData &outData);
i64 FindLastDifferingBytes(NPDisk::TAlignedData &dataBefore, NPDisk::TAlignedData &dataAfter, ui32 dataSize);
ui64 DestroyLastSectors(TTestContext *tc, NPDisk::TAlignedData &dataBefore, NPDisk::TAlignedData &dataAfter,
        ui32 dataSize, ui32 count);
ui64 RestoreLastSectors(TTestContext *tc, NPDisk::TAlignedData &dataBefore, NPDisk::TAlignedData &dataAfter,
        ui32 dataSize, ui32 count);
void FillDeviceWithPattern(TTestContext *tc, ui64 chunkSize, ui64 pattern);
void FillDeviceWithZeroes(TTestContext *tc, ui64 chunkSize);
void WriteSectors(TTestContext *tc, NPDisk::TAlignedData &dataAfter, ui64 firstSector, ui32 count);
void DestroySectors(TTestContext *tc, const NPDisk::TAlignedData &dataAfter,
        ui32 dataSize, ui64 firstSector, ui32 period);
void OutputSectorMap(NPDisk::TAlignedData &dataBefore, NPDisk::TAlignedData &dataAfter, ui32 dataSize);

} // NKikimr
