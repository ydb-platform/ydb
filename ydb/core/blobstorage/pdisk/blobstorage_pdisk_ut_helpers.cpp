#include <ydb/library/pdisk_io/buffers.h>
#include "blobstorage_pdisk_actorsystem_creator.h"
#include "blobstorage_pdisk_ut.h"
#include "blobstorage_pdisk_ut_helpers.h"

#include <ydb/core/blobstorage/crypto/default.h>

#include <util/folder/dirut.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimr {

TString PrepareData(ui32 size, ui32 flavor) {
    TString data = TString::Uninitialized(size);
    for (ui32 i = 0; i < size; ++i) {
        data[i] = '0' + (i + size + flavor) % 8;
    }
    return data;
}

TString StatusToString(const NKikimrProto::EReplyStatus status) {
    return NKikimrProto::EReplyStatus_Name(status);
}

TString MakeDatabasePath(const char *dir) {
    TString databaseDirectory = Sprintf("%s/yard", dir);
    return databaseDirectory;
}

TString MakePDiskPath(const char *dir) {
    TString databaseDirectory = MakeDatabasePath(dir);
    if (IsRealBlockDevice) {
        return RealBlockDevicePath;
    } else {
        return databaseDirectory + "/pdisk.dat";
    }
}

void FormatPDiskForTest(TString path, ui64 guid, ui32& chunkSize, ui64 diskSize, bool isErasureEncodeUserLog,
        TIntrusivePtr<NPDisk::TSectorMap> sectorMap, bool enableSmallDiskOptimization) {
    NPDisk::TKey chunkKey;
    NPDisk::TKey logKey;
    NPDisk::TKey sysLogKey;
    EntropyPool().Read(&chunkKey, sizeof(NKikimr::NPDisk::TKey));
    EntropyPool().Read(&logKey, sizeof(NKikimr::NPDisk::TKey));
    EntropyPool().Read(&sysLogKey, sizeof(NKikimr::NPDisk::TKey));

    if (enableSmallDiskOptimization) {
        try {
            FormatPDisk(path, diskSize, 4 << 10, chunkSize, guid, chunkKey, logKey, sysLogKey,
                    NPDisk::YdbDefaultPDiskSequence, "Info", isErasureEncodeUserLog, false, sectorMap,
                    enableSmallDiskOptimization);
        } catch (NPDisk::TPDiskFormatBigChunkException) {
            FormatPDisk(path, diskSize, 4 << 10, NPDisk::SmallDiskMaximumChunkSize, guid, chunkKey, logKey, sysLogKey,
                    NPDisk::YdbDefaultPDiskSequence, "Info", isErasureEncodeUserLog, false, sectorMap,
                    enableSmallDiskOptimization);
        }
    } else {
        FormatPDisk(path, diskSize, 4 << 10, chunkSize, guid, chunkKey, logKey, sysLogKey,
                NPDisk::YdbDefaultPDiskSequence, "Info", isErasureEncodeUserLog, false, sectorMap,
                enableSmallDiskOptimization);
    }
}

void FormatPDiskForTest(TString path, ui64 guid, ui32& chunkSize, bool isErasureEncodeUserLog,
        TIntrusivePtr<NPDisk::TSectorMap> sectorMap, bool enableSmallDiskOptimization) {
    ui64 diskSizeHeuristic = (ui64)chunkSize * 1000;
    FormatPDiskForTest(path, guid, chunkSize, diskSizeHeuristic, isErasureEncodeUserLog, sectorMap,
            enableSmallDiskOptimization);
}

void ReadPdiskFile(TTestContext *tc, ui32 dataSize, NPDisk::TAlignedData &outData) {
    VERBOSE_COUT("ReadPdiskFile");
    TString path = EnsurePDiskExists(tc);
    {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;
        THolder<TPDiskMon> mon(new TPDiskMon(counters, 0, nullptr));
        TActorSystemCreator creator;
        THolder<NPDisk::IBlockDevice> device(NPDisk::CreateRealBlockDeviceWithDefaults(path, *mon,
                    NPDisk::TDeviceMode::LockFile, tc->SectorMap, creator.GetActorSystem()));
        VERBOSE_COUT("  Performing Pread of " << dataSize);
        device->PreadSync(outData.Get(), dataSize, 0, NPDisk::TReqId(NPDisk::TReqId::Test4, 0), {});
    }
    VERBOSE_COUT("Done");
}

i64 FindLastDifferingBytes(NPDisk::TAlignedData &dataBefore, NPDisk::TAlignedData &dataAfter, ui32 dataSize) {
    ui8 *before = dataBefore.Get();
    ui8 *after = dataAfter.Get();
    for (i64 i = dataSize-1; i < dataSize; --i) {
        if (before[i] != after[i]) {
            return i;
        }
    }
    return 0x7fffffffffffffffull;
}

ui64 DestroyLastSectors(TTestContext *tc, NPDisk::TAlignedData &dataBefore, NPDisk::TAlignedData &dataAfter,
        ui32 dataSize, ui32 count) {
    VERBOSE_COUT("DestroyLastSectors: destroying " << count << " sectors.");
    TString path = EnsurePDiskExists(tc);

    i64 lastDifference = FindLastDifferingBytes(dataBefore, dataAfter, dataSize);
    ui32 sectorSize = 4096;
    VERBOSE_COUT("Differing bytes at " << lastDifference);
    // Magic 8 stands for random character of the difference.
    ASSERT_YTHROW(lastDifference < i64(dataSize) - 8, "No data changes detected.");
    ASSERT_YTHROW(lastDifference > i64(sectorSize * count) + 8, "No remotely sutable data changes detected.");
    ui64 offset = ((lastDifference - 8) / sectorSize + 1 - count) * sectorSize;
    {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;
        THolder<TPDiskMon> mon(new TPDiskMon(counters, 0, nullptr));
        NPDisk::TAlignedData buffer(sectorSize * count);
        memset(buffer.Get(), 0xf, sectorSize * count);
        TActorSystemCreator creator;
        THolder<NPDisk::IBlockDevice> device(NPDisk::CreateRealBlockDeviceWithDefaults(path, *mon,
                    NPDisk::TDeviceMode::LockFile, tc->SectorMap, creator.GetActorSystem()));
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(buffer.Get(), sectorSize * count);
        device->PwriteSync(buffer.Get(), sectorSize * count, offset, NPDisk::TReqId(NPDisk::TReqId::Test4, 0), {});
    }
    VERBOSE_COUT("Done");
    return offset / sectorSize;
}

TString EnsurePDiskExists(TTestContext *tc) {
    TString path;
    if (tc->TempDir) {
        TString databaseDirectory = MakeDatabasePath((*tc->TempDir)().c_str());
        MakeDirIfNotExist(databaseDirectory.c_str());
        path = MakePDiskPath((*tc->TempDir)().c_str());
        ASSERT_YTHROW(NFs::Exists(path), "File " << path << " does not exist.");
    } else {
        ASSERT_YTHROW(tc->IsFormatedDiskExpected(), "SectorMap does not exist.");
    }
    return path;
}

ui64 RestoreLastSectors(TTestContext *tc, NPDisk::TAlignedData &dataBefore, NPDisk::TAlignedData &dataAfter,
        ui32 dataSize, ui32 count) {
    VERBOSE_COUT("RestoreLastSectors: restoring " << count << " sectors.");
    TString path = EnsurePDiskExists(tc);

    i64 lastDifference = FindLastDifferingBytes(dataBefore, dataAfter, dataSize);
    ui32 sectorSize = 4096;
    VERBOSE_COUT("Differing bytes at " << lastDifference);
    ASSERT_YTHROW(lastDifference < i64(dataSize) - 8, "No data changes detected.");
    ASSERT_YTHROW(lastDifference > i64(sectorSize * count) + 8, "No remotely sutable data changes detected.");
    ui64 offset = ((lastDifference - 8) / sectorSize + 1 - count) * sectorSize;
    {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;
        THolder<TPDiskMon> mon(new TPDiskMon(counters, 0, nullptr));
        TActorSystemCreator creator;
        THolder<NPDisk::IBlockDevice> device(NPDisk::CreateRealBlockDeviceWithDefaults(path, *mon,
                    NPDisk::TDeviceMode::LockFile, tc->SectorMap, creator.GetActorSystem()));
        VERBOSE_COUT("Offset = " << offset << " sectorIdx = " << offset/sectorSize);
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(dataBefore.Get() + offset, sectorSize * count);
        device->PwriteSync(dataBefore.Get() + offset, sectorSize * count, offset, NPDisk::TReqId(NPDisk::TReqId::Test4, 0), {});
    }
    VERBOSE_COUT("Done");
    return offset / sectorSize;
}

void FillDeviceWithPattern(TTestContext *tc, ui64 chunkSize, ui64 pattern) {
    TString path;
    if (tc->TempDir) {
        TString databaseDirectory = MakeDatabasePath((*tc->TempDir)().c_str());
        MakeDirIfNotExist(databaseDirectory.c_str());
        path = MakePDiskPath((*tc->TempDir)().c_str());
    }
    ui64 diskSizeHeuristic = chunkSize * 1000;
    if (tc->SectorMap) {
        tc->SectorMap->ForceSize(diskSizeHeuristic);
    } else {
        TFile file(path.c_str(), OpenAlways | RdWr | Seq | Direct);
        file.Flock(LOCK_EX | LOCK_NB);
        file.Resize(diskSizeHeuristic);
        file.Close();
        ASSERT_YTHROW(NFs::Exists(path), "File " << path << " does not exist.");
    }

    const ui32 formatSectorsSize = NPDisk::FormatSectorSize * NPDisk::ReplicationFactor;
    NPDisk::TAlignedData data(formatSectorsSize);

    Y_ABORT_UNLESS(data.Size() % sizeof(ui64) == 0);
    Fill((ui64*)data.Get(), (ui64*)(data.Get() + data.Size()), pattern);

    {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;
        THolder<TPDiskMon> mon(new TPDiskMon(counters, 0, nullptr));
        TActorSystemCreator creator;
        THolder<NPDisk::IBlockDevice> device(NPDisk::CreateRealBlockDeviceWithDefaults(path, *mon,
                    NPDisk::TDeviceMode::LockFile, tc->SectorMap, creator.GetActorSystem()));
        VERBOSE_COUT("Filling first " << data.Size() << "bytes of device with data");
        device->PwriteSync(data.Get(), data.Size(), 0, NPDisk::TReqId(NPDisk::TReqId::Test4, 0), {});
    }
    VERBOSE_COUT("Done");
    return;
}

void FillDeviceWithZeroes(TTestContext *tc, ui64 chunkSize) {
    FillDeviceWithPattern(tc, chunkSize, 0);
}

void WriteSectors(TTestContext *tc, NPDisk::TAlignedData &dataAfter, ui64 firstSector, ui32 count) {
    VERBOSE_COUT("WriteSectors: restoring " << count << " sectors.");
    TString path = EnsurePDiskExists(tc);

    ui32 sectorSize = 4096;
    ui64 offset = firstSector * sectorSize;
    {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;
        THolder<TPDiskMon> mon(new TPDiskMon(counters, 0, nullptr));
        TActorSystemCreator creator;
        THolder<NPDisk::IBlockDevice> device(NPDisk::CreateRealBlockDeviceWithDefaults(path, *mon,
                    NPDisk::TDeviceMode::LockFile, tc->SectorMap, creator.GetActorSystem()));
        VERBOSE_COUT("Offset = " << offset << " sectorIdx = " << offset/sectorSize);
        device->PwriteSync(dataAfter.Get() + offset, sectorSize * count, offset, NPDisk::TReqId(NPDisk::TReqId::Test4, 0), {});
    }
    VERBOSE_COUT("Done");
    return;
}

void DestroySectors(TTestContext *tc, const NPDisk::TAlignedData &dataAfter,
        ui32 dataSize, ui64 firstSector, ui32 period) {
    VERBOSE_COUT("DestroySectors: destroying " << firstSector << " + k * " << period << " sectors.");
    TString path = EnsurePDiskExists(tc);

    ui32 sectorSize = 4096;
    {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;
        THolder<TPDiskMon> mon(new TPDiskMon(counters, 0, nullptr));
        NPDisk::TAlignedData buffer((dataSize + sectorSize - 1)/ sectorSize * sectorSize);
        memcpy(buffer.Get(), dataAfter.Get(), dataSize);
        for (ui64 i = firstSector; i < dataSize / sectorSize; i += period) {
            memset(buffer.Get() + i * sectorSize, 0xf, sectorSize);
        }
        TActorSystemCreator creator;
        THolder<NPDisk::IBlockDevice> device(NPDisk::CreateRealBlockDeviceWithDefaults(path, *mon,
                    NPDisk::TDeviceMode::LockFile, tc->SectorMap, creator.GetActorSystem()));
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(buffer.Get(), buffer.Size());
        device->PwriteSync(buffer.Get(), buffer.Size(), 0, NPDisk::TReqId(NPDisk::TReqId::Test4, 0), {});
    }
    VERBOSE_COUT("Done");
}

void OutputSectorMap(NPDisk::TAlignedData &dataBefore, NPDisk::TAlignedData &dataAfter, ui32 dataSize) {
    if (!IsVerbose) {
        return;
    }
    ui8 *before = dataBefore.Get();
    ui8 *after = dataAfter.Get();
    ui32 sectorSize = 4096;
    bool isSectorOk = true;
    Cerr << "Begin SectorMap" << Endl;
    for (i64 i = 0; i < dataSize; ++i) {
        if (before[i] != after[i]) {
            isSectorOk = false;
        }
        ui32 thisIdx = i / sectorSize;
        ui32 nextIdx = (i + 1) / sectorSize;
        if (thisIdx != nextIdx) {
            Cerr << (isSectorOk ? "." : "X");
            isSectorOk = true;
        }
    }
    Cerr << Endl << "End SectorMap" << Endl;
}

}
