#include "blobstorage_pdisk_impl.h"

#include "blobstorage_pdisk_blockdevice.h"
#include <ydb/library/pdisk_io/buffers.h>
#include "blobstorage_pdisk_completion_impl.h"
#include "blobstorage_pdisk_crypto.h"
#include "blobstorage_pdisk_data.h"
#include "blobstorage_pdisk_actorsystem_creator.h"
#include "blobstorage_pdisk_mon.h"
#include "blobstorage_pdisk_requestimpl.h"
#include "blobstorage_pdisk_state.h"
#include "blobstorage_pdisk_thread.h"
#include "blobstorage_pdisk_tools.h"
#include "blobstorage_pdisk_util_countedqueueoneone.h"
#include "blobstorage_pdisk_writer.h"

#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/util/queue_oneone_inplace.h>
#include <ydb/library/pdisk_io/aio.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/algorithm.h>
#include <util/generic/queue.h>
#include <util/system/backtrace.h>
#include <util/system/condvar.h>
#include <util/system/filemap.h>
#include <util/system/file.h>
#include <util/thread/lfqueue.h>
#include <util/system/mutex.h>
#include <util/system/sanitizers.h>
#include <ydb/core/protos/base.pb.h>

namespace NKikimr {
namespace NPDisk {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

} // NPDisk

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PDisk Formatting
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void FormatPDisk(TString path, ui64 diskSizeBytes, ui32 sectorSizeBytes, ui32 userAccessibleChunkSizeBytes,
    const ui64 &diskGuid, const NPDisk::TKey &chunkKey, const NPDisk::TKey &logKey, const NPDisk::TKey &sysLogKey,
    const NPDisk::TKey &mainKey, TString textMessage, const bool isErasureEncodeUserLog, bool trimEntireDevice,
    TIntrusivePtr<NPDisk::TSectorMap> sectorMap, bool enableSmallDiskOptimization, std::optional<TRcBuf> metadata)
{
    TActorSystemCreator creator;

    bool isBlockDevice = false;
    NPDisk::EDeviceType deviceType = NPDisk::DEVICE_TYPE_ROT;
    if (sectorMap) {
        if (diskSizeBytes) {
            sectorMap->ForceSize(diskSizeBytes);
        } else {
            if (sectorMap->DeviceSize == 0) {
                ythrow yexception() << "Can't create in-memory fake disk map with 0 size, path# " << path.Quote();
            }
            diskSizeBytes = sectorMap->DeviceSize;
        }
    } else {
        if (path.StartsWith("PCIe:")) {
            deviceType = NPDisk::DEVICE_TYPE_NVME;
        }
        if (diskSizeBytes == 0) {
            creator.GetActorSystem()->AppData<TAppData>()->IoContextFactory
                ->DetectFileParameters(path, diskSizeBytes, isBlockDevice);
        }
    }
    if (enableSmallDiskOptimization && diskSizeBytes > 0 && diskSizeBytes < NPDisk::FullSizeDiskMinimumSize &&
        userAccessibleChunkSizeBytes > NPDisk::SmallDiskMaximumChunkSize) {
        throw NPDisk::TPDiskFormatBigChunkException() << "diskSizeBytes# " << diskSizeBytes <<
            " userAccessibleChunkSizeBytes# " << userAccessibleChunkSizeBytes <<
            " bool(sectorMap)# " << bool(sectorMap) <<
            " sectorMap->DeviceSize# " << (sectorMap ? sectorMap->DeviceSize : 0);
    }
    Y_VERIFY_S((enableSmallDiskOptimization && diskSizeBytes < NPDisk::FullSizeDiskMinimumSize) || (
            diskSizeBytes > 0 && diskSizeBytes / userAccessibleChunkSizeBytes > 200),
            " diskSizeBytes# " << diskSizeBytes <<
            " userAccessibleChunkSizeBytes# " << userAccessibleChunkSizeBytes <<
            " bool(sectorMap)# " << bool(sectorMap) <<
            " sectorMap->DeviceSize# " << (sectorMap ? sectorMap->DeviceSize : 0)
        );

    TIntrusivePtr<TPDiskConfig> cfg(new TPDiskConfig(path, diskGuid, 0xffffffffull,
                TPDiskCategory(deviceType, 0).GetRaw()));
    cfg->SectorMap = sectorMap;
    // Disable encryption for SectorMap
    cfg->EnableSectorEncryption = !cfg->SectorMap;

    if (!isBlockDevice && !cfg->UseSpdkNvmeDriver && !sectorMap) {
        // path is a regular file
        if (diskSizeBytes == 0) {
            ythrow yexception() << "Can't create file with 0 size, path# " << path;
        }
        TFile file(path.c_str(), OpenAlways | RdWr | Seq | Direct);
        file.Flock(LOCK_EX | LOCK_NB);
        file.Resize(diskSizeBytes);
        file.Close();
    }

    const TIntrusivePtr<::NMonitoring::TDynamicCounters> counters(new ::NMonitoring::TDynamicCounters);

    THolder<NPDisk::TPDisk> pDisk(new NPDisk::TPDisk(cfg, counters));

    pDisk->Initialize(creator.GetActorSystem(), TActorId());

    if (!pDisk->BlockDevice->IsGood()) {
        ythrow yexception() << "Device with path# " << path << " is not good, info# " << pDisk->BlockDevice->DebugInfo();
    }
    pDisk->WriteDiskFormat(diskSizeBytes, sectorSizeBytes, userAccessibleChunkSizeBytes, diskGuid,
        chunkKey, logKey, sysLogKey, mainKey, textMessage, isErasureEncodeUserLog, trimEntireDevice,
        std::move(metadata));
}

bool ReadPDiskFormatInfo(const TString &path, const NPDisk::TMainKey &mainKey, TPDiskInfo &outInfo,
        const bool doLock, TIntrusivePtr<NPDisk::TSectorMap> sectorMap) {
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> counters(new ::NMonitoring::TDynamicCounters);
    auto mon = std::make_unique<TPDiskMon>(counters, 0, nullptr);

    bool useSdpkNvmeDriver = path.StartsWith("PCIe:");
    NPDisk::TDeviceMode::TFlags deviceFlags = 0;
    if (useSdpkNvmeDriver) {
        deviceFlags |= NPDisk::TDeviceMode::UseSpdk;
    }
    if (doLock) {
        deviceFlags |= NPDisk::TDeviceMode::LockFile;
    }

    TActorSystemCreator creator;
    THolder<NPDisk::IBlockDevice> blockDevice(
        NPDisk::CreateRealBlockDeviceWithDefaults(path, *mon, deviceFlags, sectorMap, creator.GetActorSystem()));
    if (!blockDevice->IsGood()) {
        TStringStream str;
        str << "Can't lock file, make sure you have access rights, file exists and is not locked by another process.";
        str << " Path# \"" << path << "\"";
        outInfo.ErrorReason = str.Str();
        return false;
    }

    ui32 formatSectorsSize = NPDisk::FormatSectorSize * NPDisk::ReplicationFactor;

    THolder<NPDisk::TBufferPool> bufferPool(NPDisk::CreateBufferPool(512 << 10, 2, useSdpkNvmeDriver, {}));
    NPDisk::TBuffer::TPtr formatRaw(bufferPool->Pop());
    Y_ABORT_UNLESS(formatRaw->Size() >= formatSectorsSize);

    blockDevice->PreadSync(formatRaw->Data(), formatSectorsSize, 0,
            NPDisk::TReqId(NPDisk::TReqId::ReadFormatInfo, 0), {});

    for (auto& key : mainKey.Keys) {
        NPDisk::TPDiskStreamCypher cypher(true); // Format record is always encrypted
        cypher.SetKey(key);
        bool isOk = false;
        alignas(16) NPDisk::TDiskFormat format;
        for (ui32 recordIdx = 0; recordIdx < NPDisk::ReplicationFactor; ++recordIdx) {
            ui64 recordSectorOffset = recordIdx * NPDisk::FormatSectorSize;
            ui8 *formatSector = formatRaw->Data() + recordSectorOffset;
            NPDisk::TDataSectorFooter *footer = (NPDisk::TDataSectorFooter*)
                (formatSector + NPDisk::FormatSectorSize - sizeof(NPDisk::TDataSectorFooter));

            cypher.StartMessage(footer->Nonce);

            alignas(16) NPDisk::TDiskFormatSector formatCandidate;
            cypher.Encrypt(formatCandidate.Raw, formatSector, NPDisk::FormatSectorSize);

            if (formatCandidate.Format.IsHashOk(NPDisk::FormatSectorSize)) {
                format.UpgradeFrom(formatCandidate.Format);
                isOk = true;
            }
        }

        if (isOk) {
            outInfo.Version = format.Version;
            outInfo.DiskSize = format.DiskSize;
            outInfo.SectorSizeBytes = format.SectorSize;
            outInfo.UserAccessibleChunkSizeBytes = format.GetUserAccessibleChunkSize();
            outInfo.DiskGuid = format.Guid;
            format.FormatText[sizeof(format.FormatText) - 1] = 0;
            outInfo.TextMessage = format.FormatText;
            outInfo.RawChunkSizeBytes = format.ChunkSize;
            outInfo.SysLogSectorCount = format.SysLogSectorCount;
            outInfo.SystemChunkCount = format.SystemChunkCount;
            outInfo.Timestamp = TInstant::MicroSeconds(format.TimestampUs);
            outInfo.FormatFlags = format.FormatFlagsToString(format.FormatFlags);

            ui32 sysLogSize = format.SectorSize * format.SysLogSectorCount * 3;
            ui32 formatBytes = NPDisk::FormatSectorSize * NPDisk::ReplicationFactor;
            ui32 sysLogOffsetSectors = (formatBytes + format.SectorSize - 1) / format.SectorSize;
            ui32 sysLogOffset = sysLogOffsetSectors * format.SectorSize;

            NPDisk::TAlignedData sysLogRaw(sysLogSize);
            NPDisk::TBuffer::TPtr buffer(bufferPool->Pop());
            const ui32 bufferSize = AlignDown(bufferPool->GetBufferSize(), format.SectorSize);
            const ui32 sysLogRawParts = (sysLogSize + bufferSize - 1) / bufferSize;
            for (ui32 i = 0; i < sysLogRawParts; i++) {
                const ui32 sysLogPartSize = Min(bufferSize, sysLogSize - i * bufferSize);
                Y_ABORT_UNLESS(buffer->Size() >= sysLogPartSize);
                blockDevice->PreadSync(buffer->Data(), sysLogPartSize, sysLogOffset + i * bufferSize,
                        NPDisk::TReqId(NPDisk::TReqId::ReadSysLogData, 0), {});
                memcpy(sysLogRaw.Get() + i * bufferSize, buffer->Data(), sysLogPartSize);
            }

            outInfo.SectorInfo.clear();
            for (ui32 idx = 0; idx < format.SysLogSectorCount * 3; ++idx) {
                ui64 logSectorOffset = (ui64)idx * (ui64)format.SectorSize;
                ui8 *sector = sysLogRaw.Get() + logSectorOffset;
                NPDisk::TDataSectorFooter *logFooter = (NPDisk::TDataSectorFooter*)
                    (sector + format.SectorSize - sizeof(NPDisk::TDataSectorFooter));

                ui64 sectorOffset = sysLogOffset + (ui64)((idx / 3) * 3) * (ui64)format.SectorSize;
                bool isCrcOk = NPDisk::TPDiskHashCalculator().CheckSectorHash(
                        sectorOffset, format.MagicSysLogChunk, sector, format.SectorSize, logFooter->Hash);
                outInfo.SectorInfo.push_back(TPDiskInfo::TSectorInfo(logFooter->Nonce, logFooter->Version, isCrcOk));
            }

            return true;
        }
    }

    TStringStream str;
    str << "Error parsing format record, make sure you use the correct MainKey. Path# \"" << path << "\"";
    outInfo.ErrorReason = str.Str();
    return false;
}

void ObliterateDisk(TString path) {
    TFile f(path, OpenAlways | RdWr);
    f.Flock(LOCK_EX | LOCK_NB);

    TVector<ui8> zeros(NPDisk::FormatSectorSize * NPDisk::ReplicationFactor, 0);
    f.Pwrite(zeros.data(), zeros.size(), 0);
    f.Flush();
}

} // NKikimr
