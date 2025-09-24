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
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/crypto/default.h>

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
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>
#include <future>
#include <library/cpp/openssl/crypto/sha.h>

namespace NKikimr {
namespace NPDisk {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

} // NPDisk

namespace {
constexpr ui32 kNodeIdForMetadata = 1;
constexpr ui32 kPDiskIdForMetadata = 1;
constexpr ui32 kMetaTimeoutSeconds = 10;

static NPDisk::TMainKey GetEffectiveMainKey(const NPDisk::TMainKey& mainKey) {
    NPDisk::TMainKey result = mainKey;
    if (!result.IsInitialized || result.Keys.empty()) {
        result.Initialize();
    }
    return result;
}

static TIntrusivePtr<TPDiskConfig> MakeMetadataPDiskConfig(const TString& path, ui32 pdiskId, bool readOnly, TStringStream& details) {
    std::optional<NPDisk::TDriveData> driveData = NPDisk::GetDriveData(path, &details);
    const NPDisk::EDeviceType deviceType = driveData ? driveData->DeviceType : NPDisk::DEVICE_TYPE_ROT;
    const ui64 category = static_cast<ui64>(TPDiskCategory(deviceType, 0).GetRaw());

    auto cfg = MakeIntrusive<TPDiskConfig>(path, static_cast<ui64>(0), static_cast<ui32>(pdiskId), category);
    cfg->ReadOnly = readOnly;
    cfg->MetadataOnly = true;
    return cfg;
}

static NActors::TActorId RegisterPDiskActor(NActors::TActorSystem* sys,
                                           const TIntrusivePtr<TPDiskConfig>& cfg,
                                           const NPDisk::TMainKey& mainKey,
                                           const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    IActor* pdiskActorImpl = CreatePDisk(cfg, mainKey, counters);
    const NActors::TActorId pdiskActor = sys->Register(pdiskActorImpl);
    const NActors::TActorId pdiskActorId = MakeBlobStoragePDiskID(/*nodeId*/kNodeIdForMetadata, /*pdiskId*/kPDiskIdForMetadata);
    sys->RegisterLocalService(pdiskActorId, pdiskActor);
    return pdiskActorId;
}

template <typename TFut>
static inline void WaitOrThrow(TFut& fut, int timeoutSeconds, const TString& details, const char* action) {
    if (fut.wait_for(std::chrono::seconds(timeoutSeconds)) != std::future_status::ready) {
        ythrow yexception()
            << "PDisk metadata " << action << " timeout after " << timeoutSeconds << "s\n"
            << "Details: " << details;
    }
}

}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PDisk Formatting
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void FormatPDisk(TString path, ui64 diskSizeBytes, ui32 sectorSizeBytes, ui32 userAccessibleChunkSizeBytes,
    const ui64 &diskGuid, const NPDisk::TKey &chunkKey, const NPDisk::TKey &logKey, const NPDisk::TKey &sysLogKey,
    const NPDisk::TKey &mainKey, TString textMessage, const bool isErasureEncodeUserLog, bool trimEntireDevice,
    TIntrusivePtr<NPDisk::TSectorMap> sectorMap, bool enableSmallDiskOptimization, std::optional<TRcBuf> metadata,
    bool plainDataChunks)
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
    cfg->PlainDataChunks = plainDataChunks;

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

    auto pCtx = std::make_shared<NPDisk::TPDiskCtx>(creator.GetActorSystem());
    THolder<NPDisk::TPDisk> pDisk(new NPDisk::TPDisk(pCtx, cfg, counters));

    pDisk->Initialize();

    if (!pDisk->BlockDevice->IsGood()) {
        ythrow yexception() << "Device with path# " << path << " is not good, info# " << pDisk->BlockDevice->DebugInfo();
    }
    pDisk->WriteDiskFormat(diskSizeBytes, sectorSizeBytes, userAccessibleChunkSizeBytes, diskGuid,
        chunkKey, logKey, sysLogKey, mainKey, textMessage, isErasureEncodeUserLog, trimEntireDevice,
        std::move(metadata), cfg->PlainDataChunks);
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
    Y_VERIFY(formatRaw->Size() >= formatSectorsSize);

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
                Y_VERIFY(buffer->Size() >= sysLogPartSize);
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
    TFile f(path, OpenExisting | RdWr);
    f.Flock(LOCK_EX | LOCK_NB);

    bool isBlockDevice = false;
    ui64 diskSizeBytes = 0;
    DetectFileParameters(path, diskSizeBytes, isBlockDevice);

    constexpr size_t portionSize = NPDisk::FormatSectorSize * NPDisk::ReplicationFactor;
    if (diskSizeBytes <= portionSize) {
        ythrow yexception() << "file is too small to be the YDB storage device, path# " << path.Quote() <<
            " diskSizeBytes# " << diskSizeBytes;
    }

    TVector<ui8> zeros(portionSize, 0);
    f.Pwrite(zeros.data(), zeros.size(), 0);
    f.Flush();
}

NKikimrBlobStorage::TPDiskMetadataRecord ReadPDiskMetadata(const TString& path, const NPDisk::TMainKey& mainKey) {
    TActorSystemCreator creator;
    auto* sys = creator.GetActorSystem();
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();

    TStringStream details;

    auto pdiskCfg = MakeMetadataPDiskConfig(path, kPDiskIdForMetadata, /*readOnly*/true, details);

    const NPDisk::TMainKey effectiveMainKey = GetEffectiveMainKey(mainKey);

    const NActors::TActorId pdiskActorId = RegisterPDiskActor(sys, pdiskCfg, effectiveMainKey, counters);

    NKikimrBlobStorage::TPDiskMetadataRecord out;

    class TMetadataReader : public NActors::TActorBootstrapped<TMetadataReader> {
        const NActors::TActorId PDiskActorId;
        NKikimrBlobStorage::TPDiskMetadataRecord& Out;
        std::promise<bool>& Done;
        TString& Error;

        static const char* OutcomeToStr(NPDisk::EPDiskMetadataOutcome oc) {
            switch (oc) {
                case NPDisk::EPDiskMetadataOutcome::OK: return "OK";
                case NPDisk::EPDiskMetadataOutcome::NO_METADATA: return "NO_METADATA";
                case NPDisk::EPDiskMetadataOutcome::ERROR: return "ERROR";
            }
            return "UNKNOWN";
        }
    public:
        TMetadataReader(NActors::TActorId pdiskActorId,
                        NKikimrBlobStorage::TPDiskMetadataRecord& out,
                        std::promise<bool>& done,
                        TString& error)
            : PDiskActorId(pdiskActorId)
            , Out(out)
            , Done(done)
            , Error(error)
        {}

        void Bootstrap() {
            Send(PDiskActorId, new NPDisk::TEvReadMetadata());
            Become(&TThis::StateFunc, TDuration::Seconds(kMetaTimeoutSeconds), new TEvents::TEvWakeup);
        }

        void Handle(NPDisk::TEvReadMetadataResult::TPtr ev) {
            auto* msg = ev->Get();
            bool ok = false;
            switch (msg->Outcome) {
                case NPDisk::EPDiskMetadataOutcome::OK: {
                    TRope rope(std::move(msg->Metadata));
                    TRopeStream stream(rope.begin(), rope.size());
                    ok = Out.ParseFromZeroCopyStream(&stream);
                    if (!ok) {
                        Error = "PARSE_FAILED";
                    }
                    break;
                }
                case NPDisk::EPDiskMetadataOutcome::NO_METADATA: {
                    Error = OutcomeToStr(msg->Outcome);
                    ok = false;
                    break;
                }
                case NPDisk::EPDiskMetadataOutcome::ERROR: {
                    Error = OutcomeToStr(msg->Outcome);
                    ok = false;
                    break;
                }
            }
            Done.set_value(ok);
            PassAway();
        }

        void HandleWakeup() {
            Error = "TIMEOUT";
            Done.set_value(false);
            PassAway();
        }

        STRICT_STFUNC(StateFunc,
            hFunc(NPDisk::TEvReadMetadataResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        )
    };

    std::promise<bool> done;
    auto fut = done.get_future();
    TString error;
    sys->Register(new TMetadataReader(pdiskActorId, out, done, error));

    WaitOrThrow(fut, 10, details.Str(), "read");
    bool ok = fut.get();
    if (!ok) {
        ythrow yexception()
            << "PDisk metadata read failed: " << (!error.empty() ? error : TString("ERROR")) << "\n"
            << "Details: " << details.Str();
    }
    return out;
}

static void UpdateStorageConfigFingerprint(NKikimrBlobStorage::TStorageConfig* config) {
    if (!config) {
        return;
    }
    config->ClearFingerprint();
    TString s;
    const bool success = config->SerializeToString(&s);
    Y_ABORT_UNLESS(success);
    auto digest = NOpenSsl::NSha1::Calc(s.data(), s.size());
    config->SetFingerprint(digest.data(), digest.size());
}

void WritePDiskMetadata(const TString& path, const NKikimrBlobStorage::TPDiskMetadataRecord& record, const NPDisk::TMainKey& mainKey) {
    NKikimrBlobStorage::TPDiskMetadataRecord adjustedRecord(record);

    const NPDisk::TMainKey effectiveMainKey = GetEffectiveMainKey(mainKey);

    NKikimrBlobStorage::TPDiskMetadataRecord previous;
    bool havePrevious = false;
    try {
        previous = ReadPDiskMetadata(path, effectiveMainKey);
        havePrevious = true;
    } catch (...) {
        havePrevious = false;
    }

    if (adjustedRecord.HasCommittedStorageConfig()) {
        auto* cfg = adjustedRecord.MutableCommittedStorageConfig();
        if (!cfg->HasPrevConfig() && havePrevious) {
            if (previous.HasCommittedStorageConfig()) {
                cfg->MutablePrevConfig()->CopyFrom(previous.GetCommittedStorageConfig());
            } else if (previous.HasProposedStorageConfig()) {
                cfg->MutablePrevConfig()->CopyFrom(previous.GetProposedStorageConfig());
            }
        }
        UpdateStorageConfigFingerprint(cfg);
    }
    if (adjustedRecord.HasProposedStorageConfig()) {
        auto* cfg = adjustedRecord.MutableProposedStorageConfig();
        if (!cfg->HasPrevConfig() && havePrevious) {
            if (previous.HasCommittedStorageConfig()) {
                cfg->MutablePrevConfig()->CopyFrom(previous.GetCommittedStorageConfig());
            } else if (previous.HasProposedStorageConfig()) {
                cfg->MutablePrevConfig()->CopyFrom(previous.GetProposedStorageConfig());
            }
        }
        UpdateStorageConfigFingerprint(cfg);
    }
    TActorSystemCreator creator;
    auto* sys = creator.GetActorSystem();
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();

    TStringStream details;

    auto pdiskCfg = MakeMetadataPDiskConfig(path, kPDiskIdForMetadata, /*readOnly*/false, details);

    const NActors::TActorId pdiskActorId = RegisterPDiskActor(sys, pdiskCfg, effectiveMainKey, counters);

    class TWriter : public NActors::TActorBootstrapped<TWriter> {
        const NActors::TActorId PDiskActorId;
        const NKikimrBlobStorage::TPDiskMetadataRecord& Record;
        std::promise<bool>& Done;
    public:
        TWriter(NActors::TActorId pdiskActorId,
                const NKikimrBlobStorage::TPDiskMetadataRecord& record,
                std::promise<bool>& done)
            : PDiskActorId(pdiskActorId)
            , Record(record)
            , Done(done)
        {}

        void Bootstrap() {
            TString data;
            if (!Record.SerializeToString(&data)) {
                Done.set_value(false);
                PassAway();
                return;
            }
            Send(PDiskActorId, new NPDisk::TEvWriteMetadata(TRcBuf(std::move(data))));
            Become(&TThis::StateFunc, TDuration::Seconds(kMetaTimeoutSeconds), new TEvents::TEvWakeup);
        }

        void Handle(NPDisk::TEvWriteMetadataResult::TPtr ev) {
            bool ok = (ev->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK);
            Done.set_value(ok);
            PassAway();
        }

        void HandleWakeup() {
            Done.set_value(false);
            PassAway();
        }

        STRICT_STFUNC(StateFunc,
            hFunc(NPDisk::TEvWriteMetadataResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        )
    };

    std::promise<bool> done;
    auto fut = done.get_future();
    sys->Register(new TWriter(pdiskActorId, adjustedRecord, done));

    WaitOrThrow(fut, 10, details.Str(), "write");
    if (!fut.get()) {
        ythrow yexception()
            << "PDisk metadata write failed\n"
            << "Details: " << details.Str();
    }
}

} // NKikimr
