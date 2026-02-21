#pragma once
#include "defs.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/base/vdisk_priorities.h>
#include <ydb/core/control/lib/immediate_control_board_wrapper.h>
#include <ydb/core/protos/blobstorage_base.pb.h>
#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/core/protos/blobstorage_disk.pb.h>
#include <ydb/core/protos/blobstorage_pdisk_config.pb.h>
#include <ydb/core/protos/blobstorage_disk_color.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>

#include <ydb/library/pdisk_io/drivedata.h>
#include <ydb/library/pdisk_io/file_params.h>
#include <ydb/library/pdisk_io/sector_map.h>
#include <ydb/library/pdisk_io/wcache.h>

namespace NKikimr {

struct TPDiskSchedulerConfig {
    ui64 BytesSchedulerWeight = BytesSchedulerWeightDefault;
    ui64 LogWeight = LogWeightDefault;
    ui64 FreshWeight = FreshWeightDefault;
    ui64 CompWeight = CompWeightDefault;
    ui64 SyncLogWeight = SyncLogWeightDefault;
    ui64 HugeWeight = HugeWeightDefault;
    ui64 FastReadWeight = FastReadWeightDefault;
    ui64 OtherReadWeight = OtherReadWeightDefault;
    ui64 LoadWeight = LoadWeightDefault;
    ui64 LowReadWeight = LowWeightDefault;

    size_t MaxChunkReadsPerCycle = 16;
    double MaxChunkReadsDurationPerCycleMs = 0.25;
    size_t MaxChunkWritesPerCycle = 8;
    double MaxChunkWritesDurationPerCycleMs = 1;

    TString ToString(bool isMultiline) const {
        const char *x = isMultiline ? "\n " : "";
        TStringStream str;
        str << "{TPDiskSchedulerConfig" << x;
        str << " BytesSchedulerWeight# " << BytesSchedulerWeight << x;
        str << " LogWeight# " << LogWeight << x;
        str << " FreshWeight# " << FreshWeight << x;
        str << " CompWeight# " << CompWeight << x;
        str << " SyncLogWeight# " << SyncLogWeight << x;
        str << " HugeWeight# " << HugeWeight << x;
        str << " FastReadWeight# " << FastReadWeight << x;
        str << " OtherReadWeight# " << OtherReadWeight << x;
        str << " LoadWeight# " << LoadWeight << x;
        str << " LowReadWeight# " << LowReadWeight << x;
        str << " MaxChunkReadsPerCycle# " << MaxChunkReadsPerCycle << x;
        str << " MaxChunkReadsDurationPerCycleMs# " << MaxChunkReadsDurationPerCycleMs << x;
        str << " MaxChunkWritesPerCycle# " << MaxChunkWritesPerCycle << x;
        str << " MaxChunkWritesDurationPerCycleMs# " << MaxChunkWritesDurationPerCycleMs << x;
        str << "}";
        return str.Str();
    }

    void Apply(const NKikimrBlobStorage::TPDiskConfig *cfg) {
        if (cfg->HasBytesSchedulerWeight()) {
            BytesSchedulerWeight = cfg->GetBytesSchedulerWeight();
        }
        if (cfg->HasLogWeight()) {
            LogWeight = cfg->GetLogWeight();
        }
        if (cfg->HasFreshWeight()) {
            FreshWeight = cfg->GetFreshWeight();
        }
        if (cfg->HasCompWeight()) {
            CompWeight = cfg->GetCompWeight();
        }
        if (cfg->HasSyncLogWeight()) {
            SyncLogWeight = cfg->GetSyncLogWeight();
        }
        if (cfg->HasHugeWeight()) {
            HugeWeight = cfg->GetHugeWeight();
        }
        if (cfg->HasFastReadWeight()) {
            FastReadWeight = cfg->GetFastReadWeight();
        }
        if (cfg->HasOtherReadWeight()) {
            OtherReadWeight = cfg->GetOtherReadWeight();
        }
        if (cfg->HasLoadWeight()) {
            LoadWeight = cfg->GetLoadWeight();
        }
        if (cfg->HasLowReadWeight()) {
            LowReadWeight = cfg->GetLowReadWeight();
        }
    }
};

struct TPDiskConfig : public TThrRefBase {
    TString Path;     // set only by constructor
    TString ExpectedPath;
    TString ExpectedSerial;
    NKikimrBlobStorage::TSerialManagementStage::E SerialManagementStage
            = NKikimrBlobStorage::TSerialManagementStage::DISCOVER_SERIAL;

    ui64 PDiskGuid;  // set only by constructor
    ui32 PDiskId;    // set only by constructor
    TPDiskCategory PDiskCategory;  // set only by constructor
    TStackVec<TString, 2> HashedMainKey;

    ui64 StartOwnerRound = 1ull;  // set only by warden
    TIntrusivePtr<NPDisk::TSectorMap> SectorMap; // set only by warden

    // EnablePDiskDataEncryption feature flag is not the same as the DISABLE_PDISK_ENCRYPTION macro:
    // unlike the macro, it does not disable metadata encryption.
    // Tests need a runtime way to emulate DISABLE_PDISK_ENCRYPTION.
    bool EnableFormatAndMetadataEncryption = true;

    ui32 ChunkSize = 128 << 20;
    ui32 SectorSize = 4 << 10;

    ui64 StatisticsUpdateIntervalMs = 1000;

    TPDiskSchedulerConfig SchedulerCfg;

    ui64 SortFreeChunksPerItems = 100;

    NKikimrBlobStorage::TPDiskConfig::ESwitch GetDriveDataSwitch = NKikimrBlobStorage::TPDiskConfig::Enable;
    NKikimrBlobStorage::TPDiskConfig::ESwitch WriteCacheSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;

    ui64 DriveModelSeekTimeNs;
    ui64 DriveModelSpeedBps;
    ui64 DriveModelSpeedBpsMin;
    ui64 DriveModelSpeedBpsMax;
    ui64 DriveModelBulkWrieBlockSize;
    ui64 DriveModelTrimSpeedBps;

    ui64 ReorderingMs;
    ui64 DeviceInFlight;
    ui64 CostLimitNs;

    // AsyncBlockDevice settings
    ui32 BufferPoolBufferSizeBytes;
    ui32 BufferPoolBufferCount;
    ui32 MaxQueuedCompletionActions;
    bool UseSpdkNvmeDriver;

    // Next 2 are either user-defined or inferred from drive size
    ui32 ExpectedSlotCount = 0;
    ui32 SlotSizeInUnits = 0;

    // Free chunk permille that triggers Cyan color (e.g. 100 is 10%). Between 130 (default) and 13.
    ui32 ChunkBaseLimit = 130;

    NKikimrConfig::TFeatureFlags FeatureFlags;

    i64 MaxCommonLogChunks = 200ll;
    i64 CommonStaticLogChunks = 70ll;
    ui64 MinLogChunksTotal = 4ull; // for tiny disks

    // Common multiplier and divisor
    // CommonK = MaxLogChunksPerOwnerMultiplier / MaxLogChunksPerOwnerDivisor
    ui64 MaxLogChunksPerOwnerMultiplier = 5ull;
    ui64 MaxLogChunksPerOwnerDivisor = 4ull;

    // Threshold multipliers
    // For N owners ReserveThreshold = N * CommonK * ReserveLogChunksMultiplier, etc.
    ui64 ReserveLogChunksMultiplier = 56;
    ui64 InsaneLogChunksMultiplier = 40;
    ui64 RedLogChunksMultiplier = 30;
    ui64 OrangeLogChunksMultiplier = 20;
    ui64 WarningLogChunksMultiplier = 4;
    ui64 YellowLogChunksMultiplier = 4;

    ui32 MaxMetadataMegabytes = 32; // maximum size of raw metadata (in megabytes)

    NKikimrBlobStorage::TPDiskSpaceColor::E SpaceColorBorder = NKikimrBlobStorage::TPDiskSpaceColor::GREEN;

    ui32 CompletionThreadsCount = 1;
    bool UseNoopScheduler = false;
    bool UseBytesFlightControl = false;

    bool PlainDataChunks = false;

    bool SeparateHugePriorities = false;

    bool MetadataOnly = false;

    bool ReadOnly = false;

    // used for tests only
    std::optional<ui64> NonceRandNum;

    TPDiskConfig(ui64 pDiskGuid, ui32 pdiskId, ui64 pDiskCategory)
        : TPDiskConfig({}, pDiskGuid, pdiskId, pDiskCategory)
    {}

    TPDiskConfig(TString path, ui64 pDiskGuid, ui32 pdiskId, ui64 pDiskCategory)
        : Path(path)
        , PDiskGuid(pDiskGuid)
        , PDiskId(pdiskId)
        , PDiskCategory(pDiskCategory)
    {
        Initialize();
    }

    NPDisk::EDeviceType RetrieveDeviceType() {
        TStringStream outDetails;

        if (std::optional<NPDisk::TDriveData> data = NPDisk::GetDriveData(Path, &outDetails)) {
            return data->DeviceType;
        } else if (Path.Contains("nvme") || Path.Contains("NVME")) {
            return NPDisk::DEVICE_TYPE_NVME;
        } else if (Path.Contains("ssd") || Path.Contains("SSD")) {
            return NPDisk::DEVICE_TYPE_SSD;
        } else {
            return PDiskCategory.Type();
        }
    }

    void Initialize() {
        NPDisk::EDeviceType deviceType = RetrieveDeviceType();

        auto choose = [&] (ui64 nvme, ui64 ssd, ui64 hdd) {
            if (deviceType == NPDisk::DEVICE_TYPE_ROT) {
                return hdd;
            } else if (deviceType == NPDisk::DEVICE_TYPE_SSD) {
                return ssd;
            } else if (deviceType == NPDisk::DEVICE_TYPE_NVME) {
                return nvme;
            } else {
                return hdd;
            }
        };

        DriveModelSeekTimeNs = choose(40'000ull, 40'000ull, 8'000'000ull);
        DriveModelSpeedBps = choose(900'000'000ull, 375'000'000ull, 127'000'000ull);
        DriveModelSpeedBpsMin = choose(900'000'000ull, 375'000'000ull, 135'000'000ull);
        DriveModelSpeedBpsMax = choose(900'000'000ull, 375'000'000ull, 200'000'000ull);
        DriveModelBulkWrieBlockSize = choose(64'000, 1 << 20, 2 << 20);
        DriveModelTrimSpeedBps = choose(6ull << 30, 6ull << 30, 0);
        ReorderingMs = choose(1, 7, 50);
        const ui64 hddInFlight = FeatureFlags.GetEnablePDiskHighHDDInFlight() ? 32 : 4;
        DeviceInFlight = choose(128, 4, hddInFlight);
        CostLimitNs = choose(500'000ull, 20'000'000ull, 50'000'000ull);

        BufferPoolBufferSizeBytes = choose(128 << 10, 256 << 10, 512 << 10);
        BufferPoolBufferCount = choose(1024, 512, 256);
        MaxQueuedCompletionActions = BufferPoolBufferCount / 2;

        UseSpdkNvmeDriver = Path.StartsWith("PCIe:");
        Y_VERIFY_S(!UseSpdkNvmeDriver || deviceType == NPDisk::DEVICE_TYPE_NVME,
                "PDiskId# " << PDiskId << " SPDK NVMe driver can be used only with NVMe devices!");
    }

    TString GetDevicePath() {
        if (ExpectedSerial && !Path && !ExpectedPath) {
            if (std::optional<NPDisk::TDriveData> dev = FindDeviceBySerialNumber(ExpectedSerial, true)) {
                ExpectedPath = dev->Path;
            }
        }

        if (ExpectedPath) {
            return ExpectedPath;
        } else {
            return Path;
        }
    }

    bool CheckSerial(const TString& deviceSerial) const {
        switch (SerialManagementStage) {
        case NKikimrBlobStorage::TSerialManagementStage::CHECK_SERIAL:
            if (ExpectedSerial && ExpectedSerial != deviceSerial) {
                return false;
            }
            break;
        case NKikimrBlobStorage::TSerialManagementStage::ONLY_SERIAL:
            if (ExpectedSerial != deviceSerial) {
                return false;
            }
            break;
        default:
            break;
        }
        return true;
    }

    TString ToString() const {
        return ToString(false);
    }

    TString ToString(bool isMultiline) const {
        TStringStream str;
        const char *x = isMultiline ? "\n" : "";
        str << "{TPDiskConfig" << x;
        str << " Path# \"" << Path << "\"" << x;
        str << " ExpectedPath# \"" << ExpectedPath << "\"" << x;
        str << " ExpectedSerial# \"" << ExpectedSerial << "\"" << x;
        str << " PDiskGuid# " << PDiskGuid << x;
        str << " PDiskId# " << PDiskId << x;
        str << " PDiskCategory# " << PDiskCategory.ToString() << x;
        str << " MetadataOnly# " << MetadataOnly << x;
        for (ui32 i = 0; i < HashedMainKey.size(); ++i) {
            str << " HashedMainKey[" << i << "]# " << HashedMainKey[i] << x;
        }
        str << " StartOwnerRound# " << StartOwnerRound << x;
        str << " SectorMap# " << (SectorMap ? "true" : "false") << x;
        str << " EnableSectorEncryption # " << FeatureFlags.GetEnablePDiskDataEncryption() << x;

        str << " ChunkSize# " << ChunkSize << x;
        str << " SectorSize# " << SectorSize << x;

        str << " StatisticsUpdateIntervalMs# " << StatisticsUpdateIntervalMs << x;

        str << " SchedulerCfg# " << SchedulerCfg.ToString(isMultiline) << x;

        str << " MinLogChunksTotal# " << MinLogChunksTotal << x;
        str << " MaxLogChunksPerOwnerMultiplier# " << MaxLogChunksPerOwnerMultiplier << x;
        str << " MaxLogChunksPerOwnerDivisor# " << MaxLogChunksPerOwnerDivisor << x;
        str << " SortFreeChunksPerItems# " << SortFreeChunksPerItems << x;
        str << " GetDriveDataSwitch# " << NKikimrBlobStorage::TPDiskConfig::ESwitch_Name(GetDriveDataSwitch) << x;
        str << " WriteCacheSwitch# " << NKikimrBlobStorage::TPDiskConfig::ESwitch_Name(WriteCacheSwitch) << x;

        str << " DriveModelSeekTimeNs# " << DriveModelSeekTimeNs << x;
        str << " DriveModelSpeedBps# " << DriveModelSpeedBps << x;
        str << " DriveModelSpeedBpsMin# " << DriveModelSpeedBpsMin << x;
        str << " DriveModelSpeedBpsMax# " << DriveModelSpeedBpsMax << x;
        str << " DriveModelBulkWrieBlockSize# " << DriveModelBulkWrieBlockSize << x;
        str << " DriveModelTrimSpeedBps# " << DriveModelTrimSpeedBps << x;
        str << " ReorderingMs# " << ReorderingMs << x;
        str << " DeviceInFlight# " << DeviceInFlight << x;
        str << " CostLimitNs# " << CostLimitNs << x;
        str << " BufferPoolBufferSizeBytes# " << BufferPoolBufferSizeBytes << x;
        str << " BufferPoolBufferCount# " << BufferPoolBufferCount << x;
        str << " MaxQueuedCompletionActions# " << MaxQueuedCompletionActions << x;
        str << " ExpectedSlotCount# " << ExpectedSlotCount << x;
        str << " SlotSizeInUnits# " << SlotSizeInUnits << x;

        str << " ReserveLogChunksMultiplier# " << ReserveLogChunksMultiplier << x;
        str << " InsaneLogChunksMultiplier# " << InsaneLogChunksMultiplier << x;
        str << " RedLogChunksMultiplier# " << RedLogChunksMultiplier << x;
        str << " OrangeLogChunksMultiplier# " << OrangeLogChunksMultiplier << x;
        str << " WarningLogChunksMultiplier# " << WarningLogChunksMultiplier << x;
        str << " YellowLogChunksMultiplier# " << YellowLogChunksMultiplier << x;
        str << " MaxMetadataMegabytes# " << MaxMetadataMegabytes << x;
        str << " SpaceColorBorder# " << SpaceColorBorder << x;
        str << " CompletionThreadsCount# " << CompletionThreadsCount << x;
        str << " UseNoopScheduler# " << (UseNoopScheduler ? "true" : "false") << x;
        str << " UseBytesFlightControl# " << (UseBytesFlightControl ? "true" : "false") << x;
        str << " PlainDataChunks# " << PlainDataChunks << x;
        str << " SeparateHugePriorities# " << SeparateHugePriorities << x;
        str << "}";
        return str.Str();
    }

    void Apply(const NKikimrBlobStorage::TPDiskConfig *cfg) {
        if (!cfg) {
            return;
        }

        if (cfg->HasChunkSize()) {
            ChunkSize = cfg->GetChunkSize();
        }
        if (cfg->HasSectorSize()) {
            SectorSize = cfg->GetSectorSize();
        }
        if (cfg->HasStatisticsUpdateIntervalMs()) {
            StatisticsUpdateIntervalMs = cfg->GetStatisticsUpdateIntervalMs();
        }

        SchedulerCfg.Apply(cfg);

        if (cfg->HasMinLogChunksTotal()) {
            MinLogChunksTotal = cfg->GetMinLogChunksTotal();
        }
        if (cfg->HasMaxLogChunksPerOwnerMultiplier()) {
            MaxLogChunksPerOwnerMultiplier = cfg->GetMaxLogChunksPerOwnerMultiplier();
        }
        if (cfg->HasMaxLogChunksPerOwnerDivisor()) {
            MaxLogChunksPerOwnerDivisor = cfg->GetMaxLogChunksPerOwnerDivisor();
        }
        if (cfg->HasSortFreeChunksPerItems()) {
            SortFreeChunksPerItems = cfg->GetSortFreeChunksPerItems();
        }
        if (cfg->HasGetDriveDataSwitch()) {
            GetDriveDataSwitch = cfg->GetGetDriveDataSwitch();
        }
        if (cfg->HasWriteCacheSwitch()) {
            WriteCacheSwitch = cfg->GetWriteCacheSwitch();
        }

        if (cfg->HasDriveModelSeekTimeNs()) {
            DriveModelSeekTimeNs = cfg->GetDriveModelSeekTimeNs();
        }
        if (cfg->HasDriveModelSpeedBps()) {
            DriveModelSpeedBps = cfg->GetDriveModelSpeedBps();
        }
        if (cfg->HasDriveModelBulkWrieBlockSize()) {
            DriveModelBulkWrieBlockSize = cfg->GetDriveModelBulkWrieBlockSize();
        }
        if (cfg->HasDriveModelTrimSpeedBps()) {
            DriveModelTrimSpeedBps = cfg->GetDriveModelTrimSpeedBps();
        }
        if (cfg->HasReorderingMs()) {
            ReorderingMs = cfg->GetReorderingMs();
        }
        if (cfg->HasDeviceInFlight()) {
            DeviceInFlight = cfg->GetDeviceInFlight();
        }
        if (cfg->HasCostLimitNs()) {
            CostLimitNs = cfg->GetCostLimitNs();
        }
        if (cfg->HasBufferPoolBufferSizeBytes()) {
            BufferPoolBufferSizeBytes = cfg->GetBufferPoolBufferSizeBytes();
        }
        if (cfg->HasBufferPoolBufferCount()) {
            BufferPoolBufferCount = cfg->GetBufferPoolBufferCount();
        }
        if (cfg->HasMaxQueuedCompletionActions()) {
            MaxQueuedCompletionActions = cfg->GetMaxQueuedCompletionActions();
        }
        if (cfg->HasInsaneLogChunksMultiplier()) {
            InsaneLogChunksMultiplier = cfg->GetInsaneLogChunksMultiplier();
        }

        if (cfg->HasExpectedSlotCount()) {
            ExpectedSlotCount = cfg->GetExpectedSlotCount();
        }

        if (cfg->HasChunkBaseLimit()) {
            ui32 limit = cfg->GetChunkBaseLimit();
            limit = Min<ui32>(130, limit);
            limit = Max<ui32>(13, limit);
            ChunkBaseLimit = limit;
        }

        if (cfg->HasCompletionThreadsCount()) {
            CompletionThreadsCount = cfg->GetCompletionThreadsCount();
        }

        if (cfg->HasUseNoopScheduler()) {
            UseNoopScheduler = cfg->GetUseNoopScheduler();
        }
        if (cfg->HasUseBytesFlightControl()) {
            UseBytesFlightControl = cfg->GetUseBytesFlightControl();
        }
        if (cfg->HasPlainDataChunks()) {
            PlainDataChunks = cfg->GetPlainDataChunks();
        }

        if (cfg->HasSlotSizeInUnits()) {
            SlotSizeInUnits = cfg->GetSlotSizeInUnits();
        }

        if (cfg->HasSeparateHugePriorities()) {
            SeparateHugePriorities = cfg->GetSeparateHugePriorities();
        }
    }

    ui32 GetOwnerWeight(ui32 groupSizeInUnits) {
        return TPDiskConfig::GetOwnerWeight(groupSizeInUnits, SlotSizeInUnits);
    }

    static ui32 GetOwnerWeight(ui32 groupSizeInUnits, ui32 slotSizeInUnits) {
        ui32 vu = groupSizeInUnits ? groupSizeInUnits : 1;
        ui32 pu = slotSizeInUnits ? slotSizeInUnits : 1;
        return int(vu / pu) + !!(vu % pu);
    }
};

struct TInferPDiskSlotCountSettingsForDriveType {
    ui64 UnitSize = 0;
    ui32 MaxSlots = 0;
    bool PreferInferredSettingsOverExplicit = false;

    TInferPDiskSlotCountSettingsForDriveType(const NKikimrBlobStorage::TInferPDiskSlotCountSettings& settings, NPDisk::EDeviceType type) {
        switch (type) {
            case NPDisk::DEVICE_TYPE_ROT:
                UnitSize = settings.GetRot().GetUnitSize();
                MaxSlots = settings.GetRot().GetMaxSlots();
                PreferInferredSettingsOverExplicit = settings.GetRot().GetPreferInferredSettingsOverExplicit();
                break;
            case NPDisk::DEVICE_TYPE_SSD:
            case NPDisk::DEVICE_TYPE_NVME:
                UnitSize = settings.GetSsd().GetUnitSize();
                MaxSlots = settings.GetSsd().GetMaxSlots();
                PreferInferredSettingsOverExplicit = settings.GetSsd().GetPreferInferredSettingsOverExplicit();
                break;
            case NPDisk::DEVICE_TYPE_UNKNOWN:
                break;
        }
    }

    explicit operator bool() const {
        return UnitSize && MaxSlots;
    }
};

} // NKikimr
