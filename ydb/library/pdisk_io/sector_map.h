#pragma once

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/actors/util/ticket_lock.h>

#include <util/datetime/base.h>
#include <util/datetime/cputimer.h>
#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/system/mutex.h>
#include <util/system/types.h>
#include <util/system/hp_timer.h>

#include <contrib/libs/lz4/lz4.h>

#include <array>
#include <atomic>
#include <optional>

namespace NKikimr {
namespace NPDisk {

namespace NSectorMap {

enum EDiskMode {
    DM_NONE = 0,
    DM_HDD,
    DM_SSD,
    DM_NVME,
    DM_COUNT
};

inline EDiskMode DiskModeFromString(const TString& diskMode) {
    if (diskMode == "HDD") {
        return DM_HDD;
    } else if (diskMode == "SSD") {
        return DM_SSD;
    } else if (diskMode == "NVME") {
        return DM_NVME;
    } else if (diskMode == "NONE") {
        return DM_NONE;
    }

    return DM_NONE;
}

inline TString DiskModeToString(EDiskMode diskMode) {
    switch (diskMode) {
    case DM_NONE:
        return "NONE";
    case DM_HDD:
        return "HDD";
    case DM_SSD:
        return "SSD";
    case DM_NVME:
        return "NVME";
    default:
        return "UNKNOWN";
    }
}

static constexpr std::array<std::array<ui64, 3>, NSectorMap::DM_COUNT> DiskModeParamPresets = {
    {
        {0, 0, 0}, // DM_NONE
        {9000, 200ull * 1024 * 1024, 66ull * 1024 * 1024}, // DM_HDD
        {0, 500ull * 1024 * 1024, 500ull * 1024 * 1024}, // DM_SSD
        {0, 1000ull * 1024 * 1024, 1000ull * 1024 * 1024}, // DM_NVME, probably unusable
    }
};

constexpr ui64 SECTOR_SIZE = 4096;

/* TSectorOperationThrottler: thread-safe */

class TSectorOperationThrottler {
public:
    struct TDiskModeParams {
        std::atomic<ui64> SeekSleepMicroSeconds;
        std::atomic<ui64> FirstSectorRate;
        std::atomic<ui64> LastSectorRate;
    };

public:
    TSectorOperationThrottler(ui64 sectors, NSectorMap::EDiskMode diskMode) {
        Init(sectors, diskMode);
    }

    void Init(ui64 sectors, NSectorMap::EDiskMode diskMode) {
        Y_VERIFY(sectors > 0);

        Y_VERIFY((ui32)diskMode < DiskModeParamPresets.size());
        DiskModeParams.SeekSleepMicroSeconds = DiskModeParamPresets[diskMode][0];
        DiskModeParams.FirstSectorRate = DiskModeParamPresets[diskMode][1];
        DiskModeParams.LastSectorRate = DiskModeParamPresets[diskMode][2];

        MaxSector = sectors - 1;
        MostRecentlyUsedSector = 0;
    }

    void ThrottleRead(i64 size, ui64 offset, bool prevOperationIsInProgress, double operationTimeMs) {
        ThrottleOperation(size, offset, prevOperationIsInProgress, operationTimeMs);
    }

    void ThrottleWrite(i64 size, ui64 offset, bool prevOperationIsInProgress, double operationTimeMs) {
        ThrottleOperation(size, offset, prevOperationIsInProgress, operationTimeMs);
    }

    TDiskModeParams* GetDiskModeParams() {
        return &DiskModeParams;
    }

private:
    /* throttle read/write operation */
    void ThrottleOperation(i64 size, ui64 offset, bool prevOperationIsInProgress, double operationTimeMs) {
        if (size == 0) {
            return;
        }
        
        ui64 beginSector = offset / NSectorMap::SECTOR_SIZE;
        ui64 endSector = (offset + size + NSectorMap::SECTOR_SIZE - 1) / NSectorMap::SECTOR_SIZE;
        ui64 midSector = (beginSector + endSector) / 2;

        {
            TGuard<TMutex> guard(Mutex);
            if (beginSector != MostRecentlyUsedSector + 1 || !prevOperationIsInProgress) {
                Sleep(TDuration::MicroSeconds(DiskModeParams.SeekSleepMicroSeconds));
            }

            MostRecentlyUsedSector = endSector - 1;
        }

        auto rate = CalcRate(DiskModeParams.FirstSectorRate, DiskModeParams.LastSectorRate, midSector, MaxSector);

        auto rateByMilliSeconds = rate / 1000;
        auto milliSecondsToWait = std::max(0., (double)size / rateByMilliSeconds - operationTimeMs);
        Sleep(TDuration::MilliSeconds(milliSecondsToWait));
    }

    static double CalcRate(double firstSectorRate, double lastSectorRate, double sector, double lastSector) {
        Y_VERIFY(sector <= lastSector, "%lf %lf", sector, lastSector);
        Y_VERIFY(lastSectorRate <= firstSectorRate, "%lf %lf", firstSectorRate, lastSectorRate);
        return firstSectorRate - (sector / lastSector) * (firstSectorRate - lastSectorRate);
    }

private:
    TMutex Mutex;
    ui64 MaxSector = 0;
    ui64 MostRecentlyUsedSector = 0;
    TDiskModeParams DiskModeParams;
};

} // NSectorMap

/* TSectorMap */

class TSectorMap : public TThrRefBase {
public:
    TString Serial = CreateGuidAsString();
    ui64 DeviceSize;

    TTicketLock MapLock;
    std::atomic<bool> IsLocked;
    std::optional<std::pair<TDuration, TDuration>> ImitateRandomWait;
    std::atomic<double> ImitateIoErrorProbability;
    std::atomic<double> ImitateReadIoErrorProbability;

    std::atomic<ui64> AllocatedBytes;

    TSectorMap(ui64 deviceSize = 0, NSectorMap::EDiskMode diskMode = NSectorMap::DM_NONE)
      : DeviceSize(deviceSize)
      , IsLocked(false)
      , ImitateIoErrorProbability(0.0)
      , ImitateReadIoErrorProbability(0.0)
      , AllocatedBytes(0)
      , DiskMode(diskMode)
    {
        InitSectorOperationThrottler();
    }

    bool Lock() {
        return !IsLocked.exchange(true);
    }

    bool Unlock() {
        return IsLocked.exchange(false);
    }

    void ForceSize(ui64 size) {
        DeviceSize = size;
        if (DeviceSize < size) {
            for (const auto& [offset, data] : Map) {
                Y_VERIFY_S(offset + 4096 <= DeviceSize, "It is not possible to shrink TSectorMap with data");
            }
        }

        InitSectorOperationThrottler();
    }

    void InitSectorOperationThrottler() {
        if (DeviceSize > 0 && DiskMode != NSectorMap::DM_NONE) {
            SectorOperationThrottler = MakeHolder<NSectorMap::TSectorOperationThrottler>((DeviceSize + NSectorMap::SECTOR_SIZE - 1) / NSectorMap::SECTOR_SIZE, DiskMode);
        } else {
            SectorOperationThrottler.Reset();
        }
    }

    void ZeroInit(ui64 sectors) {
        ui64 bytes = sectors * NSectorMap::SECTOR_SIZE;
        TString str = TString::Uninitialized(bytes);
        memset(str.Detach(), 0, bytes);
        Write((ui8*)str.Detach(), bytes, 0);
    }

    void Read(ui8 *data, i64 size, ui64 offset, bool prevOperationIsInProgress = false) {
        Y_VERIFY(size % NSectorMap::SECTOR_SIZE == 0);
        Y_VERIFY(offset % NSectorMap::SECTOR_SIZE == 0);

        i64 dataSize = size;
        ui64 dataOffset = offset;
        THPTimer timer;

        TGuard<TTicketLock> guard(MapLock);
        for (; size > 0; size -= NSectorMap::SECTOR_SIZE) {
            if (auto it = Map.find(offset); it == Map.end()) {
                memset(data, 0x33, NSectorMap::SECTOR_SIZE);
            } else {
                char tmp[4 * NSectorMap::SECTOR_SIZE];
                int processed = LZ4_decompress_safe(it->second.data(), tmp, it->second.size(), 4 * NSectorMap::SECTOR_SIZE);
                Y_VERIFY_S(processed == NSectorMap::SECTOR_SIZE, "processed# " << processed);
                memcpy(data, tmp, NSectorMap::SECTOR_SIZE);
            }
            offset += NSectorMap::SECTOR_SIZE;
            data += NSectorMap::SECTOR_SIZE;
        }
        
        if (SectorOperationThrottler.Get() != nullptr) {
            SectorOperationThrottler->ThrottleRead(dataSize, dataOffset, prevOperationIsInProgress, timer.Passed() * 1000);
        }
    }

    void Write(const ui8 *data, i64 size, ui64 offset, bool prevOperationIsInProgress = false) {
        Y_VERIFY(size % NSectorMap::SECTOR_SIZE == 0);
        Y_VERIFY(offset % NSectorMap::SECTOR_SIZE == 0);

        i64 dataSize = size;
        ui64 dataOffset = offset;
        THPTimer timer;

        {
            TGuard<TTicketLock> guard(MapLock);
            for (; size > 0; size -= NSectorMap::SECTOR_SIZE) {
                char tmp[4 * NSectorMap::SECTOR_SIZE];
                int written = LZ4_compress_default((const char*)data, tmp, NSectorMap::SECTOR_SIZE, 4 * NSectorMap::SECTOR_SIZE);
                Y_VERIFY_S(written > 0, "written# " << written);
                TString str = TString::Uninitialized(written);
                memcpy(str.Detach(), tmp, written);
                if (auto it = Map.find(offset); it != Map.end()) {
                    AllocatedBytes.fetch_sub(it->second.size());
                    it->second = str;
                } else {
                    Map[offset] = str;
                }
                AllocatedBytes.fetch_add(Map[offset].size());
                offset += NSectorMap::SECTOR_SIZE;
                data += NSectorMap::SECTOR_SIZE;
            }
        }
        
        if (SectorOperationThrottler.Get() != nullptr) {
            SectorOperationThrottler->ThrottleRead(dataSize, dataOffset, prevOperationIsInProgress, timer.Passed() * 1000);
        }
    }

    void Trim(i64 size, ui64 offset) {
        TGuard<TTicketLock> guard(MapLock);
        Y_VERIFY(size % NSectorMap::SECTOR_SIZE == 0);
        Y_VERIFY(offset % NSectorMap::SECTOR_SIZE == 0);
        for (; size > 0; size -= NSectorMap::SECTOR_SIZE) {
            if (auto it = Map.find(offset); it != Map.end()) {
                AllocatedBytes.fetch_sub(it->second.size());
                Map.erase(it);
            }
            offset += NSectorMap::SECTOR_SIZE;
        }
    }

    ui64 DataBytes() const {
        return Map.size() * NSectorMap::SECTOR_SIZE;
    }

    TString ToString() const {
        TStringStream str;
        str << "Serial# " << Serial.Quote() << "\n";
        str << "DeviceSize# " << DeviceSize << "\n";
        str << "IsLocked# " << IsLocked.load() << "\n";
        if (ImitateRandomWait) {
            str << "ImitateRandomWait# [" << ImitateRandomWait->first << ", "
                << ImitateRandomWait->first + ImitateRandomWait->second << ")" << "\n";
        }
        str << "ImitateReadIoErrorProbability# " << ImitateReadIoErrorProbability.load() << "\n";
        str << "ImitateIoErrorProbability# " << ImitateIoErrorProbability.load() << "\n";
        str << "AllocatedBytes (approx.)# " << HumanReadableSize(AllocatedBytes.load(), SF_QUANTITY)  << "\n";
        str << "DataBytes# " << HumanReadableSize(DataBytes(), SF_QUANTITY)  << "\n";
        str << "DiskMode# " << DiskModeToString(DiskMode) << "\n";
        return str.Str();
    }

    // Requires proto information, so should be defined in cpp
    void LoadFromFile(const TString& path);
    void StoreToFile(const TString& path);

    NSectorMap::TSectorOperationThrottler::TDiskModeParams* GetDiskModeParams() {
        if (SectorOperationThrottler) {
            return SectorOperationThrottler->GetDiskModeParams();
        }
        return nullptr;
    }

private:
    THashMap<ui64, TString> Map;
    NSectorMap::EDiskMode DiskMode = NSectorMap::DM_NONE;
    THolder<NSectorMap::TSectorOperationThrottler> SectorOperationThrottler;
};

} // NPDisk
} // NKikimr
