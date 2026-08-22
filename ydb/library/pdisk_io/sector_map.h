#pragma once

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/library/actors/util/ticket_lock.h>

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
#include <random>
#include <deque>
#include <algorithm>
#include <variant>

#include <util/random/random.h>

#include "device_type.h"

namespace NKikimr {
namespace NPDisk {

namespace NSectorMap {

struct TFailureProbabilities {
    std::atomic<double> WriteErrorProbability;
    std::atomic<double> ReadErrorProbability;
    std::atomic<double> SilentWriteFailProbability;
    std::atomic<double> ReadReplayProbability;
};

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

inline EDiskMode DiskModeFromDeviceType(const EDeviceType& deviceType) {
    switch (deviceType) {
    case DEVICE_TYPE_ROT:
        return DM_HDD;
    case DEVICE_TYPE_SSD:
        return DM_SSD;
    case DEVICE_TYPE_NVME:
        return DM_NVME;
    default:
        return DM_NONE;
    }
}

inline EDeviceType DiskModeToDeviceType(const EDiskMode& diskMode) {
    switch (diskMode) {
    case DM_HDD:
        return DEVICE_TYPE_ROT;
    case DM_SSD:
        return DEVICE_TYPE_SSD;
    case DM_NVME:
        return DEVICE_TYPE_NVME;
    default:
        return DEVICE_TYPE_UNKNOWN;
    }
}

constexpr ui64 SECTOR_SIZE = 4096;

/* TSectorOperationThrottler: thread-safe */

class TSectorOperationThrottler {
public:
    struct TDiskModeParams {
        std::atomic<ui64> SeekSleepMicroSeconds;
        std::atomic<ui64> FirstSectorReadRate;
        std::atomic<ui64> LastSectorReadRate;
        std::atomic<ui64> FirstSectorWriteRate;
        std::atomic<ui64> LastSectorWriteRate;
    };

public:
    TSectorOperationThrottler(ui64 sectors, NSectorMap::EDiskMode diskMode) {
        Init(sectors, diskMode);
    }

    void Init(ui64 sectors, NSectorMap::EDiskMode diskMode) {
        Y_ABORT_UNLESS(sectors > 0);

        Y_ABORT_UNLESS((ui32)diskMode < DM_COUNT);
        EDeviceType deviceType = DiskModeToDeviceType(diskMode);
        const auto &devicePerformance = TDevicePerformanceParams::Get(deviceType);
        DiskModeParams.SeekSleepMicroSeconds = (devicePerformance.SeekTimeNs + 1000) / 1000 - 1;
        DiskModeParams.FirstSectorReadRate = devicePerformance.FirstSectorReadBytesPerSec;
        DiskModeParams.LastSectorReadRate = devicePerformance.LastSectorReadBytesPerSec;
        DiskModeParams.FirstSectorWriteRate = devicePerformance.FirstSectorWriteBytesPerSec;
        DiskModeParams.LastSectorWriteRate = devicePerformance.LastSectorWriteBytesPerSec;

        MaxSector = sectors - 1;
        MostRecentlyUsedSector = 0;
    }

    void ThrottleRead(i64 size, ui64 offset, bool prevOperationIsInProgress, double operationTimeMs) {
        ThrottleOperation(size, offset, prevOperationIsInProgress, operationTimeMs,
                DiskModeParams.FirstSectorReadRate, DiskModeParams.LastSectorReadRate);
    }

    void ThrottleWrite(i64 size, ui64 offset, bool prevOperationIsInProgress, double operationTimeMs) {
        ThrottleOperation(size, offset, prevOperationIsInProgress, operationTimeMs,
                DiskModeParams.FirstSectorWriteRate, DiskModeParams.LastSectorWriteRate);
    }

    TDiskModeParams* GetDiskModeParams() {
        return &DiskModeParams;
    }

private:
    /* throttle read/write operation */
    void ThrottleOperation(i64 size, ui64 offset, bool prevOperationIsInProgress, double operationTimeMs,
            ui64 firstSectorRate, ui64 lastSectorRate) {
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

        auto rate = CalcRate(firstSectorRate, lastSectorRate, midSector, MaxSector);

        auto rateByMilliSeconds = rate / 1000;
        auto milliSecondsToWait = std::max(0., (double)size / rateByMilliSeconds - operationTimeMs);
        Sleep(TDuration::MilliSeconds(milliSecondsToWait));
    }

    static double CalcRate(double firstSectorRate, double lastSectorRate, double sector, double lastSector) {
        Y_ABORT_UNLESS(sector <= lastSector, "%lf %lf", sector, lastSector);
        Y_ABORT_UNLESS(lastSectorRate <= firstSectorRate, "%lf %lf", firstSectorRate, lastSectorRate);
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
    std::atomic<ui64> IoErrorEveryNthRequests;
    std::atomic<ui64> ReadIoErrorEveryNthRequests;

    std::atomic<ui64> AllocatedBytes;
    NSectorMap::TFailureProbabilities FailureProbabilities;

    std::atomic<ui64> EmulatedWriteErrors;
    std::atomic<ui64> EmulatedReadErrors;
    std::atomic<ui64> EmulatedSilentWriteFails;
    std::atomic<ui64> EmulatedReadReplays;

private:
    THashMap<ui64, std::variant<TString, std::pair<TString, TString>>> Map;
    NSectorMap::EDiskMode DiskMode = NSectorMap::DM_NONE;
    THolder<NSectorMap::TSectorOperationThrottler> SectorOperationThrottler;
    std::function<void()> ReadCallback = nullptr;

    std::mt19937_64 RandomGenerator{RandomNumber<ui64>()};

public:
    TSectorMap(ui64 deviceSize = 0, NSectorMap::EDiskMode diskMode = NSectorMap::DM_NONE)
      : DeviceSize(deviceSize)
      , IsLocked(false)
      , IoErrorEveryNthRequests(0)
      , ReadIoErrorEveryNthRequests(0)
      , AllocatedBytes(0)
      , DiskMode(diskMode)
    {
        FailureProbabilities.WriteErrorProbability = 0.0;
        FailureProbabilities.ReadErrorProbability = 0.0;
        FailureProbabilities.SilentWriteFailProbability = 0.0;
        FailureProbabilities.ReadReplayProbability = 0.0;
        EmulatedWriteErrors = 0;
        EmulatedReadErrors = 0;
        EmulatedSilentWriteFails = 0;
        EmulatedReadReplays = 0;
        InitSectorOperationThrottler();
    }

    bool Lock() {
        return !IsLocked.exchange(true);
    }

    bool Unlock() {
        return IsLocked.exchange(false);
    }

    void ForceSize(ui64 size) {
        if (size < DeviceSize) {
            for (const auto& [offset, data] : Map) {
                Y_VERIFY_S(offset + 4096 <= DeviceSize, "It is not possible to shrink TSectorMap with data");
            }
        }

        DeviceSize = size;

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

    bool Read(ui8 *data, i64 size, ui64 offset, bool prevOperationIsInProgress = false) {
        Y_ABORT_UNLESS(size % NSectorMap::SECTOR_SIZE == 0);
        Y_ABORT_UNLESS(offset % NSectorMap::SECTOR_SIZE == 0);

        if (ShouldFailRead()) {
            return false;
        }

        i64 dataSize = size;
        ui64 dataOffset = offset;
        THPTimer timer;

        TGuard<TTicketLock> guard(MapLock);
        for (; size > 0; size -= NSectorMap::SECTOR_SIZE) {
            if (auto it = Map.find(offset); it == Map.end()) {
                memset(data, 0x33, NSectorMap::SECTOR_SIZE);
            } else {
                const TString* value = nullptr;
                const auto& dataVar = it->second;
                if (std::holds_alternative<TString>(dataVar)) {
                    value = &std::get<TString>(dataVar);
                } else {
                    const auto& pair = std::get<std::pair<TString, TString>>(dataVar);
                    if (ShouldReplayRead()) {
                        value = &pair.first;
                    } else {
                        value = &pair.second;
                    }
                }
                char tmp[4 * NSectorMap::SECTOR_SIZE];
                int processed = LZ4_decompress_safe(value->data(), tmp, value->size(), 4 * NSectorMap::SECTOR_SIZE);
                Y_VERIFY_S(processed == NSectorMap::SECTOR_SIZE, "processed# " << processed);
                memcpy(data, tmp, NSectorMap::SECTOR_SIZE);
            }
            offset += NSectorMap::SECTOR_SIZE;
            data += NSectorMap::SECTOR_SIZE;
        }

        if (SectorOperationThrottler.Get() != nullptr) {
            SectorOperationThrottler->ThrottleRead(dataSize, dataOffset, prevOperationIsInProgress, timer.Passed() * 1000);
        }

        if (ReadCallback) {
            ReadCallback();
        }
        return true;
    }

    bool Write(const ui8 *data, i64 size, ui64 offset, bool prevOperationIsInProgress = false) {
        Y_ABORT_UNLESS(size % NSectorMap::SECTOR_SIZE == 0);
        Y_ABORT_UNLESS(offset % NSectorMap::SECTOR_SIZE == 0);

        if (ShouldFailWrite()) {
            return false;
        }
        if (ShouldSilentWriteFail()) {
            return true;
        }

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

                bool replay = FailureProbabilities.ReadReplayProbability.load(std::memory_order_relaxed) > 0;
                i64 allocatedBytesDelta = 0;
                if (auto it = Map.find(offset); it != Map.end()) {
                    if (std::holds_alternative<TString>(it->second)) {
                        TString& old_val = std::get<TString>(it->second);
                        if (replay) {
                            allocatedBytesDelta = str.size();
                            it->second = std::make_pair(std::move(old_val), str);
                        } else {
                            allocatedBytesDelta = (i64)str.size() - (i64)old_val.size();
                            it->second = str;
                        }
                    } else {
                        auto& pair = std::get<std::pair<TString, TString>>(it->second);
                        if (replay) {
                            allocatedBytesDelta = (i64)str.size() - (i64)pair.first.size();
                            pair.first = std::move(pair.second);
                            pair.second = str;
                        } else {
                            allocatedBytesDelta = -(i64)(pair.first.size() + pair.second.size()) + str.size();
                            it->second = str;
                        }
                    }
                } else {
                    Map[offset] = str;
                    allocatedBytesDelta = str.size();
                }

                AllocatedBytes.fetch_add(allocatedBytesDelta);
                offset += NSectorMap::SECTOR_SIZE;
                data += NSectorMap::SECTOR_SIZE;
            }
        }

        if (SectorOperationThrottler.Get() != nullptr) {
            SectorOperationThrottler->ThrottleWrite(dataSize, dataOffset, prevOperationIsInProgress, timer.Passed() * 1000);
        }
        return true;
    }

    void Trim(i64 size, ui64 offset) {
        TGuard<TTicketLock> guard(MapLock);
        Y_ABORT_UNLESS(size % NSectorMap::SECTOR_SIZE == 0);
        Y_ABORT_UNLESS(offset % NSectorMap::SECTOR_SIZE == 0);
        for (; size > 0; size -= NSectorMap::SECTOR_SIZE) {
            if (auto it = Map.find(offset); it != Map.end()) {
                size_t previousSize = 0;
                if (std::holds_alternative<TString>(it->second)) {
                    previousSize = std::get<TString>(it->second).size();
                } else {
                    auto& pair = std::get<std::pair<TString, TString>>(it->second);
                    previousSize = pair.first.size() + pair.second.size();
                }
                AllocatedBytes.fetch_sub(previousSize);
                Map.erase(it);
            }
            offset += NSectorMap::SECTOR_SIZE;
        }
    }

    ui64 DataBytes() const {
        return Map.size() * NSectorMap::SECTOR_SIZE;
    }

    void SetReadCallback(std::function<void()> callback) {
        TGuard<TTicketLock> guard(MapLock);
        ReadCallback = callback;
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
        str << "ReadIoErrorEveryNthRequests# " << ReadIoErrorEveryNthRequests.load() << "\n";
        str << "IoErrorEveryNthRequests# " << IoErrorEveryNthRequests.load() << "\n";
        str << "AllocatedBytes (approx.)# " << HumanReadableSize(AllocatedBytes.load(), SF_QUANTITY)  << "\n";
        str << "DataBytes# " << HumanReadableSize(DataBytes(), SF_QUANTITY)  << "\n";
        str << "DiskMode# " << DiskModeToString(DiskMode) << "\n";
        return str.Str();
    }

    // Requires proto information, so should be defined in cpp
    void LoadFromFile(const TString& path);
    void StoreToFile(const TString& path);

    ui64 GetDeviceSize() const {
        return DeviceSize;
    }

    NSectorMap::TSectorOperationThrottler::TDiskModeParams* GetDiskModeParams() {
        if (SectorOperationThrottler) {
            return SectorOperationThrottler->GetDiskModeParams();
        }
        return nullptr;
    }

    NSectorMap::TFailureProbabilities* GetFailureProbabilities() {
        return &FailureProbabilities;
    }

    bool ShouldFailWrite() {
        if (CheckFailure(FailureProbabilities.WriteErrorProbability.load(std::memory_order_relaxed))) {
            EmulatedWriteErrors.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

    bool ShouldFailRead() {
        if (CheckFailure(FailureProbabilities.ReadErrorProbability.load(std::memory_order_relaxed))) {
            EmulatedReadErrors.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

    bool ShouldSilentWriteFail() {
        if (CheckFailure(FailureProbabilities.SilentWriteFailProbability.load(std::memory_order_relaxed))) {
            EmulatedSilentWriteFails.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

    bool ShouldReplayRead() {
        if (CheckFailure(FailureProbabilities.ReadReplayProbability.load(std::memory_order_relaxed))) {
            EmulatedReadReplays.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

private:
    bool CheckFailure(double probability) {
        if (probability <= 0.0) {
            return false;
        }
        if (probability >= 1.0) {
            return true;
        }
        std::uniform_real_distribution<double> distribution(0.0, 1.0);
        return distribution(RandomGenerator) < probability;
    }
};

} // NPDisk
} // NKikimr
