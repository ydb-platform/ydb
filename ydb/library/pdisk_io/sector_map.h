#pragma once

#include <ydb/core/util/yverify_stream.h> 
#include <library/cpp/actors/util/ticket_lock.h>

#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/system/mutex.h>

#include <contrib/libs/lz4/lz4.h>

#include <atomic>
#include <optional>

namespace NKikimr {
namespace NPDisk {


constexpr ui64 SectorMapSectorSize = 4096;

class TSectorMap : public TThrRefBase {
    THashMap<ui64, TString> Map;

public:
    TString Serial = CreateGuidAsString();
    ui64 DeviceSize;

    TTicketLock MapLock;
    std::atomic<bool> IsLocked;
    std::optional<std::pair<TDuration, TDuration>> ImitateRandomWait;
    std::atomic<double> ImitateIoErrorProbability;
    std::atomic<double> ImitateReadIoErrorProbability;

    std::atomic<ui64> AllocatedBytes;

    TSectorMap(ui64 deviceSize = 0)
      : DeviceSize(deviceSize)
      , IsLocked(false)
      , ImitateIoErrorProbability(0.0)
      , ImitateReadIoErrorProbability(0.0)
      , AllocatedBytes(0)
    {}

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
    }

    void ZeroInit(ui64 sectors) {
        ui64 bytes = sectors * SectorMapSectorSize;
        TString str = TString::Uninitialized(bytes);
        memset(str.Detach(), 0, bytes);
        Write((ui8*)str.Detach(), bytes, 0);
    }

    void Read(ui8 *data, i64 size, ui64 offset) {
        Y_VERIFY(size % SectorMapSectorSize == 0);
        Y_VERIFY(offset % SectorMapSectorSize == 0);

        TGuard<TTicketLock> guard(MapLock);
        for (; size > 0; size -= SectorMapSectorSize) {
            if (auto it = Map.find(offset); it == Map.end()) {
                memset(data, 0x33, SectorMapSectorSize);
            } else {
                char tmp[4 * SectorMapSectorSize];
                int processed = LZ4_decompress_safe(it->second.data(), tmp, it->second.size(), 4 * SectorMapSectorSize);
                Y_VERIFY_S(processed == SectorMapSectorSize, "processed# " << processed);
                memcpy(data, tmp, SectorMapSectorSize);
            }
            offset += SectorMapSectorSize;
            data += SectorMapSectorSize;
        }
    }

    void Write(const ui8 *data, i64 size, ui64 offset) {
        Y_VERIFY(size % SectorMapSectorSize == 0);
        Y_VERIFY(offset % SectorMapSectorSize == 0);

        TGuard<TTicketLock> guard(MapLock);
        for (; size > 0; size -= SectorMapSectorSize) {
            char tmp[4 * SectorMapSectorSize];
            int written = LZ4_compress_default((const char*)data, tmp, SectorMapSectorSize, 4 * SectorMapSectorSize);
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
            offset += SectorMapSectorSize;
            data += SectorMapSectorSize;
        }
    }

    void Trim(i64 size, ui64 offset) {
        TGuard<TTicketLock> guard(MapLock);
        Y_VERIFY(size % SectorMapSectorSize == 0);
        Y_VERIFY(offset % SectorMapSectorSize == 0);
        for (; size > 0; size -= SectorMapSectorSize) {
            if (auto it = Map.find(offset); it != Map.end()) {
                AllocatedBytes.fetch_sub(it->second.size());
                Map.erase(it);
            }
            offset += SectorMapSectorSize;
        }
    }

    ui64 DataBytes() const {
        return Map.size() * 4096;
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
        return str.Str();
    }

    // Requires proto information, so should be defined in cpp
    void LoadFromFile(const TString& path);
    void StoreToFile(const TString& path);
};

} // NPDisk
} // NKikimr
