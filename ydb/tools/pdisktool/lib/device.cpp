#include "device.h"

#include <ydb/library/pdisk_io/file_params.h>
#include <ydb/library/pdisk_io/sector_map.h>

#include <util/system/file.h>
#include <util/generic/yexception.h>
#include <cstring>

namespace NKikimr::NPDiskTool {

namespace {

class TFileDeviceReader final : public IDeviceReader {
    TFile File;
    ui64 DeviceSize = 0;
    TString Path;

public:
    TFileDeviceReader(const TString& path, bool tryLock, TIssueLog& issues)
        : Path(path)
    {
        File = TFile(path, OpenExisting | RdOnly);
        if (tryLock) {
            try {
                File.Flock(LOCK_SH | LOCK_NB);
            } catch (const yexception&) {
                issues.Warning(path, "Could not take a shared lock on the device; continuing read-only");
            }
        }
        bool isBlockDevice = false;
        DetectFileParameters(path, DeviceSize, isBlockDevice);
        if (DeviceSize == 0) {
            DeviceSize = File.GetLength();
        }
        if (DeviceSize == 0) {
            issues.Error(path, "Device size is 0");
        }
    }

    ui64 Size() const override {
        return DeviceSize;
    }

    void Pread(void* buffer, ui32 size, ui64 offset, TIssueLog& issues) override {
        if (offset + size > DeviceSize) {
            issues.Error(Path, TStringBuilder() << "Read past end of device offset# " << offset
                << " size# " << size << " deviceSize# " << DeviceSize);
            memset(buffer, 0, size);
            return;
        }
        const i32 got = File.Pread(buffer, size, offset);
        if (got < 0 || static_cast<ui32>(got) != size) {
            issues.Error(Path, TStringBuilder() << "Pread failed offset# " << offset << " size# " << size
                << " got# " << got);
            if (got > 0 && static_cast<ui32>(got) < size) {
                memset(static_cast<ui8*>(buffer) + got, 0, size - got);
            } else if (got <= 0) {
                memset(buffer, 0, size);
            }
        }
    }

    TString Description() const override {
        return Path;
    }
};

class TSectorMapDeviceReader final : public IDeviceReader {
    TIntrusivePtr<NPDisk::TSectorMap> Map;

public:
    explicit TSectorMapDeviceReader(TIntrusivePtr<NPDisk::TSectorMap> sectorMap)
        : Map(std::move(sectorMap))
    {}

    ui64 Size() const override {
        return Map->GetDeviceSize();
    }

    void Pread(void* buffer, ui32 size, ui64 offset, TIssueLog& issues) override {
        const ui32 sectorSize = NPDisk::NSectorMap::SECTOR_SIZE;
        ui8* dst = static_cast<ui8*>(buffer);
        ui64 remaining = size;
        ui64 pos = offset;

        // TSectorMap::Read requires sector-aligned offset and size. Handle head/tail by reading full sectors.
        if (pos % sectorSize != 0) {
            const ui64 aligned = pos - (pos % sectorSize);
            TVector<ui8> tmp(sectorSize);
            if (!Map->Read(tmp.data(), sectorSize, aligned)) {
                issues.Error("sectormap", TStringBuilder() << "Read failed offset# " << aligned);
                memset(dst, 0, remaining);
                return;
            }
            const ui32 skip = pos % sectorSize;
            const ui32 take = Min<ui32>(sectorSize - skip, remaining);
            memcpy(dst, tmp.data() + skip, take);
            dst += take;
            pos += take;
            remaining -= take;
        }
        const ui64 alignedSize = remaining - (remaining % sectorSize);
        if (alignedSize) {
            if (!Map->Read(dst, alignedSize, pos)) {
                issues.Error("sectormap", TStringBuilder() << "Read failed offset# " << pos << " size# " << alignedSize);
                memset(dst, 0, remaining);
                return;
            }
            dst += alignedSize;
            pos += alignedSize;
            remaining -= alignedSize;
        }
        if (remaining) {
            TVector<ui8> tmp(sectorSize);
            if (!Map->Read(tmp.data(), sectorSize, pos)) {
                issues.Error("sectormap", TStringBuilder() << "Read failed offset# " << pos);
                memset(dst, 0, remaining);
                return;
            }
            memcpy(dst, tmp.data(), remaining);
        }
    }

    TString Description() const override {
        return "TSectorMap";
    }
};

} // namespace

TIntrusivePtr<IDeviceReader> OpenFileDevice(const TString& path, bool tryLock, TIssueLog& issues) {
    return MakeIntrusive<TFileDeviceReader>(path, tryLock, issues);
}

TIntrusivePtr<IDeviceReader> OpenSectorMapDevice(TIntrusivePtr<NPDisk::TSectorMap> sectorMap) {
    return MakeIntrusive<TSectorMapDeviceReader>(std::move(sectorMap));
}

} // namespace NKikimr::NPDiskTool
