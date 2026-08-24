#include "device.h"

#include <ydb/library/pdisk_io/file_params.h>
#include <ydb/library/pdisk_io/sector_map.h>

#include <util/generic/algorithm.h>
#include <util/generic/ptr.h>
#include <util/generic/yexception.h>
#include <util/system/file.h>

#include <cstring>

#if defined(_unix_)
#include <sys/stat.h>
#include <unistd.h>
#endif

#if defined(_linux_)
#include <sys/ioctl.h>
#if !defined(_musl_)
#include <linux/fs.h>
#else
#include <sys/mount.h>
#endif
#endif

namespace NKikimr::NPDiskTool {

namespace {

constexpr ui32 IoAlign = 4096;

class TAlignedBuf {
    TArrayHolder<ui8> Raw;
    ui8* Ptr = nullptr;
    ui32 Cap = 0;

public:
    void Ensure(ui32 size) {
        if (Ptr && size <= Cap) {
            return;
        }
        size = Max<ui32>(size, IoAlign);
        Raw.Reset(new ui8[size + IoAlign - 1]);
        Ptr = reinterpret_cast<ui8*>(
            (reinterpret_cast<uintptr_t>(Raw.Get()) + IoAlign - 1) / IoAlign * IoAlign);
        Cap = size;
    }

    ui8* Get() {
        return Ptr;
    }
};

ui64 QueryDeviceSize(const TFile& file, const TString& path, bool& isBlockDevice, TIssueLog& issues) {
    isBlockDevice = false;

#if defined(_unix_)
    struct stat st;
    if (::fstat(file.GetHandle(), &st) == 0) {
        if (S_ISREG(st.st_mode)) {
            return static_cast<ui64>(st.st_size);
        }
        if (S_ISBLK(st.st_mode)) {
            isBlockDevice = true;
#if defined(_linux_)
            ui64 size = 0;
            if (::ioctl(file.GetHandle(), BLKGETSIZE64, &size) == 0 && size > 0) {
                return size;
            }
#endif
            const off_t off = ::lseek(file.GetHandle(), 0, SEEK_END);
            if (off > 0) {
                ::lseek(file.GetHandle(), 0, SEEK_SET);
                return static_cast<ui64>(off);
            }
        }
    }
#endif

    try {
        ui64 size = 0;
        DetectFileParameters(path, size, isBlockDevice);
        if (size > 0) {
            return size;
        }
    } catch (const yexception& e) {
        issues.Warning(path, TStringBuilder() << "DetectFileParameters failed: " << e.what());
    }

    const i64 len = file.GetLength();
    return len > 0 ? static_cast<ui64>(len) : 0;
}

class TFileDeviceReader final : public IDeviceReader {
    TFile File;
    ui64 DeviceSize = 0;
    TString Path;
    bool Direct = false;
    TAlignedBuf Bounce;

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
        DeviceSize = QueryDeviceSize(File, path, isBlockDevice, issues);
        if (DeviceSize == 0) {
            issues.Warning(path, "Could not determine device size; reads will still be attempted");
        }

        if (isBlockDevice) {
            // O_DIRECT needs 4KiB-aligned buffers; fall back to buffered I/O if the kernel rejects it.
            try {
                File.SetDirect();
                Direct = true;
            } catch (const yexception&) {
                Direct = false;
            }
        }
    }

    ui64 Size() const override {
        return DeviceSize;
    }

    void Pread(void* buffer, ui32 size, ui64 offset, TIssueLog& issues) override {
        if (DeviceSize > 0 && offset + size > DeviceSize) {
            issues.Error(Path, TStringBuilder() << "Read past end of device offset# " << offset
                << " size# " << size << " deviceSize# " << DeviceSize);
            memset(buffer, 0, size);
            return;
        }

        auto copyOut = [&](const ui8* src, size_t got) {
            if (got > 0) {
                memcpy(buffer, src, got);
            }
            if (got < size) {
                memset(static_cast<ui8*>(buffer) + got, 0, size - got);
                issues.Error(Path, TStringBuilder() << "Short pread offset# " << offset
                    << " size# " << size << " got# " << got
                    << " deviceSize# " << DeviceSize);
            }
        };

        try {
            if (Direct) {
                const ui64 alignedOff = offset - (offset % IoAlign);
                const ui32 head = static_cast<ui32>(offset - alignedOff);
                const ui32 need = head + size;
                const ui32 alignedSize = (need + IoAlign - 1) / IoAlign * IoAlign;
                Bounce.Ensure(alignedSize);
                const size_t got = File.Pread(Bounce.Get(), alignedSize, static_cast<i64>(alignedOff));
                if (got <= head) {
                    copyOut(nullptr, 0);
                    return;
                }
                copyOut(Bounce.Get() + head, Min<size_t>(size, got - head));
                return;
            }
            Bounce.Ensure(size);
            const size_t got = File.Pread(Bounce.Get(), size, static_cast<i64>(offset));
            copyOut(Bounce.Get(), got);
        } catch (const yexception& e) {
            issues.Error(Path, TStringBuilder() << "Pread failed offset# " << offset
                << " size# " << size << ": " << e.what());
            memset(buffer, 0, size);
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
