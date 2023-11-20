#pragma once

#include "defs.h"
#include "blobstorage_pdisk_data.h"

#include <util/generic/strbuf.h>
#include <util/stream/format.h>

namespace NKikimr::NPDisk {

class TSector {
    TStringBuf Buf;

public:
    TSector(const ui8* data, ui32 size)
        : Buf(reinterpret_cast<const char*>(data), size)
    {}

    TSector(const char* data, ui32 size)
        : Buf(data, size)
    {}

    ui8* Begin() {
        return (ui8*)Buf.Data();
    }

    const ui8* Begin() const {
        return (const ui8*)Buf.Data();
    }

    ui8* End() {
        return Begin() + Size();
    }

    const ui8* End() const {
        return Begin() + Size();
    }

    ui8 &operator[](ui32 idx) {
        return *(Begin() + idx);
    }

    const ui8 &operator[](ui32 idx) const {
        return *(Begin() + idx);
    }

    size_t Size() const {
        return Buf.Size();
    }

    TDataSectorFooter *GetDataFooter() {
        Y_DEBUG_ABORT_UNLESS(Size() >= sizeof(TDataSectorFooter));
        return (TDataSectorFooter*) (End() - sizeof(TDataSectorFooter));
    }

    ui64 GetCanary() const {
        Y_DEBUG_ABORT_UNLESS(Size() >= sizeof(TDataSectorFooter) + CanarySize);
        return ReadUnaligned<ui64>(End() - sizeof(TDataSectorFooter) - CanarySize);
    }

    void SetCanary(ui64 canary = NPDisk::Canary) {
        Y_DEBUG_ABORT_UNLESS(Size() >= sizeof(TDataSectorFooter) + CanarySize);
        WriteUnaligned<ui64>(End() - sizeof(TDataSectorFooter) - CanarySize, canary);
    }

    TString ToString(size_t widthBytes = 16) {
        TStringStream out;
        for (ui32 row = 0; row < (Buf.size() + widthBytes - 1) / widthBytes; ++row) {
            out << LeftPad(row * widthBytes, 6) << ": ";
            for (ui32 col = 0; col < widthBytes; ++col) {
                const ui32 idx = row * widthBytes + col;
                if (col) {
                    out << ' ';
                    if (col % (widthBytes / 2) == 0) {
                        out << ' ';
                    }
                }
                if (idx < Buf.size()) {
                    out << Hex(Buf[idx], HF_FULL);
                } else {
                    out << "  ";
                }
            }
            out << '\n';
        }
        return out.Str();
    }

    bool CheckCanary() {
        return GetCanary() == NPDisk::Canary;
    }
};

class TSectorsWithData {
    const ui32 SectorSize;
    const ui32 SectorCount;
    TString Buf;

public:
    TSectorsWithData(ui32 sectorSize, ui32 sectorCount)
        : SectorSize(sectorSize)
        , SectorCount(sectorCount)
        , Buf(TString::Uninitialized(sectorSize * sectorCount))
    {}

    ui8* Data() {
        return reinterpret_cast<ui8*>(Buf.Detach());
    }

    size_t Size() const {
        return SectorCount;
    }

    TSector Begin() {
        return (*this)[0];
    }

    TSector End() {
        return (*this)[SectorCount - 1];
    }

    TSector operator[](ui32 idx) {
        return {Buf.Data() + idx * SectorSize, SectorSize};
    }

    const TSector operator[](ui32 idx) const {
        return {Buf.Data() + idx * SectorSize, SectorSize};
    }
};


} // namespace NKikimr::NPDisk
