#pragma once
#include <ydb/library/accessor/accessor.h>

#include <util/generic/bitmap.h>
#include <util/generic/string.h>

namespace NKikimr::NOlap::NIndexes {
class TFixStringBitsStorage {
private:
    YDB_READONLY_DEF(TString, Data);

    template <class T>
    class TSizeDetector {};

    template <>
    class TSizeDetector<std::vector<bool>> {
    public:
        static ui32 GetSize(const std::vector<bool>& v) {
            return v.size();
        }
    };

    template <>
    class TSizeDetector<TDynBitMap> {
    public:
        static ui32 GetSize(const TDynBitMap& v) {
            return v.Size();
        }
    };

public:
    TFixStringBitsStorage(const TString& data)
        : Data(data) {
    }

    static ui32 GrowBitsCountToByte(const ui32 bitsCount) {
        const ui32 bytesCount = bitsCount / 8;
        return (bytesCount + ((bitsCount % 8) ? 1 : 0)) * 8;
    }

    TString DebugString() const;

    template <class TBitsVector>
    TFixStringBitsStorage(const TBitsVector& bitsVector)
        : TFixStringBitsStorage(TSizeDetector<TBitsVector>::GetSize(bitsVector)) {
        ui32 byteIdx = 0;
        ui8 byteCurrent = 0;
        ui8 shiftCurrent = 1;
        for (ui32 i = 0; i < TSizeDetector<TBitsVector>::GetSize(bitsVector); ++i) {
            if (i && i % 8 == 0) {
                Data[byteIdx] = (char)byteCurrent;
                byteCurrent = 0;
                shiftCurrent = 1;
                ++byteIdx;
            }
            if (bitsVector[i]) {
                byteCurrent += shiftCurrent;
            }
            shiftCurrent = (shiftCurrent << 1);
        }
        if (byteCurrent) {
            Data[byteIdx] = (char)byteCurrent;
        }
    }

    ui32 GetSizeBits() const {
        return Data.size() * 8;
    }

    TFixStringBitsStorage(const ui32 sizeBits)
        : Data(GrowBitsCountToByte(sizeBits) / 8, '\0') {
    }

    void Set(const bool val, const ui32 idx);
    bool Get(const ui32 idx) const;
};

}   // namespace NKikimr::NOlap::NIndexes
