#pragma once
#include <ydb/library/accessor/accessor.h>

#include <util/generic/bitmap.h>
#include <util/generic/string.h>

namespace NKikimr::NOlap::NIndexes {
class TFixStringBitsStorage {
private:
    TDynBitMap Bits;

public:
    TFixStringBitsStorage(const TString& data);

    TString SerializeToString() const;

    bool TestHash(const ui64 hash) const {
        return Bits.Get(hash % Bits.Size());
    }

    static ui32 GrowBitsCountToByte(const ui32 bitsCount) {
        const ui32 bytesCount = bitsCount / 8;
        return (bytesCount + ((bitsCount % 8) ? 1 : 0)) * 8;
    }

    TString DebugString() const;

    TFixStringBitsStorage(TDynBitMap&& bits)
        : Bits(std::move(bits)) {
    }
    template <class TFixedSizeBitMap>
    TFixStringBitsStorage(TFixedSizeBitMap&& bits)
        : Bits(std::move(bits)) {
    }

    bool Get(const ui32 idx) const;
};

}   // namespace NKikimr::NOlap::NIndexes
