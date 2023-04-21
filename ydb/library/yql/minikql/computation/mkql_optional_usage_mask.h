#pragma once

#include <ydb/library/yql/minikql/pack_num.h>
#include <util/generic/bitmap.h>
#include <util/generic/buffer.h>

#include <cstring>

namespace NKikimr {
namespace NMiniKQL {

namespace NDetails {

class TOptionalUsageMask {
public:
    TOptionalUsageMask() = default;

    void Reset() {
        CountOfOptional = 0U;
        Mask.Clear();
    }

    void Reset(TStringBuf& buf) {
        Reset();
        ui64 bytes = 0ULL;
        buf.Skip(Unpack64(buf.data(), buf.size(), bytes));
        if (bytes) {
            Y_VERIFY_DEBUG(bytes <= buf.size());
            Mask.Reserve(bytes << 3ULL);
            std::memcpy(const_cast<ui8*>(Mask.GetChunks()), buf.data(), bytes);
            buf.Skip(bytes);
        }
    }

    ui32 GetOptionalCount() const {
        return CountOfOptional;
    }

    void Shrink(ui32 newSize) {
        Y_VERIFY(newSize <= CountOfOptional, "Invalid shrink size");
        Mask.Reset(newSize, CountOfOptional);
        CountOfOptional = newSize;
    }

    void SetNextEmptyOptional(bool empty) {
        if (empty) {
            Mask.Set(CountOfOptional);
        }
        ++CountOfOptional;
    }

    bool IsNextEmptyOptional() {
        return Mask.Test(CountOfOptional++);
    }

    bool IsEmptyMask() const {
        return Mask.Empty();
    }

    size_t CalcSerializedSize() {
        if (!CountOfOptional || Mask.Empty()) {
            return 1ULL; // One byte for size=0
        }

        const size_t usedBits = Mask.ValueBitCount();
        const size_t usedBytes = (usedBits + 7ULL) >> 3ULL;
        return GetPack64Length(usedBytes) + usedBytes;
    }

    template<typename TBuf>
    void Serialize(TBuf& buf) const {
        if (!CountOfOptional || Mask.Empty()) {
            return buf.Append(0);
        }

        const size_t usedBits = Mask.ValueBitCount();
        const size_t usedBytes = (usedBits + 7ULL) >> 3ULL;
        buf.Advance(MAX_PACKED64_SIZE);
        // Note: usage of Pack64() is safe here - it won't overwrite useful data for small values of usedBytes
        buf.EraseBack(MAX_PACKED64_SIZE - Pack64(usedBytes, buf.Pos() - MAX_PACKED64_SIZE));
        buf.Append(reinterpret_cast<const char*>(Mask.GetChunks()), usedBytes);
    }

private:
    ui32 CountOfOptional = 0U;
    TBitMapOps<TDynamicBitMapTraits<ui8>> Mask;
};

} // NDetails
}
}
