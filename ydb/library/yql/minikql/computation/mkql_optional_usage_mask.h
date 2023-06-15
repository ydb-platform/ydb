#pragma once

#include "mkql_computation_node_pack_impl.h"

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

    void Reset(TChunkedInputBuffer& buf) {
        Reset();
        ui64 bytes = UnpackUInt64(buf);
        if (bytes) {
            Mask.Reserve(bytes << 3ULL);
            buf.CopyTo(reinterpret_cast<char*>(const_cast<ui8*>(Mask.GetChunks())), bytes);
        }
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
