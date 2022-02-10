#pragma once

#include <util/generic/bitops.h>
#include <util/system/unaligned_mem.h>

namespace NBitIO {
    class TBitInputImpl {
        i64 RealStart;
        i64 Start;
        ui64 Length;
        ui64 BOffset;
        const ui32 FakeStart;
        char Fake[16];
        const i64 FStart;

    public:
        TBitInputImpl(const char* start, const char* end)
            : RealStart((i64)start)
            , Start((i64)start)
            , Length((end - start) << 3)
            , BOffset(0)
            , FakeStart(Length > 64 ? Length - 64 : 0)
            , FStart((i64)Fake - (FakeStart >> 3))
        {
            memcpy(Fake, (const char*)(RealStart + (FakeStart >> 3)), (Length - FakeStart) >> 3);
            Start = FakeStart ? RealStart : FStart;
        }

        ui64 GetBitLength() const {
            return Length;
        }

    protected:
        template <ui32 bits>
        Y_FORCE_INLINE bool ReadKImpl(ui64& result) {
            result = (ReadUnaligned<ui64>((const void*)(Start + (BOffset >> 3))) >> (BOffset & 7)) & Mask64(bits);
            BOffset += bits;
            if (BOffset < FakeStart)
                return true;
            if (BOffset > Length) {
                result = 0;
                BOffset -= bits;
                return false;
            }
            Start = FStart;
            return true;
        }

        Y_FORCE_INLINE bool ReadImpl(ui64& result, ui32 bits) {
            result = (ReadUnaligned<ui64>((const void*)(Start + (BOffset >> 3))) >> (BOffset & 7)) & MaskLowerBits(bits);
            BOffset += bits;
            if (BOffset < FakeStart)
                return true;
            if (Y_UNLIKELY(BOffset > Length)) {
                result = 0;
                BOffset -= bits;
                return false;
            }
            Start = FStart;
            return true;
        }

        Y_FORCE_INLINE bool EofImpl() const {
            return BOffset >= Length;
        }

        Y_FORCE_INLINE ui64 BitOffset() const {
            return BOffset;
        }

        Y_FORCE_INLINE bool Seek(i64 offset) {
            if (offset < 0 || offset > (i64)Length)
                return false;
            BOffset = offset;
            Start = BOffset < FakeStart ? RealStart : FStart;
            return true;
        }

    protected:
        template <ui64 bits, typename T>
        Y_FORCE_INLINE static void CopyToResultK(T& result, ui64 r64, ui64 skipbits) {
            result = (result & ~(Mask64(bits) << skipbits)) | (r64 << skipbits);
        }

        template <typename T>
        Y_FORCE_INLINE static void CopyToResult(T& result, ui64 r64, ui64 bits, ui64 skipbits) {
            result = (result & InverseMaskLowerBits(bits, skipbits)) | (r64 << skipbits);
        }

    public:
        template <ui64 bits>
        Y_FORCE_INLINE bool ReadWordsImpl(ui64& data) {
            data = 0;

            const ui64 haveMore = NthBit64(bits);
            const ui64 mask = Mask64(bits);
            ui64 current = 0;
            ui64 byteNo = 0;

            do {
                if (!ReadKImpl<bits + 1>(current))
                    return false;

                data |= (current & mask) << (byteNo++ * bits);
            } while (current & haveMore);

            return true;
        }
    };
}
