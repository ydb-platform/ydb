#include "bitmap.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void CopyBitmap(void* dst, ui32 dstOffset, const void* src, ui32 srcOffset, ui32 count)
{
    if (!count) {
        return;
    }

    auto dstOffsetSave = dstOffset;
    auto srcOffsetSave = srcOffset;
    Y_UNUSED(srcOffsetSave);
    Y_UNUSED(dstOffsetSave);

    using TWord = ui64;

    constexpr ui8 Bits = 8 * sizeof(TWord);

    auto* dstPtr = static_cast<TWord*>(dst);
    auto* srcPtr = static_cast<const TWord*>(src);

    dstPtr += dstOffset / Bits;
    dstOffset %= Bits;

    srcPtr += srcOffset / Bits;
    srcOffset %= Bits;

    auto* srcEnd = srcPtr + (srcOffset + count + Bits - 1) / Bits;
    auto* dstEnd = dstPtr + (dstOffset + count + Bits - 1) / Bits;

    {
        // Read min(count, Bits - dstOffset).
        auto word = srcPtr[0] >> srcOffset;
        if (srcOffset + count > Bits && srcOffset > dstOffset) {
            word |= srcPtr[1] << (Bits - srcOffset);
        }

        TWord firstWordMask = (TWord(1) << dstOffset) - 1;
        *dstPtr++ = (*dstPtr & firstWordMask) | (word << dstOffset);

        srcOffset += Bits - dstOffset;
        // Now dstOffset is zero.

        srcPtr += srcOffset / Bits;
        srcOffset %= Bits;
    }

    if (srcOffset) {
        if (srcPtr != srcEnd) {
            auto srcWord = *srcPtr++;
            auto dstWord = srcWord >> srcOffset;
            while (srcPtr != srcEnd) {
                srcWord = *srcPtr++;
                dstWord |= srcWord << (Bits - srcOffset);
                *dstPtr++ = dstWord;
                dstWord = srcWord >> srcOffset;
            }

            if (dstPtr != dstEnd) {
                *dstPtr++ = dstWord;
            }
        }
    } else {
        while (srcPtr != srcEnd) {
            *dstPtr++ = *srcPtr++;
        }
    }

#ifndef NDEBUG
    TBitmap srcBitmap(src);
    TBitmap dstBitmap(dst);

    for (int index = 0; index < static_cast<int>(count); ++index) {
        YT_VERIFY(srcBitmap[srcOffsetSave + index] == dstBitmap[dstOffsetSave + index]);
    }
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
