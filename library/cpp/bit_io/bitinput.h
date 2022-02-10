#pragma once

#include "bitinput_impl.h"

#include <util/system/yassert.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>

#include <iterator>

namespace NBitIO {
    // Based on junk/solar/codecs/bitstream.h

    class TBitInput: protected  TBitInputImpl {
    public:
        template <typename TVec>
        explicit TBitInput(const TVec& vec)
            : TBitInputImpl(std::begin(vec), std::end(vec))
        {
        }

        TBitInput(const char* start, const char* end)
            : TBitInputImpl(start, end)
        {
        }

        bool Eof() const {
            return EofImpl();
        }

        ui64 GetOffset() const {
            ui64 bo = BitOffset();
            return bo / 8 + !!(bo % 8);
        }

        using TBitInputImpl::GetBitLength;

        ui64 GetBitOffset() const {
            return BitOffset() % 8;
        }

    public:
        // Read with static number of bits.
        // Preserves what's in result.
        template <ui64 bits, typename T>
        Y_FORCE_INLINE bool ReadK(T& result, ui64 skipbits) {
            ui64 r64 = 0;
            bool ret = bits <= 56 ? ReadKImpl<bits>(r64) : ReadSafe(r64, bits);
            CopyToResultK<bits>(result, r64, skipbits);
            return ret;
        }

        // Read with static number of bits.
        // Zeroes other bits in result.
        template <ui64 bits, typename T>
        Y_FORCE_INLINE bool ReadK(T& result) {
            ui64 r = 0;
            bool res = ReadK<bits>(r);
            result = r;
            return res;
        }

        // Shortcut to impl.
        template <ui64 bits>
        Y_FORCE_INLINE bool ReadK(ui64& result) {
            if (bits <= 56)
                return ReadKImpl<bits>(result);

            ui64 r1 = 0ULL;
            ui64 r2 = 0ULL;

            bool ret1 = ReadKImpl<56ULL>(r1);
            bool ret2 = ReadKImpl<(bits > 56ULL ? bits - 56ULL : 0) /*or else we get negative param in template*/>(r2);

            result = (r2 << 56ULL) | r1;

            return ret1 & ret2;
        }

        // It's safe to read up to 64 bits.
        // Zeroes other bits in result.
        template <typename T>
        Y_FORCE_INLINE bool ReadSafe(T& result, ui64 bits) {
            if (bits <= 56ULL)
                return Read(result, bits);

            ui64 r1 = 0ULL;
            ui64 r2 = 0ULL;

            bool ret1 = ReadKImpl<56ULL>(r1);
            bool ret2 = ReadImpl(r2, bits - 56ULL);

            result = (r2 << 56ULL) | r1;

            return ret1 & ret2;
        }

        // It's safe to read up to 64 bits.
        // Preserves what's in result.
        template <typename T>
        Y_FORCE_INLINE bool ReadSafe(T& result, ui64 bits, ui64 skipbits) {
            ui64 r64 = 0;
            bool ret = ReadSafe(r64, bits);
            CopyToResult(result, r64, bits, skipbits);
            return ret;
        }

        // Do not try to read more than 56 bits at once. Split in two reads or use ReadSafe.
        // Zeroes other bits in result.
        template <typename T>
        Y_FORCE_INLINE bool Read(T& result, ui64 bits) {
            ui64 r64 = 0;
            bool ret = ReadImpl(r64, bits);
            result = r64;
            return ret;
        }

        // Shortcut to impl.
        Y_FORCE_INLINE bool Read(ui64& result, ui64 bits) {
            return ReadImpl(result, bits);
        }

        // Do not try to read more than 56 bits at once. Split in two reads or use ReadSafe.
        // Preserves what's in result.
        template <typename T>
        Y_FORCE_INLINE bool Read(T& result, ui64 bits, ui64 skipbits) {
            ui64 r64 = 0;
            bool ret = ReadImpl(r64, bits);
            CopyToResult(result, r64, bits, skipbits);
            return ret;
        }

        // Unsigned wordwise read. Underlying data is splitted in "words" of "bits(data) + 1(flag)" bits.
        // Like this: (unsigned char)0x2E<3> (0010 1110) <=> 1110 0101
        //                                                   fddd fddd
        template <ui64 bits, typename T>
        Y_FORCE_INLINE bool ReadWords(T& result) {
            ui64 r64 = 0;

            bool retCode = ReadWordsImpl<bits>(r64);
            result = r64;

            return retCode;
        }

        // Shortcut to impl.
        template <ui64 bits>
        Y_FORCE_INLINE bool ReadWords(ui64& result) {
            return ReadWordsImpl<bits>(result);
        }

        Y_FORCE_INLINE bool Back(int bits) {
            return Seek(BitOffset() - bits);
        }

        Y_FORCE_INLINE bool Seek(int bitoffset) {
            return TBitInputImpl::Seek(bitoffset);
        }

        // A way to read a portion of bits at random location.
        // Didn't want to complicate sequential read, neither to copypaste.
        template <typename T>
        Y_FORCE_INLINE bool ReadRandom(ui64 bitoffset, T& result, ui64 bits, ui64 skipbits) {
            const ui64 curr = BitOffset();
            Seek(bitoffset);
            bool ret = ReadSafe<T>(result, bits, skipbits);
            Seek(curr);
            return ret;
        }
    };
}
