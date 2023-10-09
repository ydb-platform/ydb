#pragma once

#include <library/cpp/deprecated/accessors/accessors.h>

#include <util/stream/output.h>
#include <util/system/yassert.h>
#include <util/generic/bitops.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>

namespace NBitIO {
    // Based on junk/solar/codecs/bitstream.h

    // Almost all code is hard tuned for sequential write performance.
    // Use tools/bursttrie/benchmarks/bitstreams_benchmark to check your changes.

    inline constexpr ui64 BytesUp(ui64 bits) {
        return (bits + 7ULL) >> 3ULL;
    }

    template <typename TStorage>
    class TBitOutputBase {
    protected:
        TStorage* Storage;
        ui64 FreeBits;
        ui64 Active;
        ui64 Offset;

    public:
        TBitOutputBase(TStorage* storage)
            : Storage(storage)
            , FreeBits(64)
            , Active()
            , Offset()
        {
        }

        ui64 GetOffset() const {
            return Offset + BytesUp(64ULL - FreeBits);
        }

        ui64 GetBitOffset() const {
            return (64ULL - FreeBits) & 7ULL;
        }

        ui64 GetByteReminder() const {
            return FreeBits & 7ULL;
        }

    public:
        // interface

        // Write "bits" lower bits.
        Y_FORCE_INLINE void Write(ui64 data, ui64 bits) {
            if (FreeBits < bits) {
                if (FreeBits) {
                    bits -= FreeBits;

                    Active |= (data & MaskLowerBits(FreeBits)) << (64ULL - FreeBits);
                    data >>= FreeBits;

                    FreeBits = 0ULL;
                }

                Flush();
            }

            Active |= bits ? ((data & MaskLowerBits(bits)) << (64ULL - FreeBits)) : 0;
            FreeBits -= bits;
        }

        // Write "bits" lower bits starting from "skipbits" bit.
        Y_FORCE_INLINE void Write(ui64 data, ui64 bits, ui64 skipbits) {
            Write(data >> skipbits, bits);
        }

        // Unsigned wordwise write. Underlying data is splitted in "words" of "bits(data) + 1(flag)" bits.
        // Like this: (unsigned char)0x2E<3> (0000 0010 1110) <=> 1110 0101
        //                                                        fddd fddd
        template <ui64 bits>
        Y_FORCE_INLINE void WriteWords(ui64 data) {
            do {
                ui64 part = data;

                data >>= bits;
                part |= FastZeroIfFalse(data, NthBit64(bits));
                Write(part, bits + 1ULL);
            } while (data);
        }

        Y_FORCE_INLINE ui64 /* padded bits */ Flush() {
            const ui64 ubytes = 8ULL - (FreeBits >> 3ULL);

            if (ubytes) {
                Active <<= FreeBits;
                Active >>= FreeBits;

                Storage->WriteData((const char*)&Active, (const char*)&Active + ubytes);
                Offset += ubytes;
            }

            const ui64 padded = FreeBits & 7;

            FreeBits = 64ULL;
            Active = 0ULL;

            return padded;
        }

        virtual ~TBitOutputBase() {
            Flush();
        }

    private:
        static Y_FORCE_INLINE ui64 FastZeroIfFalse(bool cond, ui64 iftrue) {
            return -i64(cond) & iftrue;
        }
    };

    template <typename TVec>
    class TBitOutputVectorImpl {
        TVec* Data;

    public:
        void WriteData(const char* begin, const char* end) {
            NAccessors::Append(*Data, begin, end);
        }

        TBitOutputVectorImpl(TVec* data)
            : Data(data)
        {
        }
    };

    template <typename TVec>
    struct TBitOutputVector: public TBitOutputVectorImpl<TVec>, public TBitOutputBase<TBitOutputVectorImpl<TVec>> {
        inline TBitOutputVector(TVec* data)
            : TBitOutputVectorImpl<TVec>(data)
            , TBitOutputBase<TBitOutputVectorImpl<TVec>>(this)
        {
        }
    };

    class TBitOutputArrayImpl {
        char* Data;
        size_t Left;

    public:
        void WriteData(const char* begin, const char* end) {
            size_t sz = end - begin;
            Y_ABORT_UNLESS(sz <= Left, " ");
            memcpy(Data, begin, sz);
            Data += sz;
            Left -= sz;
        }

        TBitOutputArrayImpl(char* begin, size_t len)
            : Data(begin)
            , Left(len)
        {
        }
    };

    struct TBitOutputArray: public TBitOutputArrayImpl, public TBitOutputBase<TBitOutputArrayImpl> {
        inline TBitOutputArray(char* begin, size_t len)
            : TBitOutputArrayImpl(begin, len)
            , TBitOutputBase<TBitOutputArrayImpl>(this)
        {
        }
    };

    using TBitOutputYVector = TBitOutputVector<TVector<char>>;

    class TBitOutputStreamImpl {
        IOutputStream* Out;

    public:
        void WriteData(const char* begin, const char* end) {
            Out->Write(begin, end - begin);
        }

        TBitOutputStreamImpl(IOutputStream* out)
            : Out(out)
        {
        }
    };

    struct TBitOutputStream: public TBitOutputStreamImpl, public TBitOutputBase<TBitOutputStreamImpl> {
        inline TBitOutputStream(IOutputStream* out)
            : TBitOutputStreamImpl(out)
            , TBitOutputBase<TBitOutputStreamImpl>(this)
        {
        }
    };
}
