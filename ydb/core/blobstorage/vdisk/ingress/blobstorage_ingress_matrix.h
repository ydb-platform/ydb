#pragma once

#include <ydb/core/util/bits.h>

#include <util/stream/str.h>
#include <util/string/cast.h>

#include <bit>

namespace NKikimr {

    namespace NMatrix {

        static const ui32 BitsInByte = CHAR_BIT;

        /*

               | parts
               |-------------
               | 1 2 3 4 5 6
         --------------------
         n | 0 | 1
         o | 1 |   1
         d | 2 |     1
         e | 3 |       1
         s | 4 |         0
           | 5 |           1
         --------------------
   handoff | 6 | 0 0 0 0 1 0
  replicas | 7 | 0 0 1 0 1 0

         */



        //////////////////////////////////////////////////////////////////////////////////
        // TVectorType
        //////////////////////////////////////////////////////////////////////////////////
        class TVectorType {
        public:
            using TRaw = ui16;

            TVectorType()
                : Vec(0)
                , Size(0)
            {}

            TVectorType(TRaw vec, ui8 size)
                : Vec(vec & ValidMask(size))
                , Size(size)
            {}

            TVectorType(const TVectorType &v)
                : Vec(v.Vec)
                , Size(v.Size)
            {}

            void Set(ui8 i) {
                Y_ABORT_UNLESS(i < Size);
                Vec |= BitMask(i);
            }

            void Clear(ui8 i) {
                Y_ABORT_UNLESS(i < Size);
                Vec &= ~BitMask(i);
            }

            void Clear() {
                Vec = 0;
            }

            bool Get(ui8 i) const {
                Y_ABORT_UNLESS(i < Size);
                return Vec & BitMask(i);
            }

            ui8 BitsBefore(ui8 i) const {
                Y_ABORT_UNLESS(i <= Size);
                return ::std::popcount(TRaw(Vec & ValidMask(i)));
            }

            ui8 FirstPosition() const {
                return FirstSetBit(Vec);
            }

            ui8 NextPosition(ui8 i) const {
                Y_ABORT_UNLESS(i < Size);
                return FirstSetBit(Vec & ~ValidMask(i + 1));
            }

            ui8 CountBits() const {
                return ::std::popcount(Vec);
            }

            TRaw Raw() const {
                return Vec;
            }

            ui8 Raw8() const {
                // The size is part of this boundary: even an empty wide vector
                // cannot be represented by the legacy one-byte disk header.
                Y_ABORT_UNLESS(Size <= 8);
                return Vec;
            }

            bool Empty() const {
                return Vec == 0;
            }

            bool IsSupersetOf(NMatrix::TVectorType v) const {
                Y_DEBUG_ABORT_UNLESS(Size == v.Size);
                return (Vec & v.Vec) == v.Vec;
            }

            TVectorType &operator =(const TVectorType &v) {
                Y_DEBUG_ABORT_UNLESS(Size == 0 || Size == v.Size || v.Size == 0);
                Size = v.Size;
                Vec = v.Vec;
                return *this;
            }

            TVectorType &operator |=(const TVectorType &v) {
                Y_DEBUG_ABORT_UNLESS(Size == v.Size);
                Vec |= v.Vec;
                return *this;
            }

            TVectorType &operator &=(const TVectorType &v) {
                Y_DEBUG_ABORT_UNLESS(Size == v.Size);
                Vec &= v.Vec;
                return *this;
            }

            TVectorType &operator -=(const TVectorType &v) {
                Y_DEBUG_ABORT_UNLESS(Size == v.Size);
                Vec &= ~v.Vec;
                return *this;
            }

            TVectorType operator ~() const {
                return TVectorType(~Vec, Size);
            }

            bool operator ==(const TVectorType &v) const {
                Y_DEBUG_ABORT_UNLESS(v.Size == Size);
                return Vec == v.Vec;
            }

            bool operator !=(const TVectorType &v) const {
                return !operator ==(v);
            }

            TString ToString() const {
                TStringStream s;
                for (ui8 i = 0; i < Size; i++) {
                    if (i) {
                        s << " ";
                    }
                    s << (Get(i) ? "1" : "0");
                }
                return s.Str();
            }

            void DebugPrint() const {
                fprintf(stderr, "%s\n", ToString().data());
            }

            inline ui8 GetSize() const {
                return Size;
            }

            void Swap(TVectorType &v) {
                DoSwap(Vec, v.Vec);
                DoSwap(Size, v.Size);
            }

            static TVectorType MakeOneHot(ui8 pos, ui8 size) {
                TVectorType res(0, size);
                res.Set(pos);
                return res;
            }

            class TIterator {
                const TVectorType& Parts;
                ui8 Index;

            public:
                TIterator(const TVectorType& parts, bool end)
                    : Parts(parts)
                    , Index(end ? Parts.GetSize() : Parts.FirstPosition())
                {}

                friend bool operator ==(const TIterator& x, const TIterator& y) {
                    Y_DEBUG_ABORT_UNLESS(&x.Parts == &y.Parts);
                    return x.Index == y.Index;
                }

                ui8 operator *() const {
                    return Index;
                }

                TIterator& operator ++() {
                    Index = Parts.NextPosition(Index);
                    return *this;
                }
            };

            TIterator begin() const { return {*this, false}; }
            TIterator end() const { return {*this, true}; }

        private:
            TRaw Vec;
            ui8 Size;

            // Keep old raw masks in the low byte. Logical parts 8..15 occupy
            // the high byte with the same most-significant-bit-first order.
            static TRaw BitMask(ui8 i) {
                return (0x80u >> (i & 7)) << (i & 8);
            }

            static TRaw SwapBytes(TRaw vec) {
                return (vec << 8) | (vec >> 8);
            }

            static TRaw ValidMask(ui8 size) {
                Y_ABORT_UNLESS(size <= 16);
                return SwapBytes(TRaw(0xffffu << (16 - size)));
            }

            friend TVectorType operator -(const TVectorType &v1, const TVectorType &v2);
            friend TVectorType operator &(const TVectorType &v1, const TVectorType &v2);
            friend TVectorType operator |(const TVectorType &v1, const TVectorType &v2);

            ui8 FirstSetBit(TRaw vec) const {
                return vec ? std::countl_zero(SwapBytes(vec)) : Size;
            }
        };

        inline TVectorType operator -(const TVectorType &v1, const TVectorType &v2) {
            Y_DEBUG_ABORT_UNLESS(v1.Size == v2.Size);
            return TVectorType(v1.Vec ^ (v1.Vec & v2.Vec), v1.Size);
        }

        inline TVectorType operator &(const TVectorType &v1, const TVectorType &v2) {
            Y_DEBUG_ABORT_UNLESS(v1.Size == v2.Size);
            return TVectorType(v1.Vec & v2.Vec, v1.Size);
        }

        inline TVectorType operator |(const TVectorType &v1, const TVectorType &v2) {
            Y_DEBUG_ABORT_UNLESS(v1.Size == v2.Size);
            return TVectorType(v1.Vec | v2.Vec, v1.Size);
        }



        //////////////////////////////////////////////////////////////////////////////////
        // TShiftedBitVecBase
        //////////////////////////////////////////////////////////////////////////////////
        class TShiftedBitVecBase {
        public:
            TShiftedBitVecBase()
                : Ptr(nullptr)
                , Beg(0)
                , End(0)
            {}

            TShiftedBitVecBase(ui8 *ptr, ui8 b, ui8 e)
                : Ptr(ptr)
                , Beg(b)
                , End(e)
            {
                Y_DEBUG_ABORT_UNLESS(End > Beg);
            }

            void Set(ui8 i) {
                ui8 byte = 0, mask = 0;
                CalcPos(byte, mask, i);
                Ptr[byte] |= mask;
            }

            void Clear(ui8 i) {
                ui8 byte = 0, mask = 0;
                CalcPos(byte, mask, i);
                Ptr[byte] &= ~mask;
            }

            bool Get(ui8 i) const {
                ui8 byte = 0, mask = 0;
                CalcPos(byte, mask, i);
                return Ptr[byte] & mask;
            }

            ////////////// TIterator //////////////////////
            class TIterator {
            public:
                TIterator(ui8 *ptr, ui8 b, ui8 e) {
                    ui8 fullByteBits = b >> 3 << 3;
                    Ptr = ptr + (fullByteBits >> 3);
                    Pos = b - fullByteBits;
                    End = e - fullByteBits;
                    Mask = 0x80 >> Pos;
                }

                bool Get() const {
                    return *Ptr & Mask;
                }

                bool IsEnd() const {
                    return Pos == End;
                }

                void Next() {
                    Y_DEBUG_ABORT_UNLESS(!IsEnd());
                    Pos++;
                    Mask >>= 1;
                    if (!Mask) {
                        Ptr++;
                        Mask = 0x80;
                    }
                }

            private:
                ui8 *Ptr;
                ui8 Pos;
                ui8 End;
                ui8 Mask;
            };

            TIterator Begin() {
                return TIterator(Ptr, Beg, End);
            }

            TIterator Begin() const {
                return TIterator(Ptr, Beg, End);
            }


        protected:
            ui8 *Ptr;
            ui8 Beg;
            ui8 End;

            void CalcPos(ui8 &byte, ui8 &mask, ui8 i) const {
                Y_DEBUG_ABORT_UNLESS(i < (End - Beg));
                ui8 bitPos = Beg + i;
                byte = bitPos >> 3;
                ui8 bit = bitPos - (byte << 3);
                mask = 0x80 >> bit;
            }
        };


        //////////////////////////////////////////////////////////////////////////////////
        // TShiftedMainBitVec
        //////////////////////////////////////////////////////////////////////////////////
        class TShiftedMainBitVec : protected TShiftedBitVecBase {
        public:
            TShiftedMainBitVec(ui8 *ptr, ui8 b, ui8 e)
                : TShiftedBitVecBase(ptr, b, e)
            {}

            void Set(ui8 i) {
                TShiftedBitVecBase::Set(i);
            }

            void Clear(ui8 i) {
                TShiftedBitVecBase::Clear(i);
            }

            bool Get(ui8 i) const {
                return TShiftedBitVecBase::Get(i);
            }

            TVectorType ToVector() const {
                TVectorType vec(0, End - Beg);
                TIterator it = Begin();
                ui8 pos = 0;
                while (!it.IsEnd()) {
                    if (it.Get()) {
                        vec.Set(pos);
                    }
                    ++pos;
                    it.Next();
                }
                return vec;
            }
        };



        //////////////////////////////////////////////////////////////////////////////////
        // TShiftedHandoffBitVec
        //////////////////////////////////////////////////////////////////////////////////
        class TShiftedHandoffBitVec : protected TShiftedBitVecBase {
        public:
            TShiftedHandoffBitVec()
                : TShiftedBitVecBase()
            {}

            TShiftedHandoffBitVec(ui8 *ptr, ui8 b, ui8 e)
                : TShiftedBitVecBase(ptr, b, e)
            {
                Y_DEBUG_ABORT_UNLESS(((End - Beg) >> 1 << 1) == (End - Beg));
            }

            // 00 -- not set
            // 01 -- set
            // 10 -- already not set (and we get inconsistency here)
            // 11 -- already not set

            void Set(ui8 i) {
                TShiftedBitVecBase::Set((i << 1) + 1);
            }

            void Delete(ui8 i) {
                TShiftedBitVecBase::Set(i << 1);
            }

            bool Get(ui8 i) const {
                bool firstBit = TShiftedBitVecBase::Get(i << 1);
                bool secondBit = TShiftedBitVecBase::Get((i << 1) + 1);
                return !firstBit && secondBit;
            }

            bool NotSet() const {
                return ToVector().Empty();
            }

            ui8 GetRaw(ui8 i) const {
                ui8 res = ui8(TShiftedBitVecBase::Get(i << 1));
                res <<= 1;
                res |= ui8(TShiftedBitVecBase::Get((i << 1) + 1));
                return res;
            }

            TVectorType DeletedPartsVector() const {
                TVectorType vec(0, (End - Beg) / 2);
                for (ui8 i = 0; i < vec.GetSize(); ++i) {
                    if (TShiftedBitVecBase::Get(i * 2)) {
                        vec.Set(i);
                    }
                }
                return vec;
            }

            TVectorType ToVector() const {
                TVectorType vec(0, (End - Beg) / 2);
                for (ui8 i = 0; i < vec.GetSize(); ++i) {
                    if (Get(i)) {
                        vec.Set(i);
                    }
                }
                return vec;
            }
        };

        //////////////////////////////////////////////////////////////////////////////////
        // TMatrix
        //////////////////////////////////////////////////////////////////////////////////
        class TMatrix {
        public:
            TMatrix(ui8 *ptr, ui8 rows, ui8 columns)
                : Ptr(ptr)
                , Rows(rows)
                , Columns(columns)
            {
                Y_DEBUG_ABORT_UNLESS(ptr && rows && columns);
            }

            void Zero() {
                ui32 bits = (ui32)Rows * (ui32)Columns;
                ui32 e = bits / BitsInByte;
                ui32 bitsRest = bits - e * BitsInByte;
                ui32 i = 0;
                while (i < e)
                    Ptr[i++] = 0;
                if (bitsRest)
                    Ptr[i] &= (0xFF >> bitsRest);
            }


            TVectorType GetRow(ui8 row) const {
                TVectorType vec(0, Columns);
                for (ui8 i = 0; i < Columns; i++) {
                    if (Get(row, i))
                        vec.Set(i);
                }
                return vec;
            }

            TVectorType GetColumn(ui8 column) const {
                TVectorType vec(0, Rows);
                for (ui8 i = 0; i < Rows; i++) {
                    if (Get(i, column))
                        vec.Set(i);
                }
                return vec;
            }

            void Set(ui8 row, ui8 column) {
                TPos pos = CalcPos(row, column);
                Ptr[pos.Byte] |= pos.Mask;
            }

            bool Get(ui8 row, ui8 column) const {
                TPos pos = CalcPos(row, column);
                return Ptr[pos.Byte] & pos.Mask;
            }

            void Clear(ui8 row, ui8 column) {
                TPos pos = CalcPos(row, column);
                Ptr[pos.Byte] &= ~pos.Mask;
            }

            TVectorType OrRows() const {
                TVectorType vec(0, Columns);
                for (ui8 i = 0; i < Rows; i++) {
                    vec |= GetRow(i);
                }
                return vec;
            }

            TVectorType OrColumns() const {
                TVectorType vec(0, Rows);
                for (ui8 i = 0; i < Columns; i++) {
                    vec |= GetColumn(i);
                }
                return vec;
            }

            void DebugPrint() {
                for (ui8 i = 0; i < Rows; i++) {
                    for (ui8 j = 0; j < Columns; j++) {
                        fprintf(stderr, "%u ", !!Get(i, j));
                    }
                    fprintf(stderr, "\n");
                }
            }

        private:
            struct TPos {
                ui8 Byte;
                ui8 Mask;

                TPos(ui8 byte, ui8 mask)
                    : Byte(byte)
                    , Mask(mask)
                {}
            };

            TPos CalcPos(ui8 row, ui8 column) const {
                Y_DEBUG_ABORT_UNLESS(row < Rows && column < Columns);
                ui32 bit = (ui32)Columns * (ui32)row + (ui32)column;
                ui32 byte = bit / BitsInByte;
                ui32 localBit = byte * BitsInByte + 7 - bit;
                ui8 mask = 1 << localBit;
                return TPos(byte, mask);
            }

            ui8 *Ptr;
            ui8 Rows;
            ui8 Columns;
        };

    } // NMatrix

} // NKikimr
