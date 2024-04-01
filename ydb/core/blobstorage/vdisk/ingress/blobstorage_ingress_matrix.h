#pragma once

#include <ydb/core/util/bits.h>

#include <library/cpp/pop_count/popcount.h>

#include <util/stream/str.h>
#include <util/string/cast.h>

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
            TVectorType()
                : Vec(0)
                , Size(0)
            {}

            TVectorType(ui8 vec, ui8 size)
                : Vec(vec & EmptyMask[size])
                , Size(size)
            {
                Y_DEBUG_ABORT_UNLESS(size <= 8);
            }

            TVectorType(const TVectorType &v)
                : Vec(v.Vec)
                , Size(v.Size)
            {}

            void Set(ui8 i) {
                Y_DEBUG_ABORT_UNLESS(i < Size);
                ui8 mask = 0x80 >> i;
                Vec |= mask;
            }

            void Clear(ui8 i) {
                Y_DEBUG_ABORT_UNLESS(i < Size);
                ui8 mask = 0x80 >> i;
                Vec &= ~mask;
            }

            void Clear() {
                Vec = 0;
            }

            bool Get(ui8 i) const {
                Y_DEBUG_ABORT_UNLESS(i < Size);
                ui8 mask = 0x80 >> i;
                return Vec & mask;
            }

            ui8 BitsBefore(ui8 i) const {
                ui8 shift = 8 - i;
                ui8 mask = 0xFF >> shift << shift;
                unsigned v = Vec & mask;
                return ::PopCount(v);
            }

            ui8 FirstPosition() const {
                return FirstSetBit(Vec);
            }

            ui8 NextPosition(ui8 i) const {
                Y_DEBUG_ABORT_UNLESS(i < 8);
                unsigned shift = 8 - i - 1;
                unsigned mask = unsigned(-1) >> shift << shift;
                unsigned v = (unsigned)Vec & ~mask;
                return FirstSetBit(v);
            }

            ui8 CountBits() const {
                unsigned v = Vec;
                return ::PopCount(v);
            }

            ui8 Raw() const {
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

            TVectorType operator ~() const {
                ui8 v = ~Vec;
                return TVectorType(v, Size);
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
                s << (Get(0) ? "1" : "0");
                for (ui8 i = 1; i < Size; i++)
                    s << " " << (Get(i) ? "1" : "0");
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

        private:
            ui8 Vec;
            ui8 Size;

            const static ui8 EmptyMask[];

            friend TVectorType operator -(const TVectorType &v1, const TVectorType &v2);
            friend TVectorType operator &(const TVectorType &v1, const TVectorType &v2);
            friend TVectorType operator |(const TVectorType &v1, const TVectorType &v2);

            ui8 FirstSetBit(ui8 vec) const {
                const static unsigned shift = std::numeric_limits<unsigned int>::digits - std::numeric_limits<ui8>::digits;
                unsigned mask = unsigned(-1) >> unsigned(Size);
                unsigned t = (unsigned(vec) << shift) | mask;
                Y_DEBUG_ABORT_UNLESS(t != 0);
                return Clz(t);
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
                Y_DEBUG_ABORT_UNLESS(End - Beg <= 8);
                ui8 vec = 0;
                TIterator it = Begin();
                while (!it.IsEnd()) {
                    vec <<= 1;
                    vec |= ui8(it.Get());
                    it.Next();
                }
                vec <<= 8 - (End - Beg);
                return TVectorType(vec, End - Beg);
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
                Y_DEBUG_ABORT_UNLESS(End - Beg <= 2 * 8);
                ui8 vec = 0;
                TIterator it = Begin();
                while (!it.IsEnd()) {
                    bool firstBit = it.Get();
                    vec <<= 1;
                    vec |= ui8(firstBit);

                    it.Next();
                    Y_DEBUG_ABORT_UNLESS(!it.IsEnd());
                    it.Next();
                }
                vec <<= 8 - ((End - Beg) >> 1);
                return TVectorType(vec, (End - Beg) >> 1);
            }

            TVectorType ToVector() const {
                Y_DEBUG_ABORT_UNLESS(End - Beg <= 2 * 8);
                ui8 vec = 0;
                TIterator it = Begin();
                while (!it.IsEnd()) {
                    vec <<= 1;
                    bool firstBit = it.Get();
                    it.Next();
                    Y_DEBUG_ABORT_UNLESS(!it.IsEnd());
                    bool secondBit = it.Get();
                    it.Next();
                    vec |= ui8(!firstBit && secondBit);
                }
                vec <<= 8 - ((End - Beg) >> 1);
                return TVectorType(vec, (End - Beg) >> 1);
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
