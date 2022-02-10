#include "bitinput.h"
#include "bitoutput.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/buffer.h>
#include <util/generic/buffer.h>

namespace NBitIO {
    static const char BITS_REF[] =
        "00100010 01000000 00000000 00100111 11011111 01100111 11010101 00010100 "
        "00100010 01100011 11100011 00110000 11011011 11011111 01001100 00110101 "
        "10011110 01011111 01010000 00000110 00011011 00100110 00010100 01110011 "
        "00001010 10101010 10101010 10101010 10101010 10101010 10101010 10101010 "
        "10110101 01010101 01010101 01010101 01010101 01010101 01010101 01010101 "
        "01000000";

    inline ui64 Bits(ui64 bytes) {
        return bytes << 3ULL;
    }

    inline TString PrintBits(const char* a, const char* b, bool reverse = false) {
        TString s;
        TStringOutput out(s);
        for (const char* it = a; it != b; ++it) {
            if (it != a)
                out << ' ';

            ui8 byte = *it;

            if (reverse)
                byte = ReverseBits(byte);

            for (ui32 mask = 1; mask < 0xff; mask <<= 1) {
                out << ((byte & mask) ? '1' : '0');
            }
        }

        return s;
    }

    template <typename T>
    inline TString PrintBits(T t, ui32 bits = Bits(sizeof(T))) {
        return PrintBits((char*)&t, ((char*)&t) + BytesUp(bits));
    }
}

class TBitIOTest: public TTestBase {
    UNIT_TEST_SUITE(TBitIOTest);
    UNIT_TEST(TestBitIO)
    UNIT_TEST_SUITE_END();

private:
    using TBi = NBitIO::TBitInput;
    using TVec = TVector<char>;

    void static CheckBits(const TVec& v, const TString& ref, const TString& rem) {
        UNIT_ASSERT_VALUES_EQUAL_C(NBitIO::PrintBits(v.begin(), v.end()), ref, rem);
    }

    void DoRead(TBi& b, ui32& t) {
        b.Read(t, 1, 0);   // 1
        b.ReadK<3>(t, 1);  // 4
        b.Read(t, 5, 4);   // 9
        b.ReadK<14>(t, 9); // 23
        b.Read(t, 1, 23);  // 24
        b.ReadK<5>(t, 24); // 29
        b.Read(t, 3, 29);  // 32
    }

    template <typename TBo>
    void DoWrite(TBo& b, ui32 t) {
        b.Write(t, 1, 0);  //1
        b.Write(t, 3, 1);  //4
        b.Write(t, 5, 4);  //9
        b.Write(t, 14, 9); //23
        b.Write(t, 1, 23); //24
        b.Write(t, 5, 24); //29
        b.Write(t, 3, 29); //32
    }

    template <typename TBo>
    void DoWrite1(TBo& out, const TString& rem) {
        out.Write(0x0C, 3);
        UNIT_ASSERT_VALUES_EQUAL_C(out.GetOffset(), 1u, (rem + ", " + ToString(__LINE__)));
        out.Write(0x18, 4);
        UNIT_ASSERT_VALUES_EQUAL_C(out.GetOffset(), 1u, (rem + ", " + ToString(__LINE__)));
        out.Write(0x0C, 3);
        UNIT_ASSERT_VALUES_EQUAL_C(out.GetOffset(), 2u, (rem + ", " + ToString(__LINE__)));
        out.Write(0x30000, 17);
        UNIT_ASSERT_VALUES_EQUAL_C(out.GetOffset(), 4u, (rem + ", " + ToString(__LINE__)));
        out.Write(0x0C, 3);
        UNIT_ASSERT_VALUES_EQUAL_C(out.GetOffset(), 4u, (rem + ", " + ToString(__LINE__)));
    }

    template <typename TBo>
    void DoWrite2(TBo& out, const TString& rem) {
        out.Write(0x0C, 3);
        UNIT_ASSERT_VALUES_EQUAL_C(out.GetOffset(), 8u, (rem + ", " + ToString(__LINE__)));

        out.Write(0x42, 7);
        UNIT_ASSERT_VALUES_EQUAL_C(out.GetOffset(), 9u, (rem + ", " + ToString(__LINE__)));

        DoWrite(out, 1637415112);
        UNIT_ASSERT_VALUES_EQUAL_C(out.GetOffset(), 13u, (rem + ", " + ToString(__LINE__)));

        DoWrite(out, 897998715);
        UNIT_ASSERT_VALUES_EQUAL_C(out.GetOffset(), 17u, (rem + ", " + ToString(__LINE__)));

        DoWrite(out, 201416527);
        UNIT_ASSERT_VALUES_EQUAL_C(out.GetOffset(), 21u, (rem + ", " + ToString(__LINE__)));

        DoWrite(out, 432344219);
        UNIT_ASSERT_VALUES_EQUAL_C(out.GetOffset(), 25u, (rem + ", " + ToString(__LINE__)));

        out.Write(0xAAAAAAAAAAAAAAAAULL, 64);
        UNIT_ASSERT_VALUES_EQUAL_C(out.GetOffset(), 33u, (rem + ", " + ToString(__LINE__)));

        out.Write(0x5555555555555555ULL, 64);
        UNIT_ASSERT_VALUES_EQUAL_C(out.GetOffset(), 41u, (rem + ", " + ToString(__LINE__)));
    }

    void DoBitOutput(NBitIO::TBitOutputYVector& out, const TString& rem) {
        DoWrite1(out, rem);

        out.WriteWords<8>(0xabcdef);
        UNIT_ASSERT_VALUES_EQUAL_C(out.GetOffset(), 8u, (rem + ", " + ToString(__LINE__)));

        DoWrite2(out, rem);
    }

    void DoBitOutput(NBitIO::TBitOutputArray& out, const TString& rem) {
        DoWrite1(out, rem);

        out.WriteWords<8>(0xabcdef);
        UNIT_ASSERT_VALUES_EQUAL_C(out.GetOffset(), 8u, (rem + ", " + ToString(__LINE__)));

        DoWrite2(out, rem);
    }

    void DoBitInput(TBi& in, const TString& rem) {
        UNIT_ASSERT(!in.Eof());

        {
            ui64 val;

            val = 0;
            UNIT_ASSERT_VALUES_EQUAL_C(in.GetOffset(), 0u, (rem + ": " + NBitIO::PrintBits(val)));

            UNIT_ASSERT_C(in.Read(val, 3), (rem + ": " + NBitIO::PrintBits(val)).data());

            UNIT_ASSERT_VALUES_EQUAL_C(val, 0x4u, (rem + ": " + NBitIO::PrintBits(val)));
            UNIT_ASSERT_VALUES_EQUAL_C(in.GetOffset(), 1u, (rem + ": " + NBitIO::PrintBits(val)));
            UNIT_ASSERT_C(!in.Eof(), (rem + ", " + ToString(__LINE__)).data());

            val = 0;
            UNIT_ASSERT_C(in.Read(val, 4), (rem + ": " + NBitIO::PrintBits(val)).data());

            UNIT_ASSERT_VALUES_EQUAL_C(val, 0x8u, (rem + ": " + NBitIO::PrintBits(val)));
            UNIT_ASSERT_VALUES_EQUAL_C(in.GetOffset(), 1u, (rem + ": " + NBitIO::PrintBits(val)));
            UNIT_ASSERT_C(!in.Eof(), (rem + ", " + ToString(__LINE__)).data());

            val = 0;
            UNIT_ASSERT_C(in.Read(val, 3), (rem + ": " + NBitIO::PrintBits(val)).data());

            UNIT_ASSERT_VALUES_EQUAL_C(val, 0x4u, (rem + ": " + NBitIO::PrintBits(val)));
            UNIT_ASSERT_VALUES_EQUAL_C(in.GetOffset(), 2u, (rem + ": " + NBitIO::PrintBits(val)));
            UNIT_ASSERT_C(!in.Eof(), (rem + ", " + ToString(__LINE__)).data());

            val = 0;
            UNIT_ASSERT_C(in.Read(val, 17), (rem + ": " + NBitIO::PrintBits(val)).data());

            UNIT_ASSERT_VALUES_EQUAL_C(val, 0x10000u, (rem + ": " + NBitIO::PrintBits(val)));
            UNIT_ASSERT_VALUES_EQUAL_C(in.GetOffset(), 4u, (rem + ": " + NBitIO::PrintBits(val)));
            UNIT_ASSERT_C(!in.Eof(), (rem + ", " + ToString(__LINE__)).data());

            UNIT_ASSERT_VALUES_EQUAL_C(in.GetOffset(), 4u, (rem + ": " + NBitIO::PrintBits(val)));

            {
                ui32 rt = 0;
                in.ReadRandom(30, rt, 10, 20);
                UNIT_ASSERT_STRINGS_EQUAL(NBitIO::PrintBits(rt).data(), "00000000 00000000 00001111 01111100");
            }
            val = 0;
            UNIT_ASSERT_C(in.Read(val, 3), (rem + ": " + NBitIO::PrintBits(val)).data());

            UNIT_ASSERT_VALUES_EQUAL_C(val, 0x4u, (rem + ": " + NBitIO::PrintBits(val)));
            UNIT_ASSERT_VALUES_EQUAL_C(in.GetOffset(), 4u, (rem + ": " + NBitIO::PrintBits(val)));
            UNIT_ASSERT_C(!in.Eof(), (rem + ", " + ToString(__LINE__)).data());

            val = 0;
            UNIT_ASSERT_C(in.ReadWords<8>(val), (rem + ": " + NBitIO::PrintBits(val)).data());

            UNIT_ASSERT_VALUES_EQUAL_C(val, 0xabcdefU, (rem + ": " + NBitIO::PrintBits(val)));
            UNIT_ASSERT_VALUES_EQUAL_C(in.GetOffset(), 8u, (rem + ": " + NBitIO::PrintBits(val)));
            UNIT_ASSERT_C(!in.Eof(), (rem + ", " + ToString(__LINE__)).data());

            val = 0;
            UNIT_ASSERT_C(in.Read(val, 3), (rem + ", " + ToString(__LINE__)).data());

            UNIT_ASSERT_VALUES_EQUAL_C(val, 0x4u, (rem + ": " + NBitIO::PrintBits(val)));
            UNIT_ASSERT_VALUES_EQUAL_C(in.GetOffset(), 8u, (rem + ": " + NBitIO::PrintBits(val)));
            UNIT_ASSERT_C(!in.Eof(), (rem + ", " + ToString(__LINE__)).data());

            val = 0;
            in.Read(val, 7);
            UNIT_ASSERT_VALUES_EQUAL_C(val, 0x42u, (rem + ": " + NBitIO::PrintBits(val)));
        }

        {
            ui32 v = 0;

            DoRead(in, v);
            UNIT_ASSERT_VALUES_EQUAL_C(v, 1637415112ul, (rem + ": " + NBitIO::PrintBits(v)));
            DoRead(in, v);
            UNIT_ASSERT_VALUES_EQUAL_C(v, 897998715u, (rem + ": " + NBitIO::PrintBits(v)));
            DoRead(in, v);
            UNIT_ASSERT_VALUES_EQUAL_C(v, 201416527u, (rem + ": " + NBitIO::PrintBits(v)));
            DoRead(in, v);
            UNIT_ASSERT_VALUES_EQUAL_C(v, 432344219u, (rem + ": " + NBitIO::PrintBits(v)));

            UNIT_ASSERT_VALUES_EQUAL_C(in.GetOffset(), 25u, (rem + ": " + NBitIO::PrintBits(v)));
        }

        {
            ui64 v8 = 0;
            in.ReadSafe(v8, 64);

            UNIT_ASSERT_VALUES_EQUAL_C(v8, 0xAAAAAAAAAAAAAAAAULL, (rem + ": " + NBitIO::PrintBits(v8)));
            UNIT_ASSERT_VALUES_EQUAL_C(in.GetOffset(), 33u, (rem + ": " + NBitIO::PrintBits(v8)));

            v8 = 0;
            in.ReadK<64>(v8);

            UNIT_ASSERT_VALUES_EQUAL_C(v8, 0x5555555555555555ULL, (rem + ": " + NBitIO::PrintBits(v8)));
            UNIT_ASSERT_VALUES_EQUAL_C(in.GetOffset(), 41u, (rem + ": " + NBitIO::PrintBits(v8)));
        }

        ui32 v = 0;
        UNIT_ASSERT_C(!in.Eof(), (rem + ", " + ToString(__LINE__)).data());
        UNIT_ASSERT_C(in.Read(v, 5), (rem + ", " + ToString(__LINE__)).data());
        UNIT_ASSERT_C(in.Eof(), (rem + ", " + ToString(__LINE__)).data());
    }

    void TestBitIO() {
        {
            TVec vec;

            {
                NBitIO::TBitOutputYVector out(&vec);
                DoBitOutput(out, ToString(__LINE__));
            }

            CheckBits(vec, NBitIO::BITS_REF, ToString(__LINE__).data());

            {
                TBi in(vec);
                DoBitInput(in, ToString(__LINE__));
            }
        }
        {
            TVec vec;
            vec.resize(41, 0);
            {
                NBitIO::TBitOutputArray out(vec.begin(), vec.size());
                DoBitOutput(out, ToString(__LINE__));
            }

            CheckBits(vec, NBitIO::BITS_REF, ToString(__LINE__).data());

            {
                TBi in(vec);
                DoBitInput(in, ToString(__LINE__));
            }
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TBitIOTest);
