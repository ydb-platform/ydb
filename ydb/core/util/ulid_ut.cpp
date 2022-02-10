#include "ulid.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TULID) {

    Y_UNIT_TEST(ParseAndFormat) {
        TULID id1, id2, id3;

        UNIT_ASSERT(id1.ParseString("0123456789abcdefghjkmnpqrs"));
        UNIT_ASSERT_VALUES_EQUAL(id1.ToString(), "0123456789abcdefghjkmnpqrs");
        UNIT_ASSERT(id2.ParseString("6789ABCDEFGHJKMNPQRSTVWXYZ"));
        UNIT_ASSERT_VALUES_EQUAL(id2.ToString(), "6789abcdefghjkmnpqrstvwxyz");
        UNIT_ASSERT(id3.ParseString("7ZYXWVTSRQPNMKJHGFEDCBA987"));
        UNIT_ASSERT_VALUES_EQUAL(id3.ToString(), "7zyxwvtsrqpnmkjhgfedcba987");

        TULID idMin, idMax;
        UNIT_ASSERT(idMin.ParseString("00000000000000000000000000"));
        UNIT_ASSERT_VALUES_EQUAL(idMin.ToString(), "00000000000000000000000000");
        UNIT_ASSERT(idMax.ParseString("7zzzzzzzzzzzzzzzzzzzzzzzzz"));
        UNIT_ASSERT_VALUES_EQUAL(idMax.ToString(), "7zzzzzzzzzzzzzzzzzzzzzzzzz");
        UNIT_ASSERT(idMin == TULID::Min());
        UNIT_ASSERT(idMax == TULID::Max());

        TULID idError;

        // Shorter or larger ids are not allowed
        UNIT_ASSERT(!idError.ParseString("0123456789abcdefghjkmnpqr"));
        UNIT_ASSERT(!idError.ParseString("0123456789abcdefghjkmnpqrst"));

        // Letter i/I is not allowed
        UNIT_ASSERT(!idError.ParseString("0i23456789abcdefghjkmnpqrs"));
        UNIT_ASSERT(!idError.ParseString("0I23456789abcdefghjkmnpqrs"));

        // Letter l/L is not allowed
        UNIT_ASSERT(!idError.ParseString("0l23456789abcdefghjkmnpqrs"));
        UNIT_ASSERT(!idError.ParseString("0L23456789abcdefghjkmnpqrs"));

        // Letter o/O is not allowed
        UNIT_ASSERT(!idError.ParseString("o123456789abcdefghjkmnpqrs"));
        UNIT_ASSERT(!idError.ParseString("O123456789abcdefghjkmnpqrs"));

        // Letter u/U is not allowed
        UNIT_ASSERT(!idError.ParseString("6789abcdefghjkmnpqrstuwxyz"));
        UNIT_ASSERT(!idError.ParseString("6789abcdefghjkmnpqrstUwxyz"));

        TULID idKnown;

        idKnown.SetTimeMs(1614769211827);
        idKnown.SetRandomHi(5210);
        idKnown.SetRandomLo(5599683843616075464ull);
        UNIT_ASSERT_VALUES_EQUAL(idKnown.ToString(), "01ezvvxjdk2hd4vdgjs68knvp8");

        TULID idKnownParsed;
        UNIT_ASSERT(idKnownParsed.ParseString("01ezvvxjdk2hd4vdgjs68knvp8"));
        UNIT_ASSERT(idKnown == idKnownParsed);
    }

    Y_UNIT_TEST(HeadByteOrder) {
        TULID current = TULID::Min();
        for (int i = 0; i < 255; ++i) {
            TULID next = current;
            next.Data[0]++;
            UNIT_ASSERT_C(current.ToString() < next.ToString(),
                "Expected " << current.ToString() << " < " << next.ToString());
            TULID copy;
            UNIT_ASSERT(copy.ParseString(next.ToString()));
            UNIT_ASSERT(copy == next);
            current = next;
        }
    }

    Y_UNIT_TEST(TailByteOrder) {
        TULID current = TULID::Min();
        for (int i = 0; i < 255; ++i) {
            TULID next = current;
            next.Data[15]++;
            UNIT_ASSERT_C(current.ToString() < next.ToString(),
                "Expected " << current.ToString() << " < " << next.ToString());
            TULID copy;
            UNIT_ASSERT(copy.ParseString(next.ToString()));
            UNIT_ASSERT(copy == next);
            current = next;
        }
    }

    Y_UNIT_TEST(EveryBitOrder) {
        TULID last = TULID::Min();
        for (int i = 15; i >= 0; --i) {
            for (int j = 0; j < 8; ++j) {
                TULID current = TULID::Min();
                current.Data[i] |= ui8(1 << j);
                UNIT_ASSERT_C(last.ToString() < current.ToString(),
                    "Expected " << last.ToString() << " < " << current.ToString());
                TULID copy;
                UNIT_ASSERT(copy.ParseString(current.ToString()));
                UNIT_ASSERT(copy == current);
                last = current;
            }
        }
    }

    Y_UNIT_TEST(Generate) {
        TULIDGenerator gen;

        auto id1 = gen.Next();
        auto id2 = gen.Next();
        UNIT_ASSERT_C(id1 < id2,
            "Generated non-increasing " << id1.ToString() << " then " << id2.ToString());

        auto id3 = gen.Next(id2.GetTime());
        UNIT_ASSERT_C(id2 < id3,
            "Generated non-increasing " << id2.ToString() << " then " << id3.ToString());
        UNIT_ASSERT_C(id2.GetTime() == id3.GetTime(),
            "Generated different ts " << id2.ToString() << " then " << id3.ToString());
    }

}

} // namespace NKikimr
