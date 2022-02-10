#include "circlebufstream.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/ptr.h>
#include <util/stream/null.h>

#define STR Cnull

Y_UNIT_TEST_SUITE(TCircleBufStringStreamTest) {

    Y_UNIT_TEST(TestAligned) {
        TCircleBufStringStream<32> str;
        UNIT_ASSERT_EQUAL(str.Str(), TString()); // empty string

        TString hexDigits("0123456789ABCDEF");

        str << hexDigits;
        STR << str.Str() << "\n";
        UNIT_ASSERT_EQUAL(str.Str(), hexDigits);


        str << hexDigits;
        STR << str.Str() << "\n";
        UNIT_ASSERT_EQUAL(str.Str(), CircleBufStringStreamSkipPrefix + hexDigits + hexDigits);

        for (int i = 0; i < 10; ++i) {
            str << "0123456789ABCDEF";
        }
        UNIT_ASSERT_EQUAL(str.Str(), CircleBufStringStreamSkipPrefix + hexDigits + hexDigits);
    }

    Y_UNIT_TEST(TestNotAligned) {
        TCircleBufStringStream<32> str;
        UNIT_ASSERT_EQUAL(str.Str(), TString()); // empty string

        TString hexDigitsPlusGarbage("0123456789ABCDEF__");

        str << hexDigitsPlusGarbage;
        STR << str.Str() << "\n" << str.ToString() << "\n";
        UNIT_ASSERT_EQUAL(str.Str(), hexDigitsPlusGarbage);


        str << hexDigitsPlusGarbage;
        STR << str.Str() << "\n" << str.ToString() << "\n";
        UNIT_ASSERT_EQUAL(str.Str(),
                          CircleBufStringStreamSkipPrefix +
                            TString("456789ABCDEF__0123456789ABCDEF__"));

        // not aligned
        for (int i = 0; i < 14; ++i) {
            str << hexDigitsPlusGarbage;
        }
        UNIT_ASSERT_EQUAL(str.Str(),
                          CircleBufStringStreamSkipPrefix +
                          TString("456789ABCDEF__0123456789ABCDEF__"));
    }

    Y_UNIT_TEST(TestOverflow) {
        TCircleBufStringStream<16> str;
        UNIT_ASSERT_EQUAL(str.Str(), TString()); // empty string

        str << "0123456789ABCDEF__";
        STR << str.Str() << "\n" << str.ToString() << "\n";
        UNIT_ASSERT_EQUAL(str.Str(),
                          CircleBufStringStreamSkipPrefix + TString("23456789ABCDEF__"));
    }
}
