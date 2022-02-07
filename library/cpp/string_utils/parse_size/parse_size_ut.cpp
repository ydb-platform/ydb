#include "parse_size.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSize;

class TParseSizeTest: public TTestBase {
    UNIT_TEST_SUITE(TParseSizeTest);

    UNIT_TEST(TestPlain);
    UNIT_TEST(TestKiloBytes);
    UNIT_TEST(TestMegaBytes);
    UNIT_TEST(TestGigaBytes);
    UNIT_TEST(TestTeraBytes);
    UNIT_TEST(TestOverflow);
    UNIT_TEST(TestStaticCreators);
    UNIT_TEST(TestToString);

    UNIT_TEST_SUITE_END();

private:
    void TestPlain() {
        UNIT_ASSERT(ParseSize("1024") == 1024);
    }

    void TestKiloBytes() {
        UNIT_ASSERT(ParseSize("10K") == 1024 * 10);
        UNIT_ASSERT(ParseSize("10k") == 1024 * 10);
    }

    void TestMegaBytes() {
        UNIT_ASSERT(ParseSize("10M") == 1024 * 1024 * 10);
        UNIT_ASSERT(ParseSize("10m") == 1024 * 1024 * 10);
    }

    void TestGigaBytes() {
        UNIT_ASSERT(ParseSize("10G") == 1024ul * 1024ul * 1024ul * 10ul);
        UNIT_ASSERT(ParseSize("10g") == 1024ul * 1024ul * 1024ul * 10ul);
    }

    void TestTeraBytes() {
        UNIT_ASSERT(ParseSize("10T") == 1024ul * 1024ul * 1024ul * 1024ul * 10ul);
        UNIT_ASSERT(ParseSize("10t") == 1024ul * 1024ul * 1024ul * 1024ul * 10ul);
    }

    void TestStaticCreators() {
        UNIT_ASSERT_EQUAL(FromKiloBytes(10), 1024ul * 10ul);
        UNIT_ASSERT_EQUAL(FromMegaBytes(10), 1024ul * 1024ul * 10ul);
        UNIT_ASSERT_EQUAL(FromGigaBytes(10), 1024ul * 1024ul * 1024ul * 10ul);
        UNIT_ASSERT_EQUAL(FromTeraBytes(10), 1024ul * 1024ul * 1024ul * 1024ul * 10ul);
    }

    void TestOverflow() {
        UNIT_ASSERT_EXCEPTION(ParseSize("20000000000G"), yexception);
        UNIT_ASSERT_EXCEPTION(FromGigaBytes(20000000000ull), yexception);
    }

    void TestToString() {
        UNIT_ASSERT_VALUES_EQUAL(ToString(FromKiloBytes(1)), TString("1024"));
    }
};

UNIT_TEST_SUITE_REGISTRATION(TParseSizeTest);
