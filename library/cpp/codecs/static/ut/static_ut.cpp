#include <library/cpp/testing/unittest/registar.h> 
#include <library/cpp/codecs/static/example/example.h>

class TStaticCodecUsageTest: public NUnitTest::TTestBase {
    UNIT_TEST_SUITE(TStaticCodecUsageTest)
    UNIT_TEST(TestUsage)
    UNIT_TEST_SUITE_END();

private:
    void DoTestUsage(NStaticCodecExample::EDictVersion dv, size_t expectedSize) {
        const TStringBuf letov = "Всё идёт по плану";

        TBuffer outEnc, outDec;
        NStaticCodecExample::Encode(outEnc, letov, dv);
        NStaticCodecExample::Decode(outDec, TStringBuf{outEnc.data(), outEnc.size()});

        UNIT_ASSERT_VALUES_EQUAL(outEnc.Size(), expectedSize);
        UNIT_ASSERT_EQUAL(TStringBuf(outDec.data(), outDec.size()), letov);
    }

    void TestUsage() {
        DoTestUsage(NStaticCodecExample::DV_HUFF_20160707, 18u);
        DoTestUsage(NStaticCodecExample::DV_SA_HUFF_20160707, 22u);
    }
};

UNIT_TEST_SUITE_REGISTRATION(TStaticCodecUsageTest)
