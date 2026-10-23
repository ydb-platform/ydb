#include "histogram_types.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(THistogramTypesTest)
{
    Y_UNIT_TEST(GetHistogramUsTimeNameTests)
    {
        TRequestUsTimeBuckets histogram;
        const TVector<TString> names = histogram.MakeNames();
        UNIT_ASSERT_STRINGS_EQUAL(names[0], "1");
        UNIT_ASSERT_STRINGS_EQUAL(names[1], "100");
        UNIT_ASSERT_STRINGS_EQUAL(names[2], "200");
        UNIT_ASSERT_STRINGS_EQUAL(names[3], "300");
        UNIT_ASSERT_STRINGS_EQUAL(names[4], "400");
        UNIT_ASSERT_STRINGS_EQUAL(names[5], "500");
        UNIT_ASSERT_STRINGS_EQUAL(names[6], "600");
        UNIT_ASSERT_STRINGS_EQUAL(names[7], "700");
        UNIT_ASSERT_STRINGS_EQUAL(names[8], "800");
        UNIT_ASSERT_STRINGS_EQUAL(names[9], "900");
        UNIT_ASSERT_STRINGS_EQUAL(names[10], "1000");
        UNIT_ASSERT_STRINGS_EQUAL(names[11], "2000");
        UNIT_ASSERT_STRINGS_EQUAL(names[12], "5000");
        UNIT_ASSERT_STRINGS_EQUAL(names[13], "10000");
        UNIT_ASSERT_STRINGS_EQUAL(names[14], "20000");
        UNIT_ASSERT_STRINGS_EQUAL(names[15], "50000");
        UNIT_ASSERT_STRINGS_EQUAL(names[16], "100000");
        UNIT_ASSERT_STRINGS_EQUAL(names[17], "200000");
        UNIT_ASSERT_STRINGS_EQUAL(names[18], "500000");
        UNIT_ASSERT_STRINGS_EQUAL(names[19], "1000000");
        UNIT_ASSERT_STRINGS_EQUAL(names[20], "2000000");
        UNIT_ASSERT_STRINGS_EQUAL(names[21], "5000000");
        UNIT_ASSERT_STRINGS_EQUAL(names[22], "10000000");
        UNIT_ASSERT_STRINGS_EQUAL(names[23], "35000000");
        UNIT_ASSERT_STRINGS_EQUAL(names[24], "Inf");
    }

    Y_UNIT_TEST(GetHistogramMsTimeNameTests)
    {
        TRequestMsTimeBuckets histogram;
        const TVector<TString> names = histogram.MakeNames();
        UNIT_ASSERT_STRINGS_EQUAL(names[0], "0.001ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[1], "0.1ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[2], "0.2ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[3], "0.3ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[4], "0.4ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[5], "0.5ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[6], "0.6ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[7], "0.7ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[8], "0.8ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[9], "0.9ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[10], "1ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[11], "2ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[12], "5ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[13], "10ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[14], "20ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[15], "50ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[16], "100ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[17], "200ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[18], "500ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[19], "1000ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[20], "2000ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[21], "5000ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[22], "10000ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[23], "35000ms");
        UNIT_ASSERT_STRINGS_EQUAL(names[24], "Inf");
    }

    Y_UNIT_TEST(GetHistogramKbSizeNameTests)
    {
        TKbSizeBuckets histogram;
        const TVector<TString> names = histogram.MakeNames();
        UNIT_ASSERT_STRINGS_EQUAL(names[0], "4KB");
        UNIT_ASSERT_STRINGS_EQUAL(names[1], "8KB");
        UNIT_ASSERT_STRINGS_EQUAL(names[2], "16KB");
        UNIT_ASSERT_STRINGS_EQUAL(names[3], "32KB");
        UNIT_ASSERT_STRINGS_EQUAL(names[4], "64KB");
        UNIT_ASSERT_STRINGS_EQUAL(names[5], "128KB");
        UNIT_ASSERT_STRINGS_EQUAL(names[6], "256KB");
        UNIT_ASSERT_STRINGS_EQUAL(names[7], "512KB");
        UNIT_ASSERT_STRINGS_EQUAL(names[8], "1024KB");
        UNIT_ASSERT_STRINGS_EQUAL(names[9], "2048KB");
        UNIT_ASSERT_STRINGS_EQUAL(names[10], "4096KB");
        UNIT_ASSERT_STRINGS_EQUAL(names[11], "Inf");
    }
}

}   // namespace NYdb::NBS
