#include <ydb/library/kll_median/sketch.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/fwd.h>
#include <util/generic/string.h>

using namespace NKikimr::NKll;

Y_UNIT_TEST_SUITE(TKllMedianTest) {

    Y_UNIT_TEST(EmptyThrowsOnQuantile) {
        TKllSketch<double> sketch(100, 42);
        UNIT_ASSERT_EXCEPTION(sketch.Quantile(0.5), yexception);
    }

    Y_UNIT_TEST(SmallKThrows) {
        UNIT_ASSERT_EXCEPTION(TKllSketch<int>(1, 42), yexception);
    }

    Y_UNIT_TEST(SingleElement) {
        TKllSketch<int> sketch(100, 42);
        sketch.Add(17);
        UNIT_ASSERT_VALUES_EQUAL(sketch.Median(), 17);
        UNIT_ASSERT_VALUES_EQUAL(sketch.Quantile(0.0), 17);
        UNIT_ASSERT_VALUES_EQUAL(sketch.Quantile(1.0), 17);
        UNIT_ASSERT_VALUES_EQUAL(sketch.Count(), 1);
    }

    Y_UNIT_TEST(TwoElements) {
        TKllSketch<int> sketch(100, 42);
        sketch.Add(10);
        sketch.Add(20);
        UNIT_ASSERT_VALUES_EQUAL(sketch.Count(), 2);
        int m = sketch.Median();
        UNIT_ASSERT(m >= 10 && m <= 20);
    }

    Y_UNIT_TEST(DeterministicWithSeed) {
        TKllSketch<int> sketch1(100, 12345);
        TKllSketch<int> sketch2(100, 12345);
        for (int i = 0; i < 1000; ++i) {
            sketch1.Add(i);
            sketch2.Add(i);
        }
        UNIT_ASSERT_VALUES_EQUAL(sketch1.Median(), sketch2.Median());
    }

    Y_UNIT_TEST(QuantileBounds) {
        TKllSketch<double> sketch(100, 42);
        for (int i = 0; i < 100; ++i) {
            sketch.Add(static_cast<double>(i));
        }
        double minVal = sketch.Quantile(0.0);
        double maxVal = sketch.Quantile(1.0);
        UNIT_ASSERT(minVal >= 0 && minVal <= 99);
        UNIT_ASSERT(maxVal >= 0 && maxVal <= 99);
        UNIT_ASSERT(minVal <= sketch.Median());
        UNIT_ASSERT(sketch.Median() <= maxVal);
    }

    Y_UNIT_TEST(StreamingManyElements) {
        TKllSketch<ui64> sketch(50, 42);
        for (ui64 i = 0; i < 10000; ++i) {
            sketch.Add(i);
        }
        UNIT_ASSERT_VALUES_EQUAL(sketch.Count(), 10000);
        ui64 median = sketch.Median();
        // approximate median for 0..9999 should be ~5000
        UNIT_ASSERT(median >= 4000 && median <= 6000);
    }

    Y_UNIT_TEST(LevelsGrow) {
        TKllSketch<int> sketch(4, 42);  // small k to trigger compaction
        for (int i = 0; i < 100; ++i) {
            sketch.Add(i);
        }
        size_t levels = sketch.NumLevels();
        UNIT_ASSERT(levels >= 1);
        UNIT_ASSERT(sketch.Count() == 100);
    }

    Y_UNIT_TEST(StringSingleElement) {
        TKllSketch<TString> sketch(100, 42);
        sketch.Add(TString{"partition-key-42"});
        UNIT_ASSERT_VALUES_EQUAL(sketch.Median(), TString{"partition-key-42"});
        UNIT_ASSERT_VALUES_EQUAL(sketch.Count(), 1);
    }

    Y_UNIT_TEST(StringTwoElementsLexicographic) {
        TKllSketch<TString> sketch(100, 42);
        sketch.Add(TString{"aaa"});
        sketch.Add(TString{"zzz"});
        TString m = sketch.Median();
        UNIT_ASSERT(m >= TString{"aaa"} && m <= TString{"zzz"});
    }

    Y_UNIT_TEST(StringQuantileMinMax) {
        TKllSketch<TString> sketch(100, 42);
        sketch.Add(TString{"b"});
        sketch.Add(TString{"a"});
        sketch.Add(TString{"c"});
        UNIT_ASSERT_VALUES_EQUAL(sketch.Quantile(0.0), TString{"a"});
        UNIT_ASSERT_VALUES_EQUAL(sketch.Quantile(1.0), TString{"c"});
    }

    Y_UNIT_TEST(StringStreamingMany) {
        TKllSketch<TString> sketch(50, 777);
        for (int i = 0; i < 200; ++i) {
            sketch.Add(TStringBuilder() << "key-" << i);
        }
        UNIT_ASSERT_VALUES_EQUAL(sketch.Count(), 200);
        TString median = sketch.Median();
        UNIT_ASSERT(median.StartsWith("key-"));
    }

    Y_UNIT_TEST(StringDeterministicWithSeed) {
        TKllSketch<TString> s1(100, 4242);
        TKllSketch<TString> s2(100, 4242);
        for (int i = 0; i < 50; ++i) {
            s1.Add(TStringBuilder() << "s" << i);
            s2.Add(TStringBuilder() << "s" << i);
        }
        UNIT_ASSERT_VALUES_EQUAL(s1.Median(), s2.Median());
    }

} // Y_UNIT_TEST_SUITE(TKllMedianTest)
