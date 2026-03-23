#include <ydb/library/kll_median/sketch.h>
#include <ydb/library/kll_median/windowed_sketch.h>

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

Y_UNIT_TEST_SUITE(TWindowedKllTest) {

    Y_UNIT_TEST(EmptyWindowThrowsOnMedian) {
        TWindowedKll<int> w(100, 60, 10, 42);
        UNIT_ASSERT_EXCEPTION(w.MedianInWindow(100), yexception);
    }

    Y_UNIT_TEST(SingleElement) {
        TWindowedKll<int> w(100, 60, 10, 42);
        w.Add(17,  50);
        UNIT_ASSERT_VALUES_EQUAL(w.MedianInWindow(50), 17);
    }

    Y_UNIT_TEST(MedianInWindowMultipleBuckets) {
        TWindowedKll<int> w(100, 60, 10, 42);
        for (int i = 0; i < 10; ++i) {
            w.Add(i * 10, i * 6);  // spread across buckets
        }
        int median = w.MedianInWindow(60);
        UNIT_ASSERT(median >= 0 && median <= 90);
    }

    Y_UNIT_TEST(WindowSlidesOldDataExcluded) {
        TWindowedKll<int> w(100, 20, 10, 42);
        w.Add(100, 0);   // bucket 0, ts 0
        w.Add(200, 5);   // bucket 0, ts 0
        UNIT_ASSERT(w.MedianInWindow(10) >= 100 && w.MedianInWindow(10) <= 200);
        // At ts=31, window is [12..31], minBucketStart=10, bucket at 0 is outside
        w.Add(300, 25);   // bucket 20
        UNIT_ASSERT_VALUES_EQUAL(w.MedianInWindow(31), 300);
    }

    Y_UNIT_TEST(DeterministicWithSeed) {
        TWindowedKll<int> w1(100, 60, 10, 12345);
        TWindowedKll<int> w2(100, 60, 10, 12345);
        for (int i = 0; i < 100; ++i) {
            w1.Add(i, i);
            w2.Add(i, i);
        }
        UNIT_ASSERT_VALUES_EQUAL(w1.MedianInWindow(99), w2.MedianInWindow(99));
    }

    Y_UNIT_TEST(BucketReuseClearsOldData) {
        TWindowedKll<int> w(100, 30, 10, 42);
        // NumBuckets = 3, so idx cycles every 30 sec
        w.Add(1, 0);   // idx 0, bucket 0
        w.Add(2, 5);   // idx 0, bucket 0
        // At ts=35, idx 0 is reused for bucket 30-40, old bucket 0 is outside window
        w.Add(99, 35);  // idx 0, new bucket 30
        UNIT_ASSERT_VALUES_EQUAL(w.MedianInWindow(35), 99);
    }

    Y_UNIT_TEST(InvalidWindowSecThrows) {
        UNIT_ASSERT_EXCEPTION(TWindowedKll<int>(100, 0, 10, 42), yexception);
    }

    Y_UNIT_TEST(StreamingManyElements) {
        TWindowedKll<ui64> w(50, 100, 10, 42);
        for (ui64 i = 0; i < 500; ++i) {
            w.Add(i, i % 50);
        }
        ui64 median = w.MedianInWindow(50);
        UNIT_ASSERT(median >= 0 && median <= 499);
    }

    Y_UNIT_TEST(StringSingleElement) {
        TWindowedKll<TString> w(100, 60, 10, 42);
        w.Add(TString{"partition-key-42"}, 50);
        UNIT_ASSERT_VALUES_EQUAL(w.MedianInWindow(50), TString{"partition-key-42"});
    }

    Y_UNIT_TEST(StringTwoElementsLexicographic) {
        TWindowedKll<TString> w(100, 60, 10, 42);
        w.Add(TString{"aaa"}, 10);
        w.Add(TString{"zzz"}, 11);
        TString m = w.MedianInWindow(20);
        UNIT_ASSERT(m >= TString{"aaa"} && m <= TString{"zzz"});
    }

    Y_UNIT_TEST(StringQuantileMinMax) {
        TWindowedKll<TString> w(100, 60, 10, 42);
        w.Add(TString{"b"}, 5);
        w.Add(TString{"a"}, 6);
        w.Add(TString{"c"}, 7);
        UNIT_ASSERT_VALUES_EQUAL(w.MedianInWindow(10), TString{"b"});
    }

    Y_UNIT_TEST(StringStreamingMany) {
        TWindowedKll<TString> w(50, 300, 10, 777);
        for (int i = 0; i < 200; ++i) {
            w.Add(TStringBuilder() << "key-" << i, static_cast<ui64>(i));
        }
        TString median = w.MedianInWindow(199);
        UNIT_ASSERT(median.StartsWith("key-"));
    }

    Y_UNIT_TEST(StringDeterministicWithSeed) {
        TWindowedKll<TString> w1(100, 60, 10, 4242);
        TWindowedKll<TString> w2(100, 60, 10, 4242);
        for (int i = 0; i < 50; ++i) {
            w1.Add(TStringBuilder() << "s" << i, static_cast<ui64>(i));
            w2.Add(TStringBuilder() << "s" << i, static_cast<ui64>(i));
        }
        UNIT_ASSERT_VALUES_EQUAL(w1.MedianInWindow(49), w2.MedianInWindow(49));
    }

    Y_UNIT_TEST(StringMultipleBucketsMedian) {
        TWindowedKll<TString> w(100, 60, 10, 42);
        w.Add(TString{"m"}, 0);
        w.Add(TString{"a"}, 15);
        w.Add(TString{"z"}, 30);
        TString med = w.MedianInWindow(35);
        UNIT_ASSERT(med >= TString{"a"} && med <= TString{"z"});
    }

    Y_UNIT_TEST(StringWindowSlides) {
        TWindowedKll<TString> w(100, 20, 10, 42);
        w.Add(TString{"old"}, 0);
        w.Add(TString{"new"}, 25);
        UNIT_ASSERT_VALUES_EQUAL(w.MedianInWindow(31), TString{"new"});
    }

} // Y_UNIT_TEST_SUITE(TWindowedKllTest)
