#include <library/cpp/testing/unittest/registar.h>

#include "eq_width_histogram.h"

namespace NKikimr {

template <typename T>
bool EqualHistograms(const std::shared_ptr<TEqWidthHistogram>& left, const std::shared_ptr<TEqWidthHistogram>& right) {
    // Not expecting any nullptr.
    if (!left || !right) {
        return false;
    } else if (!left->BucketsEqual<T>(*right)) {
        return false;
    }
    if (left->GetBucketWidth<T>() != right->GetBucketWidth<T>()) {
        return false;
    }

    for (ui32 i = 0; i < left->GetNumBuckets(); ++i) {
        if (left->GetNumElementsInBucket(i) != right->GetNumElementsInBucket(i)) {
            return false;
        }
    }
    return true;
}

template <typename T>
std::shared_ptr<TEqWidthHistogram> CreateHistogram(ui32 numBuckets, T start, T end, EHistogramValueType valueType) {
    std::shared_ptr<TEqWidthHistogram> histogram(std::make_shared<TEqWidthHistogram>(numBuckets, valueType));
    histogram->InitializeBuckets(start, end);
    return histogram;
}

template <typename T>
void PopulateHistogram(std::shared_ptr<TEqWidthHistogram> histogram, const std::pair<T, T>& range) {
    // NOTE: reconsider the loop on string, date, and bool due to i++
    for (T i = range.first; i < range.second; ++i) {
        histogram->AddElement(i);
    }
}

template <typename T>
void TestHistogramBasic(ui32 numBuckets, std::pair<T, T> range, std::pair<T, T> domainRange,
                        EHistogramValueType valueType, std::pair<T, ui64> less, std::pair<T, ui64> greater) {
    auto histogram = CreateHistogram<T>(numBuckets, domainRange.first, domainRange.second, valueType);
    UNIT_ASSERT_VALUES_EQUAL(histogram->GetNumBuckets(), numBuckets);
    PopulateHistogram<T>(histogram, range, valueType);
    TEqWidthHistogramEstimator estimator(histogram);
    UNIT_ASSERT_VALUES_EQUAL(estimator.EstimateLessOrEqual<T>(less.first), less.second);
    UNIT_ASSERT_VALUES_EQUAL(estimator.EstimateGreaterOrEqual<T>(greater.first), greater.second);
}

template <typename T>
void TestHistogramSerialization(ui32 numBuckets, std::pair<T, T> range, std::pair<T, T> domainRange,
                                EHistogramValueType valueType) {
    auto histogram = CreateHistogram<T>(numBuckets, domainRange.first, domainRange.second, valueType);
    UNIT_ASSERT(histogram);
    PopulateHistogram<T>(histogram, range);
    TString hString = histogram->Serialize();
    UNIT_ASSERT(!hString.empty());
    auto histogramFromString = std::make_shared<TEqWidthHistogram>(hString.data(), hString.size());
    UNIT_ASSERT(histogramFromString);
    UNIT_ASSERT(EqualHistograms<T>(histogram, histogramFromString));
}

template <typename T>
void TestHistogramAggregate(ui32 numBuckets, std::pair<T, T> range, std::pair<T, T> domainRange,
                            EHistogramValueType valueType, ui32 numCombine, const TVector<ui64>& resultCount) {
    auto histogram = CreateHistogram<T>(numBuckets, domainRange.first, domainRange.second, valueType);
    UNIT_ASSERT(histogram);
    PopulateHistogram<T>(histogram, range);
    auto histogramToAdd = CreateHistogram<T>(numBuckets, domainRange.first, domainRange.second, valueType);
    PopulateHistogram<T>(histogramToAdd, range);
    UNIT_ASSERT(histogramToAdd);
    for (ui32 i = 0; i < numCombine; ++i) {
        histogram->Aggregate(*histogramToAdd);
    }
    for (ui32 i = 0; i < histogram->GetNumBuckets(); ++i) {
        UNIT_ASSERT(histogram->GetNumElementsInBucket(i) == resultCount[i]);
    }
}

Y_UNIT_TEST_SUITE(EqWidthHistogram) {

Y_UNIT_TEST(Basic) {
    TestHistogramBasic<ui32>(10, /*values range=*/{0, 10}, /*column range=*/{0, 20}, EHistogramValueType::Uint32,
                             /*{value, result}=*/{9, 10},
                             /*{value, result}=*/{10, 0});
    TestHistogramBasic<ui64>(10, /*values range=*/{0, 10}, /*column range=*/{0, 20}, EHistogramValueType::Uint64,
                             /*{value, result}=*/{9, 10},
                             /*{value, result}=*/{10, 0});
    TestHistogramBasic<i32>(10, /*values range=*/{0, 10}, /*column range=*/{0, 20}, EHistogramValueType::Int32,
                            /*{value, result}=*/{9, 10},
                            /*{value, result}=*/{10, 0});
    TestHistogramBasic<i64>(10, /*values range=*/{0, 10}, /*column range=*/{0, 20}, EHistogramValueType::Int64,
                            /*{value, result}=*/{9, 10},
                            /*{value, result}=*/{10, 0});
    TestHistogramBasic<double>(10, /*values range=*/{0.0, 10.0}, /*column range=*/{0.0, 20.0},
                               EHistogramValueType::Double,
                               /*{value, result}=*/{98.0, 4},
                               /*{value, result}=*/{101.0, 0});
}

Y_UNIT_TEST(Overload) {
    TestHistogramBasic<ui32>(1, /*values range=*/{0, 25}, /*column range=*/{5, 10}, EHistogramValueType::Uint32,
                             /*{value, result}=*/{25, 25},
                             /*{value, result}=*/{4, 25});

    TestHistogramBasic<i32>(1, /*values range=*/{0, 25}, /*column range=*/{-10, -5}, EHistogramValueType::Int32,
                            /*{value, result}=*/{25, 25},
                            /*{value, result}=*/{4, 25});

    TestHistogramBasic<i32>(1, /*values range=*/{0, 25}, /*column range=*/{-5, 10}, EHistogramValueType::Int32,
                            /*{value, result}=*/{25, 25},
                            /*{value, result}=*/{4, 25});

    TestHistogramBasic<i32>(1, /*values range=*/{0, 25}, /*column range=*/{-1, std::numeric_limits<i32>::max()}, EHistogramValueType::Int32,
                            /*{value, result}=*/{25, 25},
                            /*{value, result}=*/{4, 25});
    TestHistogramBasic<i32>(1, /*values range=*/{std::numeric_limits<i32>::min(), std::numeric_limits<i32>::max()},
                            /*column range=*/{std::numeric_limits<i32>::min(), std::numeric_limits<i32>::max()}, EHistogramValueType::Int32,
                            /*{value, result}=*/{25, 25},
                            /*{value, result}=*/{4, 25});
}

Y_UNIT_TEST(Serialization) {
    TestHistogramSerialization<ui32>(10, /*values range=*/{0, 10}, /*column range=*/{0, 20},
                                     EHistogramValueType::Uint32);
    TestHistogramSerialization<ui64>(10, /*values range=*/{0, 10}, /*column range=*/{0, 20},
                                     EHistogramValueType::Uint64);
    TestHistogramSerialization<i32>(10, /*values range=*/{0, 10}, /*column range=*/{0, 20}, EHistogramValueType::Int32);
    TestHistogramSerialization<i64>(10, /*values range=*/{0, 10}, /*column range=*/{0, 20}, EHistogramValueType::Int64);
    TestHistogramSerialization<double>(10, /*values range=*/{0.0, 10.0}, /*column range=*/{0.0, 20.0},
                                       EHistogramValueType::Double);
}

Y_UNIT_TEST(AggregateHistogram) {
    TVector<ui64> resultCount{20, 20, 20, 20, 20, 0, 0, 0, 0, 0};
    TestHistogramAggregate<ui32>(10, /*values range=*/{0, 10}, /*column range=*/{0, 20}, EHistogramValueType::Uint32, 9,
                                 resultCount);
}
} // Y_UNIT_TEST_SUITE(EqWidthHistogram)
} // namespace NKikimr
