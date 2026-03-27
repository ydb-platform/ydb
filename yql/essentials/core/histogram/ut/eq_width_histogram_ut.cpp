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
void PopulateHistogram(std::shared_ptr<TEqWidthHistogram> histogram, const std::pair<T, T>& range,
                       EHistogramValueType valueType) {
    // NOTE: reconsider the loop on string, date, and bool due to i++
    if (valueType == EHistogramValueType::Float || valueType == EHistogramValueType::Double) {
        TEqWidthHistogram::THistValue bucketWidth = histogram->template GetBucketWidth<T>();
        T width = LoadFrom<T>(bucketWidth.Value);
        T step = width / 2;
        for (ui32 i = 0; i < histogram->GetNumBuckets(); ++i) {
            T border = histogram->GetBorderValue<T>(i);
            histogram->AddElement(border - step);
            histogram->AddElement(border + step);
        }
    } else {
        for (T i = range.first; i < range.second; ++i) {
            histogram->AddElement(i);
        }
    }
}

template <typename TRange, typename TDomain>
void TestHistogramBasic(ui32 numBuckets, std::pair<TRange, TRange> range, std::pair<TDomain, TDomain> domainRange,
                        EHistogramValueType valueType, std::pair<TRange, ui64> less,
                        std::pair<TRange, ui64> greater, std::pair<TRange, ui64> equal) {
    auto histogram = CreateHistogram<TDomain>(numBuckets, domainRange.first, domainRange.second, valueType);
    UNIT_ASSERT_VALUES_EQUAL(histogram->GetNumBuckets(), numBuckets);
    PopulateHistogram<TRange>(histogram, range, valueType);
    TEqWidthHistogramEstimator estimator(histogram);
    UNIT_ASSERT_VALUES_EQUAL(estimator.EstimateLessOrEqual<TRange>(less.first), less.second);
    UNIT_ASSERT_VALUES_EQUAL(estimator.EstimateGreaterOrEqual<TRange>(greater.first), greater.second);
    UNIT_ASSERT_VALUES_EQUAL(estimator.EstimateEqual<TRange>(equal.first), equal.second);
}

template <typename TRange, typename TDomain>
void TestHistogramSerialization(ui32 numBuckets, std::pair<TRange, TRange> range, std::pair<TDomain, TDomain> domainRange,
                                EHistogramValueType valueType) {
    auto histogram = CreateHistogram<TDomain>(numBuckets, domainRange.first, domainRange.second, valueType);
    UNIT_ASSERT(histogram);
    PopulateHistogram<TRange>(histogram, range, valueType);
    TString hString = histogram->Serialize();
    UNIT_ASSERT(!hString.empty());
    auto histogramFromString = std::make_shared<TEqWidthHistogram>(hString.data(), hString.size());
    UNIT_ASSERT(histogramFromString);
    UNIT_ASSERT(EqualHistograms<TDomain>(histogram, histogramFromString));
}

template <typename TRange, typename TDomain>
void TestHistogramAggregate(ui32 numBuckets, std::pair<TRange, TRange> range, std::pair<TDomain, TDomain> domainRange,
                            EHistogramValueType valueType, ui32 numCombine, const TVector<ui64>& resultCount) {
    auto histogram = CreateHistogram<TDomain>(numBuckets, domainRange.first, domainRange.second, valueType);
    UNIT_ASSERT(histogram);
    PopulateHistogram<TRange>(histogram, range, valueType);
    auto histogramToAdd = CreateHistogram<TDomain>(numBuckets, domainRange.first, domainRange.second, valueType);
    UNIT_ASSERT(histogramToAdd);
    PopulateHistogram<TRange>(histogramToAdd, range, valueType);
    for (ui32 i = 0; i < numCombine; ++i) {
        histogram->Aggregate(*histogramToAdd);
    }
    for (ui32 i = 0; i < histogram->GetNumBuckets(); ++i) {
        UNIT_ASSERT(histogram->GetNumElementsInBucket(i) == resultCount[i]);
    }
}

template <typename TRange, typename TDomain>
void TestHistogramCardinality(EHistogramValueType valueType, std::pair<ui64, ui64> resultCounts, ui64 resultCardinality,
                              ui32 numBuckets, std::pair<TRange, TRange> range, std::pair<TDomain, TDomain> domainRange,
                              ui32 otherNumBuckets, std::pair<TRange, TRange> otherRange, std::pair<TDomain, TDomain> otherDomainRange) {
    auto histogram = CreateHistogram<TDomain>(numBuckets, domainRange.first, domainRange.second, valueType);
    UNIT_ASSERT(histogram);
    PopulateHistogram<TRange>(histogram, range, valueType);
    TEqWidthHistogramEstimator estimator(histogram);
    auto otherHistogram = CreateHistogram<TDomain>(otherNumBuckets, otherDomainRange.first, otherDomainRange.second, valueType);
    UNIT_ASSERT(otherHistogram);
    PopulateHistogram<TRange>(otherHistogram, otherRange, valueType);
    TEqWidthHistogramEstimator otherEstimator(otherHistogram);

    UNIT_ASSERT(estimator.GetNumElements() == resultCounts.first);
    UNIT_ASSERT(otherEstimator.GetNumElements() == resultCounts.second);

    auto cardinality = estimator.GetOverlappingCardinality(otherEstimator);
    UNIT_ASSERT(cardinality);
    UNIT_ASSERT(cardinality.GetRef() == resultCardinality);
}

Y_UNIT_TEST_SUITE(EqWidthHistogram) {

Y_UNIT_TEST(Basic) {
    // single bucket cases + signed values cases
    TestHistogramBasic<ui32, ui32>(1, /*values range=*/{0, 25}, /*column range=*/{0, 20}, EHistogramValueType::Uint32,
                                   /*{value, result}=*/{25, 21}, /*{value, result}=*/{4, 1}, /*{value, result}=*/{4, 1});
    TestHistogramBasic<i32, i32>(1, /*values range=*/{-5, 25}, /*column range=*/{0, 20}, EHistogramValueType::Int32,
                                 /*{value, result}=*/{25, 21}, /*{value, result}=*/{4, 1}, /*{value, result}=*/{4, 1});
    TestHistogramBasic<double, double>(1, /*values range=*/{-5.0, 25.0}, /*column range=*/{0.0, 20.5}, EHistogramValueType::Double,
                                       /*{value, result}=*/{25.0, 1}, /*{value, result}=*/{4.0, 1}, /*{value, result}=*/{2.0, 1});

    // multiple bucket cases + signed values cases
    TestHistogramBasic<ui32, ui32>(10, /*values range=*/{0, 25}, /*column range=*/{0, 20}, EHistogramValueType::Uint32,
                                   /*{value, result}=*/{25, 21}, /*{value, result}=*/{4, 16}, /*{value, result}=*/{4, 1});
    TestHistogramBasic<i64, i64>(10, /*values range=*/{-30, 0}, /*column range=*/{-25, -5}, EHistogramValueType::Int64,
                                 /*{value, result}=*/{25, 21}, /*{value, result}=*/{-17, 12}, /*{value, result}=*/{1, 1});
    TestHistogramBasic<i64, i64>(10, /*values range=*/{-10, 25}, /*column range=*/{-5, 20}, EHistogramValueType::Int64,
                                 /*{value, result}=*/{25, 26}, /*{value, result}=*/{-2, 23}, /*{value, result}=*/{-4, 1});

    // floating point data test
    TestHistogramBasic<float, float>(10, /*values range=*/{-5.0, 25.0}, /*column range=*/{0.0, 20.0}, EHistogramValueType::Float,
                                     /*{value, result}=*/{25.0, 19}, /*{value, result}=*/{4.0, 15}, /*{value, result}=*/{2.0, 2});
    TestHistogramBasic<double, double>(10, /*values range=*/{-5.0, 25.0}, /*column range=*/{0.0, 20.0}, EHistogramValueType::Double,
                                       /*{value, result}=*/{25.0, 19}, /*{value, result}=*/{4.0, 15}, /*{value, result}=*/{2.0, 2});

    // large and small floating point data test
    TestHistogramBasic<double, double>(10, /*values range=*/{-0.1, 1.5},
                                       /*column range=*/{1.0, 1.0 + 30 * std::numeric_limits<double>::epsilon()}, EHistogramValueType::Double,
                                       /*{value, result}=*/{2.0, 19}, /*{value, result}=*/{2.0, 0}, /*{value, result}=*/{4.0, 1});
    TestHistogramBasic<double, double>(10, /*values range=*/{99.0, 100.5},
                                       /*column range=*/{100.0, 100.0 + 300 * std::numeric_limits<double>::epsilon()}, EHistogramValueType::Double,
                                       /*{value, result}=*/{98.0, 0}, /*{value, result}=*/{101.0, 0}, /*{value, result}=*/{4.0, 4});
}

Y_UNIT_TEST(Overflow) {
    // TO BE TESTED: domain and range are signed and unsigned, vice-verca
    TestHistogramBasic<i32, ui32>(10, /*values range=*/{-5, 25}, /*column range=*/{0, 20}, EHistogramValueType::Uint32,
                                  /*{value, result}=*/{25, 21}, /*{value, result}=*/{4, 16}, /*{value, result}=*/{4, 1});
    // UNIT_ASSERT_EXCEPTION((TestHistogramBasic<ui32, i32>(10, /*values range=*/{0, 25}, /*column range=*/{-5, 20}, EHistogramValueType::Int32,
    //                                                      /*{value, result}=*/{25, 21}, /*{value, result}=*/{4, 17}, /*{value, result}=*/{2, 1})), yexception);
    // UNIT_ASSERT_EXCEPTION((TestHistogramBasic<double, ui32>(10, /*values range=*/{-5.0, 25.0}, /*column range=*/{0, 20}, EHistogramValueType::Uint32,
    //                                                         /*{value, result}=*/{25.0, 21}, /*{value, result}=*/{4.0, 17}, /*{value, result}=*/{4.0, 2})), yexception);
    // UNIT_ASSERT_EXCEPTION((TestHistogramBasic<ui32, double>(10, /*values range=*/{0, 25}, /*column range=*/{-5.0, 20.0}, EHistogramValueType::Double,
    //                                                         /*{value, result}=*/{25, 19}, /*{value, result}=*/{4, 13}, /*{value, result}=*/{4, 2})), yexception);

    // overflow test
    TestHistogramBasic<i32, i32>(1, /*values range=*/{0, 25}, /*column range=*/{-1, std::numeric_limits<i32>::max()}, EHistogramValueType::Int32,
                                 /*{value, result}=*/{25, 0}, /*{value, result}=*/{4, 0}, /*{value, result}=*/{2, 0});
    TestHistogramBasic<i32, i32>(1, /*values range=*/{0, 25},
                                 /*column range=*/{std::numeric_limits<i32>::min(), std::numeric_limits<i32>::max()}, EHistogramValueType::Int32,
                                 /*{value, result}=*/{25, 0}, /*{value, result}=*/{4, 0}, /*{value, result}=*/{2, 0});

    // TO BE TESTED: down-casting test between types of buckets, domain range, and value range
    // UNIT_ASSERT_EXCEPTION((TestHistogramBasic<i32, i64>(10, /*values range=*/{0, 25}, /*column range=*/{0, std::numeric_limits<i64>::max()}, EHistogramValueType::Int64,
    //                                                     /*{value, result}=*/{25, 25}, /*{value, result}=*/{4, 21}, /*{value, result}=*/{2, 1})), yexception);
    // UNIT_ASSERT_EXCEPTION((TestHistogramBasic<i16, i64>(10, /*values range=*/{0, 25}, /*column range=*/{0, std::numeric_limits<i64>::max()}, EHistogramValueType::Int64,
    //                                                     /*{value, result}=*/{25, 25}, /*{value, result}=*/{4, 21}, /*{value, result}=*/{2, 1})), yexception);
    // UNIT_ASSERT_EXCEPTION((TestHistogramBasic<i64, i16>(10, /*values range=*/{0, 25}, /*column range=*/{0, std::numeric_limits<i16>::max()}, EHistogramValueType::Int16,
    //                                                     /*{value, result}=*/{25, 0}, /*{value, result}=*/{4, 0}, /*{value, result}=*/{2, 0})), yexception);
}

Y_UNIT_TEST(Serialization) {
    TestHistogramSerialization<ui32, ui32>(10, /*values range=*/{0, 10}, /*column range=*/{0, 20}, EHistogramValueType::Uint32);
    TestHistogramSerialization<ui64, ui64>(10, /*values range=*/{0, 10}, /*column range=*/{0, 20}, EHistogramValueType::Uint64);
    TestHistogramSerialization<i32, i32>(10, /*values range=*/{0, 10}, /*column range=*/{0, 20}, EHistogramValueType::Int32);
    TestHistogramSerialization<i64, i64>(10, /*values range=*/{0, 10}, /*column range=*/{0, 20}, EHistogramValueType::Int64);
    TestHistogramSerialization<double, double>(10, /*values range=*/{-0.1, 1.5},
                                               /*column range=*/{1.0, 1.0 + 30 * std::numeric_limits<double>::epsilon()}, EHistogramValueType::Double);
}

Y_UNIT_TEST(AggregateHistogram) {
    TVector<ui64> resultCountInt{20, 20, 20, 20, 20, 20, 20, 20, 20, 30};
    TestHistogramAggregate<ui32, ui32>(10, /*values range=*/{0, 25}, /*column range=*/{0, 20}, EHistogramValueType::Uint32, 9, resultCountInt);
    TVector<ui64> resultCountFloat{20, 20, 20, 20, 20, 20, 20, 20, 20, 10};
    TestHistogramAggregate<double, double>(10, /*values range=*/{-0.1, 1.5},
                                           /*column range=*/{1.0, 1.0 + 30 * std::numeric_limits<double>::epsilon()},
                                           EHistogramValueType::Double, 9, resultCountFloat);
}

Y_UNIT_TEST(OverlappingCardinality) {
    TestHistogramCardinality<ui32, ui32>(EHistogramValueType::Uint32, /*result counts=*/{21, 21}, /*cardinality=*/21,
                                         10, /*values range=*/{0, 25}, /*column range=*/{0, 20},
                                         10, /*values range=*/{0, 25}, /*column range=*/{0, 20});

    TestHistogramCardinality<ui32, ui32>(EHistogramValueType::Uint32, /*result counts=*/{25, 16}, /*cardinality=*/21,
                                         10, /*values range=*/{0, 25}, /*column range=*/{0, 25},
                                         10, /*values range=*/{0, 25}, /*column range=*/{5, 20});
}

} // Y_UNIT_TEST_SUITE(EqWidthHistogram)

} // namespace NKikimr
