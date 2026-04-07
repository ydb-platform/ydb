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
        T rangeStart = LoadFrom<T>(histogram->GetDomainRange().Start);
        for (ui32 i = 0; i < histogram->GetNumBuckets(); ++i) {
            T border = rangeStart + static_cast<T>(i) * width;
            histogram->AddElement(border - step);
            histogram->AddElement(border + step);
        }
    } else {
        for (T i = range.first; i < range.first + 25; ++i) {
            histogram->AddElement(i);
        }
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
    PopulateHistogram<T>(histogram, range, valueType);
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
    PopulateHistogram<T>(histogram, range, valueType);
    auto histogramToAdd = CreateHistogram<T>(numBuckets, domainRange.first, domainRange.second, valueType);
    PopulateHistogram<T>(histogramToAdd, range, valueType);
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
    TestHistogramBasic<ui32>(1, /*values range=*/{0, 25}, /*column range=*/{0, 20}, EHistogramValueType::Uint32,
                             /*{value, result}=*/{25, 25},
                             /*{value, result}=*/{4, 25});
    TestHistogramBasic<i32>(1, /*values range=*/{-5, 25}, /*column range=*/{0, 20}, EHistogramValueType::Int32,
                            /*{value, result}=*/{25, 25},
                            /*{value, result}=*/{4, 25});
    TestHistogramBasic<double>(1, /*values range=*/{-5.0, 25.0}, /*column range=*/{0.0, 20.0}, EHistogramValueType::Double,
                               /*{value, result}=*/{25.0, 2},
                               /*{value, result}=*/{4.0, 2});

    TestHistogramBasic<ui32>(10, /*values range=*/{0, 25}, /*column range=*/{0, 20}, EHistogramValueType::Uint32,
                             /*{value, result}=*/{25, 25},
                             /*{value, result}=*/{4, 21});
    TestHistogramBasic<ui64>(10, /*values range=*/{0, 25}, /*column range=*/{0, 20}, EHistogramValueType::Uint64,
                             /*{value, result}=*/{25, 25},
                             /*{value, result}=*/{4, 21});

    TestHistogramBasic<i32>(10, /*values range=*/{-5, 25}, /*column range=*/{0, 20}, EHistogramValueType::Int32,
                            /*{value, result}=*/{25, 25},
                            /*{value, result}=*/{4, 16});
    TestHistogramBasic<i64>(10, /*values range=*/{-5, 25}, /*column range=*/{0, 20}, EHistogramValueType::Int64,
                            /*{value, result}=*/{25, 25},
                            /*{value, result}=*/{4, 16});

    TestHistogramBasic<float>(10, /*values range=*/{-5.0, 25.0}, /*column range=*/{0.0, 20.0},
                              EHistogramValueType::Float,
                              /*{value, result}=*/{25.0, 20},
                              /*{value, result}=*/{4.0, 15});
    TestHistogramBasic<double>(10, /*values range=*/{-5.0, 25.0}, /*column range=*/{0.0, 20.0},
                               EHistogramValueType::Double,
                               /*{value, result}=*/{25.0, 20},
                               /*{value, result}=*/{4.0, 15});

    TestHistogramBasic<double>(10, /*values range=*/{-0.1, 1.5},
                               /*column range=*/{1.0, 1.0 + 30 * std::numeric_limits<double>::epsilon()},
                               EHistogramValueType::Double,
                               /*{value, result}=*/{2.0, 20},
                               /*{value, result}=*/{2.0, 1});
    TestHistogramBasic<double>(10, /*values range=*/{99.0, 100.5},
                               /*column range=*/{100.0, 100.0 + 300 * std::numeric_limits<double>::epsilon()},
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
    TestHistogramSerialization<double>(10, /*values range=*/{-0.1, 1.5},
                                       /*column range=*/{1.0, 1.0 + 30 * std::numeric_limits<double>::epsilon()},
                                       EHistogramValueType::Double);
}

Y_UNIT_TEST(AggregateHistogram) {
    TVector<ui64> resultCountInt{20, 20, 20, 20, 20, 20, 20, 20, 20, 70};
    TestHistogramAggregate<ui32>(10, /*values range=*/{0, 10}, /*column range=*/{0, 20}, EHistogramValueType::Uint32, 9,
                                 resultCountInt);
    TVector<ui64> resultCountFloat{30, 20, 20, 20, 20, 20, 20, 20, 20, 10};
    TestHistogramAggregate<double>(10, /*values range=*/{-0.1, 1.5},
                                   /*column range=*/{1.0, 1.0 + 30 * std::numeric_limits<double>::epsilon()},
                                   EHistogramValueType::Double, 9, resultCountFloat);
}
} // Y_UNIT_TEST_SUITE(EqWidthHistogram)
} // namespace NKikimr
