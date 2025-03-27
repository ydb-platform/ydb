#include <library/cpp/testing/unittest/registar.h>

#include "eq_width_histogram.h"

namespace NKikimr {
namespace NOptimizerHistograms {

template <typename T>
bool EqualHistograms(const std::shared_ptr<TEqWidthHistogram> &left, const std::shared_ptr<TEqWidthHistogram> &right) {
  // Not expecting any nullptr.
  if (!left || !right) return false;

  if (left->GetNumBuckets() != right->GetNumBuckets()) {
    return false;
  }
  if (left->GetType() != right->GetType()) {
    return false;
  }

  for (ui32 i = 0; i < left->GetNumBuckets(); ++i) {
    const auto &leftBucket = left->GetBuckets()[i];
    const auto &rightBucket = right->GetBuckets()[i];
    if (leftBucket.count != rightBucket.count) {
      return false;
    }
    if (!CmpEqual<T>(LoadFrom<T>(leftBucket.start), LoadFrom<T>(rightBucket.start))) {
      return false;
    }
  }

  return true;
}

template <typename T>
std::shared_ptr<TEqWidthHistogram> CreateHistogram(ui32 numBuckets, T start, T range, EHistogramValueType valueType) {
  std::shared_ptr<TEqWidthHistogram> histogram(std::make_shared<TEqWidthHistogram>(numBuckets, valueType));
  TEqWidthHistogram::TBucketRange bucketRange;
  StoreTo<T>(bucketRange.start, start);
  StoreTo<T>(bucketRange.end, range);
  histogram->InitializeBuckets<T>(bucketRange);
  return histogram;
}

template <typename T>
void PopulateHistogram(std::shared_ptr<TEqWidthHistogram> histogram, const std::pair<ui32, ui32> &range) {
  for (ui32 i = range.first; i < range.second; ++i) {
    histogram->AddElement<T>(i);
  }
}

std::shared_ptr<TEqWidthHistogram> CreateStringPrefixHistogram() {
  std::shared_ptr<TEqWidthHistogram> histogram(
      std::make_shared<TEqWidthHistogram>(26 * 26, EHistogramValueType::StringPrefix));
  histogram->InitializeBucketsStringPrefix(26, 26, 'A');
  return histogram;
}

template <typename T>
void TestHistogramBasic(ui32 numBuckets, std::pair<ui32, ui32> range, std::pair<T, T> bucketRange,
                        EHistogramValueType valueType, std::pair<T, ui64> less, std::pair<T, ui64> greater) {
  auto histogram = CreateHistogram<T>(numBuckets, bucketRange.first, bucketRange.second, valueType);
  UNIT_ASSERT_VALUES_EQUAL(histogram->GetNumBuckets(), numBuckets);
  PopulateHistogram<T>(histogram, range);
  TEqWidthHistogramEstimator estimator(histogram);
  UNIT_ASSERT_VALUES_EQUAL(estimator.EstimateLessOrEqual<T>(less.first), less.second);
  UNIT_ASSERT_VALUES_EQUAL(estimator.EstimateGreaterOrEqual<T>(greater.first), greater.second);
}

void TestHistogramStringPrefixSerialize() {
  auto histogram = CreateStringPrefixHistogram();
  TVector<TString> vecs{"USA", "CANADA", "RUSSIA"};
  for (const auto &str : vecs) {
    NOptimizerHistograms::TStringPrefix prefix(str.data(), str.size());
    histogram->AddElement(prefix);
  }
  ui64 binarySize = 0;
  auto binaryData = histogram->Serialize(binarySize);
  UNIT_ASSERT(binaryData && binarySize);
  TString hString(binaryData.get(), binarySize);
  auto histogramFromString = std::make_shared<TEqWidthHistogram>(hString.data(), hString.size());
  UNIT_ASSERT(histogramFromString);
  UNIT_ASSERT(EqualHistograms<TStringPrefix>(histogram, histogramFromString));
}

void TestHistogramStringPrefix() {
  auto histogram = CreateStringPrefixHistogram();
  TVector<std::pair<TString, std::pair<ui32, ui32>>> vecs{{"AFRICA", {1, 3}}, {"AMERICA", {2, 2}}, {"EUROPE", {3, 1}}};
  for (const auto &[str, result] : vecs) {
    NOptimizerHistograms::TStringPrefix prefix(str.data(), str.size());
    histogram->AddElement(prefix);
  }
  TEqWidthHistogramEstimator estimator(histogram);
  for (const auto &[str, result] : vecs) {
    NOptimizerHistograms::TStringPrefix prefix(str.data(), str.size());
    UNIT_ASSERT_VALUES_EQUAL(estimator.EstimateLessOrEqual<NOptimizerHistograms::TStringPrefix>(prefix), result.first);
    UNIT_ASSERT_VALUES_EQUAL(estimator.EstimateGreaterOrEqual<NOptimizerHistograms::TStringPrefix>(prefix),
                             result.second);
  }
}

template <typename T>
void TestHistogramSerialization(ui32 numBuckets, std::pair<ui32, ui32> range, std::pair<T, T> bucketRange,
                                EHistogramValueType valueType) {
  auto histogram = CreateHistogram<T>(numBuckets, bucketRange.first, bucketRange.second, valueType);
  UNIT_ASSERT(histogram);
  PopulateHistogram<T>(histogram, range);
  ui64 binarySize = 0;
  auto binaryData = histogram->Serialize(binarySize);
  UNIT_ASSERT(binaryData && binarySize);
  TString hString(binaryData.get(), binarySize);
  auto histogramFromString = std::make_shared<TEqWidthHistogram>(hString.data(), hString.size());
  UNIT_ASSERT(histogramFromString);
  UNIT_ASSERT(EqualHistograms<T>(histogram, histogramFromString));
}

template <typename T>
void TestHistogramAggregate(ui32 numBuckets, std::pair<ui32, ui32> range, std::pair<T, T> bucketRange,
                            EHistogramValueType valueType, ui32 numCombine, const TVector<ui64> &resultCount) {
  auto histogram = CreateHistogram<T>(numBuckets, bucketRange.first, bucketRange.second, valueType);
  UNIT_ASSERT(histogram);
  PopulateHistogram<T>(histogram, range);
  auto histogramToAdd = CreateHistogram<T>(numBuckets, bucketRange.first, bucketRange.second, valueType);
  PopulateHistogram<T>(histogramToAdd, range);
  UNIT_ASSERT(histogram);
  for (ui32 i = 0; i < numCombine; ++i) histogram->template Aggregate<T>(*histogramToAdd);
  for (ui32 i = 0; i < histogram->GetNumBuckets(); ++i) {
    UNIT_ASSERT(histogram->GetBuckets()[i].count == resultCount[i]);
  }
}

Y_UNIT_TEST_SUITE(EqWidthHistogram) {
  Y_UNIT_TEST(Basic) {
    TestHistogramBasic<ui32>(10, /*values range=*/{0, 10}, /*bucket range=*/{0, 2}, EHistogramValueType::Uint32,
                             /*{value, result}=*/{9, 10},
                             /*{value, result}=*/{10, 0});
    TestHistogramBasic<ui64>(10, /*values range=*/{0, 10}, /*bucket range=*/{0, 2}, EHistogramValueType::Uint64,
                             /*{value, result}=*/{9, 10},
                             /*{value, result}=*/{10, 0});
    TestHistogramBasic<i32>(10, /*values range=*/{0, 10}, /*bucket range=*/{0, 2}, EHistogramValueType::Int32,
                            /*{value, result}=*/{9, 10},
                            /*{value, result}=*/{10, 0});
    TestHistogramBasic<i64>(10, /*values range=*/{0, 10}, /*bucket range=*/{0, 2}, EHistogramValueType::Int64,
                            /*{value, result}=*/{9, 10},
                            /*{value, result}=*/{10, 0});
    TestHistogramBasic<double>(10, /*values range=*/{0.0, 10.0}, /*bucket range=*/{0.0, 2.0},
                               EHistogramValueType::Double,
                               /*{value, result}=*/{9.0, 10},
                               /*{value, result}=*/{10.0, 0});
  }

  Y_UNIT_TEST(Serialization) {
    TestHistogramSerialization<ui32>(10, /*values range=*/{0, 10}, /*bucket range=*/{0, 2},
                                     EHistogramValueType::Uint32);
    TestHistogramSerialization<ui64>(10, /*values range=*/{0, 10}, /*bucket range=*/{0, 2},
                                     EHistogramValueType::Uint64);
    TestHistogramSerialization<i32>(10, /*values range=*/{0, 10}, /*bucket range=*/{0, 2}, EHistogramValueType::Int32);
    TestHistogramSerialization<i64>(10, /*values range=*/{0, 10}, /*bucket range=*/{0, 2}, EHistogramValueType::Int64);
    TestHistogramSerialization<double>(10, /*values range=*/{0.0, 10.0}, /*bucket range=*/{0.0, 2.0},
                                       EHistogramValueType::Double);
  }
  Y_UNIT_TEST(AggregateHistogram) {
    TVector<ui64> resultCount{20, 20, 20, 20, 20, 0, 0, 0, 0, 0};
    TestHistogramAggregate<ui32>(10, /*values range=*/{0, 10}, /*bucket range=*/{0, 2}, EHistogramValueType::Uint32, 9,
                                 resultCount);
  }
  Y_UNIT_TEST(StringPrefix) {
    TestHistogramStringPrefix();
    TestHistogramStringPrefixSerialize();
  }
}
}  // namespace NOptimizerHistograms
}  // namespace NKikimr