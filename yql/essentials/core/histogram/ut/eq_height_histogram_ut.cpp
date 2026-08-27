#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/core/histogram/eq_height_histogram.h>
#include <yql/essentials/core/histogram/eq_height_histogram_reader.h>
#include <yql/essentials/core/histogram/ut/eq_height_histogram_test_api.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/ylimits.h>
#include <util/random/shuffle.h>
#include <util/random/fast.h>

#include <cstring>
#include <utility>

using namespace NKikimr;

namespace {

// Memcomparable key from an integer: 4 bytes big-endian, sign bit flipped.
TString MakeKey(i32 val) {
    const ui32 unsignedVal = static_cast<ui32>(val) ^ (1U << 31);
    TString out(4, '\0');
    out[0] = static_cast<char>((unsignedVal >> 24) & 0xFF);
    out[1] = static_cast<char>((unsignedVal >> 16) & 0xFF);
    out[2] = static_cast<char>((unsignedVal >> 8) & 0xFF);
    out[3] = static_cast<char>(unsignedVal & 0xFF);
    return out;
}

// Default byte budget for the histogram state (4 MB).
constexpr ui64 DEFAULT_MAX_STATE_BYTES = 4ULL << 20;
constexpr size_t HUGE_KEY_BYTES = 10ULL << 20;

TEqHeightHistogramBuilder::TParams MakeParams(ui32 numBuckets, ui64 maxStateBytes = DEFAULT_MAX_STATE_BYTES) {
    return {
        .NumBuckets = numBuckets,
        .EmissionRate = numBuckets * 8,
        .MaxStateBytes = maxStateBytes,
    };
}

// Sorted stream: 0,0,...,1,1,...,2,2,... with `perKey` copies each.
TVector<TString> MakeSortedStream(ui32 numKeys, ui32 perKey) {
    TVector<TString> keys(Reserve(static_cast<size_t>(numKeys) * perKey));
    for (ui32 keyIdx = 0; keyIdx < numKeys; ++keyIdx) {
        for (ui32 repIdx = 0; repIdx < perKey; ++repIdx) {
            keys.push_back(MakeKey(static_cast<i32>(keyIdx)));
        }
    }
    return keys;
}

TEqHeightHistogramBuilder BuildFromStream(const TVector<TString>& keys, const TEqHeightHistogramBuilder::TParams& params) {
    TEqHeightHistogramBuilder builder(params);
    for (const auto& key : keys) {
        builder.Add(key);
    }
    return builder;
}

// Split a sorted stream into numChunks contiguous chunks.
TVector<TVector<TString>> SplitContiguous(const TVector<TString>& keys, ui32 numChunks) {
    TVector<TVector<TString>> chunks(numChunks);
    size_t per = keys.size() / numChunks;
    size_t rem = keys.size() % numChunks;
    size_t idx = 0;
    for (ui32 chunkIdx = 0; chunkIdx < numChunks; ++chunkIdx) {
        size_t sz = per + (chunkIdx < rem ? 1 : 0);
        chunks[chunkIdx].assign(keys.begin() + idx, keys.begin() + idx + sz);
        idx += sz;
    }
    return chunks;
}

TEqHeightHistogramBuilder BuildChunk(const TVector<TString>& chunk, const TEqHeightHistogramBuilder::TParams& params) {
    return BuildFromStream(chunk, params);
}

// Merge in linear left-fold order.
TEqHeightHistogramBuilder MergeLinear(TVector<TEqHeightHistogramBuilder> states, const TEqHeightHistogramBuilder::TParams& params) {
    TEqHeightHistogramBuilder acc(params);
    for (auto& state : states) {
        acc.Merge(state);
    }
    return acc;
}

// Merge in a balanced binary tree.
TEqHeightHistogramBuilder MergeBalanced(TVector<TEqHeightHistogramBuilder> states, const TEqHeightHistogramBuilder::TParams& params) {
    if (states.empty()) {
        return TEqHeightHistogramBuilder(params);
    }
    while (states.size() > 1) {
        TVector<TEqHeightHistogramBuilder> next;
        for (size_t i = 0; i < states.size(); i += 2) {
            if (i + 1 < states.size()) {
                states[i].Merge(states[i + 1]);
                next.push_back(std::move(states[i]));
            } else {
                next.push_back(std::move(states[i]));
            }
        }
        states = std::move(next);
    }
    return std::move(states[0]);
}

// Merge in a random tree order.
TEqHeightHistogramBuilder MergeRandom(TVector<TEqHeightHistogramBuilder> states, const TEqHeightHistogramBuilder::TParams& params, ui64 seed) {
    if (states.empty()) {
        return TEqHeightHistogramBuilder(params);
    }
    TFastRng64 rng(seed);
    while (states.size() > 1) {
        size_t i = rng.Uniform(states.size());
        size_t j = rng.Uniform(states.size());
        if (i == j) {
            continue;
        }
        states[i].Merge(states[j]);
        states.erase(states.begin() + j);
    }
    return std::move(states[0]);
}

// Serialize/deserialize round-trip a builder.
TEqHeightHistogramBuilder RoundTrip(const TEqHeightHistogramBuilder& builder) {
    TEqHeightHistogramIntermediateState state = builder.Serialize();
    TString serialized = state.SerializeAsString();
    TEqHeightHistogramIntermediateState parsed;
    UNIT_ASSERT(parsed.ParseFromString(serialized));
    return TEqHeightHistogramBuilder(parsed);
}

// Assert each bucket's CumulativeCount is within MaxRankError and tolerance of its true rank.
void AssertTrueRanks(const TEqHeightHistogram& hist,
                     const TVector<TString>& allKeys,
                     ui64 maxRankError,
                     ui64 tolerance,
                     TStringBuf label) {
    TVector<TString> sorted = allKeys;
    Sort(sorted);
    for (size_t i = 0; i < hist.GetNumBuckets(); ++i) {
        const auto& bkt = hist.GetBucket(i);
        auto it = UpperBound(sorted.begin(), sorted.end(), bkt.UpperBound,
                             [](TStringBuf value, const TString& elem) { return value < TStringBuf(elem); });
        const ui64 trueRank = static_cast<ui64>(it - sorted.begin());
        const ui64 actual = bkt.CumulativeCount;
        const ui64 diff = (actual > trueRank) ? (actual - trueRank) : (trueRank - actual);
        UNIT_ASSERT_C(diff <= maxRankError,
                      label << ": bucket " << i << " cumulative " << actual
                            << " deviates from true rank " << trueRank << " by " << diff
                            << " > MaxRankError " << maxRankError);
        UNIT_ASSERT_C(diff <= tolerance,
                      label << ": bucket " << i << " cumulative " << actual
                            << " deviates from true rank " << trueRank << " by " << diff
                            << " > tolerance " << tolerance);
    }
}

// Assert each bucket's size deviates from ideal (totalCount/numBuckets) by at most CountCap.
void AssertBucketSizes(const TEqHeightHistogram& hist, ui64 totalCount, ui32 numBuckets, ui64 countCap, TStringBuf label) {
    const ui64 idealBucket = totalCount / numBuckets;
    for (size_t i = 0; i < hist.GetNumBuckets(); ++i) {
        ui64 prev = (i == 0) ? 0 : hist.GetBucket(i - 1).CumulativeCount;
        ui64 bucketSize = hist.GetBucket(i).CumulativeCount - prev;
        ui64 diff = (bucketSize > idealBucket) ? (bucketSize - idealBucket) : (idealBucket - bucketSize);
        UNIT_ASSERT_C(diff <= countCap,
                      label << ": bucket " << i << " size " << bucketSize
                            << " deviates from ideal " << idealBucket << " by " << diff
                            << " > CountCap " << countCap);
    }
}

// Assert each bucket's cumulative count is within `tolerance` of ideal.
void AssertCumulativeVsIdeal(const TEqHeightHistogram& hist, ui64 totalCount, ui32 numBuckets, ui64 tolerance, TStringBuf label) {
    for (size_t i = 0; i < hist.GetNumBuckets(); ++i) {
        ui64 ideal = (i + 1) * totalCount / numBuckets;
        ui64 actual = hist.GetBucket(i).CumulativeCount;
        ui64 diff = (actual > ideal) ? (actual - ideal) : (ideal - actual);
        UNIT_ASSERT_C(diff <= tolerance,
                      label << ": bucket " << i << " cumulative " << actual
                            << " deviates from ideal " << ideal << " by " << diff
                            << " > tolerance " << tolerance);
    }
}

// Helper for PartitionSeams tests: split, merge linearly, finalize, assert exactness and true ranks.
void RunPartitionSeamsTest(ui32 numKeys, ui32 perKey, ui32 numChunks, ui32 numBuckets, TStringBuf name) {
    const ui64 totalCount = static_cast<ui64>(numKeys) * perKey;
    auto keys = MakeSortedStream(numKeys, perKey);
    auto params = MakeParams(numBuckets);

    auto chunks = SplitContiguous(keys, numChunks);
    TVector<TEqHeightHistogramBuilder> states;
    for (const auto& chunk : chunks) {
        states.push_back(BuildChunk(chunk, params));
    }
    auto builder = MergeLinear(states, params);

    UNIT_ASSERT_VALUES_EQUAL_C(builder.GetTotalCount(), totalCount, name);

    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT_C(result->BucketsSize() > 0, name);
    TEqHeightHistogram hist(*result);
    UNIT_ASSERT_VALUES_EQUAL_C(hist.GetTotalCount(), totalCount, name);

    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(builder).IsExact(),
                  name << ": disjoint splice must be exact, MaxRankError = "
                       << TEqHeightHistogramBuilderTestApi(builder).GetMaxRankError());

    AssertBucketSizes(hist, totalCount, numBuckets, TEqHeightHistogramBuilderTestApi(builder).GetCountCap(), name);
    AssertTrueRanks(hist, keys, hist.GetMaxRankError(), 0, name);
}

} // namespace

Y_UNIT_TEST_SUITE(EqHeightHistogram) {

// Wrapped Bytes_ would look "small" and skip Compact; saturation must still force it.
Y_UNIT_TEST(ByteSizeSaturationForcesCompact) {
    auto params = MakeParams(10);
    TEqHeightHistogramBuilder builder(params);
    builder.Add(MakeKey(0));
    UNIT_ASSERT_C(!TEqHeightHistogramBuilderTestApi(builder).GetBudgetForced(),
                  "one tiny key must not force the byte budget");

    TEqHeightHistogramBuilderTestApi(builder).SetSummaryBytes(Max<ui64>());
    builder.Add(MakeKey(1));
    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(builder).GetBudgetForced(),
                  "SaturatedAddBytes must keep StateBytes at Max so Compact still runs");
}

Y_UNIT_TEST(EntryAndHeaderOverheadByteCounting) {
    TEqHeightHistogramIntermediateState emptyState;
    emptyState.SetBudgetForced(true);
    auto* emptyParams = emptyState.MutableParams();
    emptyParams->SetNumBuckets(Max<ui32>());
    emptyParams->SetEmissionRate(Max<ui32>());
    emptyParams->SetMaxStateBytes(Max<ui64>());
    emptyState.SetTotalCount(Max<ui64>());
    emptyState.SetMinKey(TString(1, '\0'));
    const ui64 emptySize = emptyState.ByteSizeLong();

    TEqHeightHistogramIntermediateState oneEntryState = emptyState;
    auto* entry = oneEntryState.AddEntries();
    entry->SetUpperBound(TString(1, '\0'));
    entry->SetCount(Max<ui64>());
    entry->SetRankUncertainty(Max<ui64>());
    entry->SetSingleKey(true);

    const ui64 entryOverhead = oneEntryState.ByteSizeLong() - emptySize;
    UNIT_ASSERT_GT(entryOverhead, 0u);
    UNIT_ASSERT_LT(entryOverhead, oneEntryState.ByteSizeLong());

    const ui64 headerOverhead = emptySize;
    UNIT_ASSERT_GT(headerOverhead, 0u);

    const ui32 numKeys = 8;
    auto paramsIn = MakeParams(4);
    TEqHeightHistogramBuilder builder(paramsIn);
    for (ui32 i = 0; i < numKeys; ++i) {
        builder.Add(MakeKey(static_cast<i32>(i)));
    }

    TEqHeightHistogramBuilderTestApi api(builder);
    const auto& entries = api.GetEntries();
    UNIT_ASSERT_VALUES_EQUAL(entries.size(), numKeys);

    ui64 expectedSummaryBytes = 0;
    for (const auto& e : entries) {
        expectedSummaryBytes += e.UpperBound.size() + entryOverhead;
    }
    UNIT_ASSERT_VALUES_EQUAL(api.GetSummaryBytes(), expectedSummaryBytes);

    const ui64 expectedStateBytes = expectedSummaryBytes + api.GetMinKey().size() + headerOverhead;
    UNIT_ASSERT_VALUES_EQUAL(api.GetStateBytes(), expectedStateBytes);

    const ui64 doubleCounted = expectedSummaryBytes + numKeys * headerOverhead + api.GetMinKey().size();
    UNIT_ASSERT_LT(api.GetStateBytes(), doubleCounted);
}

// Merge of identical UpperBounds adds RankUncertainty via CheckedAddCount.
Y_UNIT_TEST(MergeRankUncertaintyOverflow) {
    auto params = MakeParams(10);
    auto make = [&](ui64 rankUncertainty) {
        auto builder = BuildFromStream({MakeKey(0)}, params);
        auto state = builder.Serialize();
        state.MutableEntries(0)->SetRankUncertainty(rankUncertainty);
        return TEqHeightHistogramBuilder(state);
    };

    auto left = make(Max<ui64>());
    auto right = make(1);
    UNIT_ASSERT_EXCEPTION(left.Merge(right), yexception);
}

// Sorted single run — exact (RankUncertainty == 0)
Y_UNIT_TEST(SortedSingleRunExact) {
    const ui32 numKeys = 1000;
    const ui32 perKey = 10;
    const ui32 numBuckets = 20;
    const ui64 totalCount = static_cast<ui64>(numKeys) * perKey;
    auto keys = MakeSortedStream(numKeys, perKey);
    auto params = MakeParams(numBuckets);

    auto builder = BuildFromStream(keys, params);
    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(builder).IsExact(),
                  "sorted single run must be exact (RankUncertainty == 0), "
                  "MaxRankError = "
                      << TEqHeightHistogramBuilderTestApi(builder).GetMaxRankError());
    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);

    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT(result->BucketsSize() > 0);
    TEqHeightHistogram hist(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);
    UNIT_ASSERT_C(hist.IsExact(),
                  "finalized result must be exact for a sorted single run, "
                  "MaxRankError = "
                      << hist.GetMaxRankError());

    for (size_t i = 0; i < hist.GetNumBuckets(); ++i) {
        const auto& bkt = hist.GetBucket(i);
        if (i > 0) {
            UNIT_ASSERT(bkt.CumulativeCount > hist.GetBucket(i - 1).CumulativeCount);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(hist.GetBucket(hist.GetNumBuckets() - 1).CumulativeCount, totalCount);

    AssertTrueRanks(hist, keys, hist.GetMaxRankError(), 0, "SortedSingleRunExact");

    AssertBucketSizes(hist, totalCount, numBuckets, TEqHeightHistogramBuilderTestApi(builder).GetCountCap(), "SortedSingleRunExact");
}

// Disjoint-run splice — exact (RankUncertainty == 0)
Y_UNIT_TEST(DisjointRunSplice) {
    const ui32 numKeys = 1000;
    const ui32 perKey = 10;
    const ui32 numBuckets = 20;
    const ui64 totalCount = static_cast<ui64>(numKeys) * perKey;
    auto keys = MakeSortedStream(numKeys, perKey);
    auto params = MakeParams(numBuckets);

    // Single-run reference
    auto refBuilder = BuildFromStream(keys, params);
    auto refResult = refBuilder.Finalize();
    UNIT_ASSERT(refResult.Defined());
    UNIT_ASSERT(refResult->BucketsSize() > 0);
    TEqHeightHistogram refHist(*refResult);
    UNIT_ASSERT_VALUES_EQUAL(refHist.GetTotalCount(), totalCount);
    UNIT_ASSERT_C(refHist.IsExact(),
                  "ref: sorted single run must be exact, MaxRankError = "
                      << refHist.GetMaxRankError());

    // Semantic equivalence: same total/bucket count, each cumulative within CountCap.
    auto checkEquivalent = [&](const TEqHeightHistogramResult& result, const TString& label) {
        TEqHeightHistogram hist(result);
        UNIT_ASSERT_VALUES_EQUAL_C(hist.GetTotalCount(), refHist.GetTotalCount(), label);
        UNIT_ASSERT_VALUES_EQUAL_C(hist.GetNumBuckets(), refHist.GetNumBuckets(), label);
        const ui64 countCap = TEqHeightHistogramBuilderTestApi(refBuilder).GetCountCap();
        for (size_t i = 0; i < refHist.GetNumBuckets(); ++i) {
            ui64 refCum = refHist.GetBucket(i).CumulativeCount;
            ui64 histCum = hist.GetBucket(i).CumulativeCount;
            ui64 diff = (refCum > histCum) ? (refCum - histCum) : (histCum - refCum);
            UNIT_ASSERT_C(diff <= countCap,
                          label << ": bucket " << i << " cumulative " << histCum
                                << " deviates from ref " << refCum
                                << " by " << diff << " > CountCap " << countCap);
        }
    };

    // Split into numChunks=8 contiguous chunks, merge in different orders
    auto chunks = SplitContiguous(keys, 8);
    TVector<TEqHeightHistogramBuilder> states;
    for (const auto& chunk : chunks) {
        states.push_back(BuildChunk(chunk, params));
    }

    auto linearBuilder = MergeLinear(states, params);
    auto linearResult = linearBuilder.Finalize();
    UNIT_ASSERT(linearResult.Defined());
    UNIT_ASSERT(linearResult->BucketsSize() > 0);
    checkEquivalent(*linearResult, "linear");
    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(linearBuilder).IsExact(),
                  "linear: disjoint splice must be exact, MaxRankError = "
                      << TEqHeightHistogramBuilderTestApi(linearBuilder).GetMaxRankError());
    {
        TEqHeightHistogram hist(*linearResult);
        AssertTrueRanks(hist, keys, hist.GetMaxRankError(), 0, "linear");
    }

    // Rebuild states for balanced merge
    states.clear();
    for (const auto& chunk : chunks) {
        states.push_back(BuildChunk(chunk, params));
    }
    auto balancedBuilder = MergeBalanced(states, params);
    auto balancedResult = balancedBuilder.Finalize();
    UNIT_ASSERT(balancedResult.Defined());
    UNIT_ASSERT(balancedResult->BucketsSize() > 0);
    checkEquivalent(*balancedResult, "balanced");
    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(balancedBuilder).IsExact(),
                  "balanced: disjoint splice must be exact, MaxRankError = "
                      << TEqHeightHistogramBuilderTestApi(balancedBuilder).GetMaxRankError());
    {
        TEqHeightHistogram hist(*balancedResult);
        AssertTrueRanks(hist, keys, hist.GetMaxRankError(), 0, "balanced");
    }

    // Random merge of disjoint chunks: RankUncertainty stays 0 (no straddling).
    states.clear();
    for (const auto& chunk : chunks) {
        states.push_back(BuildChunk(chunk, params));
    }
    auto randomBuilder = MergeRandom(states, params, 42);
    auto randomResult = randomBuilder.Finalize();
    UNIT_ASSERT(randomResult.Defined());
    UNIT_ASSERT(randomResult->BucketsSize() > 0);
    checkEquivalent(*randomResult, "random");
    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(randomBuilder).IsExact(),
                  "random: disjoint splice must be exact, MaxRankError = "
                      << TEqHeightHistogramBuilderTestApi(randomBuilder).GetMaxRankError());
    {
        TEqHeightHistogram hist(*randomResult);
        AssertTrueRanks(hist, keys, hist.GetMaxRankError(), 0, "random");
    }
}

// Touching ranges — must interleave, not splice
Y_UNIT_TEST(TouchingRanges) {
    const ui32 numBuckets = 20;
    const ui64 totalCount = 1001; // 500 + 501 = 1001 (key 499 shared)
    auto params = MakeParams(numBuckets);

    // Two runs sharing key 499 at the boundary (builderA.MaxKey == builder.MinKey).
    TEqHeightHistogramBuilder builderA(params);
    for (ui32 keyIdx = 0; keyIdx < 500; ++keyIdx) {
        builderA.Add(MakeKey(static_cast<i32>(keyIdx)));
    }
    TEqHeightHistogramBuilder builder(params);
    for (ui32 keyIdx = 499; keyIdx < 1000; ++keyIdx) {
        builder.Add(MakeKey(static_cast<i32>(keyIdx)));
    }

    builderA.Merge(builder);
    UNIT_ASSERT_VALUES_EQUAL(builderA.GetTotalCount(), totalCount);

    auto result = builderA.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT(result->BucketsSize() > 0);
    TEqHeightHistogram hist(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);

    UNIT_ASSERT_C(hist.GetMaxRankError() <= totalCount / numBuckets,
                  "MaxRankError " << hist.GetMaxRankError() << " > total/numBuckets " << (totalCount / numBuckets));

    // True-rank assertion: shared key 499 at the frontier is handled by the equal-key fold.
    TVector<TString> allKeys;
    allKeys.reserve(totalCount);
    for (ui32 keyIdx = 0; keyIdx < 500; ++keyIdx) {
        allKeys.push_back(MakeKey(static_cast<i32>(keyIdx)));
    }
    for (ui32 keyIdx = 499; keyIdx < 1000; ++keyIdx) {
        allKeys.push_back(MakeKey(static_cast<i32>(keyIdx)));
    }
    AssertTrueRanks(hist, allKeys, hist.GetMaxRankError(), 0, "TouchingRanges");
}

// Multi-key frontier reproducer: one side's first entry is multi-key,
// the other ends on that key. The >= bump in InterleaveInto must fire.
Y_UNIT_TEST(TouchingRangesMultiKeyFrontier) {
    const ui32 numBuckets = 10;
    const ui32 perKey = 100;
    const ui32 aKeys = 200; // side A: keys 0..199
    const ui32 bKeys = 200; // side B: keys 0..199 (overlapping at key 0)
    const ui64 totalCount = static_cast<ui64>(aKeys + bKeys) * perKey;
    auto params = MakeParams(numBuckets);

    // Side A: keys 0..199 under tight budget, fusing keys 0 and 1 into a multi-key entry.
    auto tightParams = MakeParams(numBuckets, 1024);
    TEqHeightHistogramBuilder builderA(tightParams);
    for (ui32 keyIdx = 0; keyIdx < aKeys; ++keyIdx) {
        for (ui32 repIdx = 0; repIdx < perKey; ++repIdx) {
            builderA.Add(MakeKey(static_cast<i32>(keyIdx)));
        }
    }

    // A's first entry must be multi-key (fused by the tight budget).
    const auto& aEntries = TEqHeightHistogramBuilderTestApi(builderA).GetEntries();
    UNIT_ASSERT_C(!aEntries.empty(), "A must have at least one entry");
    UNIT_ASSERT_C(!aEntries.front().SingleKey,
                  "A's first entry must be multi-key (fused by tight budget), "
                  "got SingleKey with UpperBound "
                      << aEntries.front().UpperBound);
    UNIT_ASSERT_VALUES_EQUAL(TEqHeightHistogramBuilderTestApi(builderA).GetMinKey(), MakeKey(0));

    // Side B: keys 0..199 under normal budget, entries stay single-key.
    TEqHeightHistogramBuilder builder(params);
    for (ui32 keyIdx = 0; keyIdx < bKeys; ++keyIdx) {
        for (ui32 repIdx = 0; repIdx < perKey; ++repIdx) {
            builder.Add(MakeKey(static_cast<i32>(keyIdx)));
        }
    }
    const auto& bEntries = TEqHeightHistogramBuilderTestApi(builder).GetEntries();
    UNIT_ASSERT_C(bEntries.front().SingleKey,
                  "builderB's first entry must be single-key under a normal budget");

    // Merge: B.Merge(A). A's first entry starts at key 0 (== B's UpperBound).
    // The bump is owed because A's rows at key 0 are <= B's UpperBound.
    builder.Merge(builderA);
    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);

    // The merge must not be exact: A's multi-key entry contributes RankUncertainty.
    UNIT_ASSERT_C(!TEqHeightHistogramBuilderTestApi(builder).IsExact(),
                  "merge with a multi-key frontier at the MinKey must be inexact, "
                  "MaxRankError = "
                      << TEqHeightHistogramBuilderTestApi(builder).GetMaxRankError());

    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT_C(result->BucketsSize() > 0,
                  "Finalize must produce a histogram for the multi-key frontier case");
    TEqHeightHistogram hist(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);

    TVector<TString> allKeys;
    allKeys.reserve(totalCount);
    for (ui32 keyIdx = 0; keyIdx < aKeys; ++keyIdx) {
        for (ui32 repIdx = 0; repIdx < perKey; ++repIdx) {
            allKeys.push_back(MakeKey(static_cast<i32>(keyIdx)));
        }
    }
    for (ui32 keyIdx = 0; keyIdx < bKeys; ++keyIdx) {
        for (ui32 repIdx = 0; repIdx < perKey; ++repIdx) {
            allKeys.push_back(MakeKey(static_cast<i32>(keyIdx)));
        }
    }
    // True-rank error must be <= MaxRankError (Finalize already rejected anything larger).
    AssertTrueRanks(hist, allKeys, hist.GetMaxRankError(), hist.GetMaxRankError(),
                    "TouchingRangesMultiKeyFrontier");
}

// Partition seams — one test per chunk/bucket ratio

Y_UNIT_TEST(PartitionSeamsManyChunksGtBuckets) {
    RunPartitionSeamsTest(/*numKeys*/ 1000, /*perKey*/ 10, /*numChunks*/ 100, /*numBuckets*/ 10, "chunks>>buckets");
}

Y_UNIT_TEST(PartitionSeamsManyChunksEqBuckets) {
    RunPartitionSeamsTest(/*numKeys*/ 1000, /*perKey*/ 10, /*numChunks*/ 20, /*numBuckets*/ 20, "chunks~=buckets");
}

Y_UNIT_TEST(PartitionSeamsManyChunksLtBuckets) {
    RunPartitionSeamsTest(/*numKeys*/ 1000, /*perKey*/ 10, /*numChunks*/ 5, /*numBuckets*/ 20, "chunks<<buckets");
}

Y_UNIT_TEST(PartitionSeamsManyChunksOneRowPerChunk) {
    RunPartitionSeamsTest(/*numKeys*/ 1000, /*perKey*/ 10, /*numChunks*/ 1000, /*numBuckets*/ 10, "one_row_per_chunk");
}

// Partition seams — uneven chunk sizes
Y_UNIT_TEST(PartitionSeamsUneven) {
    const ui32 numKeys = 1000;
    const ui32 perKey = 10;
    const ui32 numBuckets = 10;
    const ui64 totalCount = static_cast<ui64>(numKeys) * perKey;
    auto keys = MakeSortedStream(numKeys, perKey);
    auto params = MakeParams(numBuckets);

    // Deliberately uneven chunks: 1%, 50%, 1%, 48%.
    TVector<TVector<TString>> chunks(4);
    size_t sizes[] = {totalCount / 100, totalCount / 2, totalCount / 100, 0};
    sizes[3] = keys.size() - sizes[0] - sizes[1] - sizes[2];
    size_t idx = 0;
    for (int chunkIdx = 0; chunkIdx < 4; ++chunkIdx) {
        chunks[chunkIdx].assign(keys.begin() + idx, keys.begin() + idx + sizes[chunkIdx]);
        idx += sizes[chunkIdx];
    }

    TVector<TEqHeightHistogramBuilder> states;
    for (const auto& chunk : chunks) {
        states.push_back(BuildChunk(chunk, params));
    }
    auto builder = MergeLinear(states, params);

    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);
    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(builder).IsExact(),
                  "uneven: disjoint splice must be exact, MaxRankError = "
                      << TEqHeightHistogramBuilderTestApi(builder).GetMaxRankError());

    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT(result->BucketsSize() > 0);
    TEqHeightHistogram hist(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);

    AssertBucketSizes(hist, totalCount, numBuckets, TEqHeightHistogramBuilderTestApi(builder).GetCountCap(), "uneven");
    AssertTrueRanks(hist, keys, hist.GetMaxRankError(), 0, "uneven");
}

// Many-way merge tree — exactness under random partitioning
Y_UNIT_TEST(ManyWayMergeTree) {
    const ui32 numKeys = 500;
    const ui32 perKey = 4;
    const ui32 numBuckets = 10;
    const ui64 totalCount = static_cast<ui64>(numKeys) * perKey;
    const ui32 numParts = 64;

    auto keys = MakeSortedStream(numKeys, perKey);
    auto params = MakeParams(numBuckets);

    TFastRng64 rng(12345);
    auto shuffled = keys;
    Shuffle(shuffled.begin(), shuffled.end(), rng);

    TVector<TVector<TString>> parts(numParts);
    for (size_t i = 0; i < shuffled.size(); ++i) {
        parts[i % numParts].push_back(shuffled[i]);
    }

    // 3 merge orders x 2 (with/without round-trip)
    const int Linear = 0, Balanced = 1, Random = 2;
    const int NoRoundTrip = 0, WithRoundTrip = 1;

    for (auto order : {Linear, Balanced, Random}) {
        for (auto roundTrip : {NoRoundTrip, WithRoundTrip}) {
            TVector<TEqHeightHistogramBuilder> states;
            for (const auto& part : parts) {
                auto state = BuildChunk(part, params);
                if (roundTrip == WithRoundTrip) {
                    state = RoundTrip(state);
                }
                states.push_back(std::move(state));
            }

            TEqHeightHistogramBuilder builder(params);
            if (order == Linear) {
                builder = MergeLinear(states, params);
            } else if (order == Balanced) {
                builder = MergeBalanced(states, params);
            } else {
                builder = MergeRandom(states, params, 99);
            }

            if (roundTrip == WithRoundTrip) {
                builder = RoundTrip(builder);
            }

            UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);

            auto result = builder.Finalize();
            UNIT_ASSERT(result.Defined());
            UNIT_ASSERT(result->BucketsSize() > 0);
            TEqHeightHistogram hist(*result);
            UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);

            // Tolerance: 2*totalCount/(fanout*numBuckets) = totalCount/(4*numBuckets).
            ui64 tolerance = totalCount / (4 * numBuckets);
            AssertCumulativeVsIdeal(hist, totalCount, numBuckets, tolerance,
                                    TStringBuf("order=") + ToString(order) + " roundTrip=" + ToString(roundTrip));
            AssertTrueRanks(hist, keys, hist.GetMaxRankError(), tolerance,
                            TStringBuf("order=") + ToString(order) + " roundTrip=" + ToString(roundTrip));
        }
    }
}

// Many-way merge with disjoint key ranges — exactness (RankUncertainty stays 0)
Y_UNIT_TEST(ManyWayMergeDisjointRanges) {
    const ui32 numBuckets = 10;
    const ui32 numParts = 32;
    const ui32 keysPerPart = 100;
    const ui32 perKey = 4;
    const ui64 totalCount = static_cast<ui64>(numParts) * keysPerPart * perKey;
    auto params = MakeParams(numBuckets);

    TVector<TVector<TString>> parts(numParts);
    for (ui32 partIdx = 0; partIdx < numParts; ++partIdx) {
        for (ui32 keyIdx = 0; keyIdx < keysPerPart; ++keyIdx) {
            TString key = MakeKey(static_cast<i32>(partIdx * keysPerPart + keyIdx));
            for (ui32 repIdx = 0; repIdx < perKey; ++repIdx) {
                parts[partIdx].push_back(key);
            }
        }
    }

    TVector<TEqHeightHistogramBuilder> states;
    for (const auto& part : parts) {
        states.push_back(BuildChunk(part, params));
    }
    auto builder = MergeRandom(states, params, 7);

    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);

    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(builder).IsExact(),
                  "disjoint ranges must stay exact, MaxRankError = "
                      << TEqHeightHistogramBuilderTestApi(builder).GetMaxRankError());

    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT(result->BucketsSize() > 0);
    TEqHeightHistogram hist(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);

    const ui64 tolerance = totalCount / (4 * numBuckets);
    AssertCumulativeVsIdeal(hist, totalCount, numBuckets, tolerance, "disjoint");

    TVector<TString> allKeys;
    allKeys.reserve(static_cast<size_t>(numParts) * keysPerPart * perKey);
    for (ui32 partIdx = 0; partIdx < numParts; ++partIdx) {
        for (ui32 keyIdx = 0; keyIdx < keysPerPart; ++keyIdx) {
            TString key = MakeKey(static_cast<i32>(partIdx * keysPerPart + keyIdx));
            for (ui32 repIdx = 0; repIdx < perKey; ++repIdx) {
                allKeys.push_back(key);
            }
        }
    }
    AssertTrueRanks(hist, allKeys, hist.GetMaxRankError(), tolerance, "disjoint");
}

// Empty-state identity
Y_UNIT_TEST(EmptyStateIdentity) {
    auto params = MakeParams(10);

    TEqHeightHistogramBuilder empty1(params);
    TEqHeightHistogramBuilder empty2(params);
    UNIT_ASSERT(!empty1.Finalize().Defined());
    UNIT_ASSERT(!empty2.Finalize().Defined());

    TVector<TString> keys = MakeSortedStream(100, 5);
    auto builder = BuildFromStream(keys, params);
    auto builderResult = builder.Finalize();
    UNIT_ASSERT(builderResult.Defined());
    UNIT_ASSERT(builderResult->BucketsSize() > 0);

    {
        TEqHeightHistogramBuilder acc(params);
        acc.Merge(builder);
        auto accResult = acc.Finalize();
        UNIT_ASSERT(accResult.Defined());
        UNIT_ASSERT(accResult->BucketsSize() > 0);
        UNIT_ASSERT_VALUES_EQUAL(builderResult->SerializeAsString(), accResult->SerializeAsString());
    }

    {
        TEqHeightHistogramBuilder acc = BuildFromStream(keys, params);
        TEqHeightHistogramBuilder empty(params);
        acc.Merge(empty);
        auto accResult = acc.Finalize();
        UNIT_ASSERT(accResult.Defined());
        UNIT_ASSERT(accResult->BucketsSize() > 0);
        UNIT_ASSERT_VALUES_EQUAL(builderResult->SerializeAsString(), accResult->SerializeAsString());
    }
}

// Merge rejects mismatched NumBuckets / EmissionRate
Y_UNIT_TEST(MergeRejectsMismatchedParams) {
    auto builderA = BuildFromStream(MakeSortedStream(10, 1), MakeParams(10));
    auto builder = BuildFromStream(MakeSortedStream(10, 1), MakeParams(20));
    UNIT_ASSERT_EXCEPTION(builderA.Merge(builder), yexception);

    auto p1 = MakeParams(10);
    auto p2 = p1;
    p2.EmissionRate = p1.EmissionRate + 1;
    auto builderC = BuildFromStream(MakeSortedStream(10, 1), p1);
    auto builderD = BuildFromStream(MakeSortedStream(10, 1), p2);
    UNIT_ASSERT_EXCEPTION(builderC.Merge(builderD), yexception);

    auto p3 = p1;
    p3.MaxStateBytes = p1.MaxStateBytes / 2;
    auto builderE = BuildFromStream(MakeSortedStream(10, 1), p1);
    auto builderF = BuildFromStream(MakeSortedStream(10, 1), p3);
    builderE.Merge(builderF);
    UNIT_ASSERT_VALUES_EQUAL(builderE.GetTotalCount(), 20U);
}

// Skew — heavy hitter gets its own bucket
Y_UNIT_TEST(Skew) {
    const ui32 numBuckets = 20;
    const ui64 totalCount = 1000;
    auto params = MakeParams(numBuckets);

    TEqHeightHistogramBuilder builder(params);
    for (ui32 i = 0; i < 300; ++i) {
        builder.Add(MakeKey(42)); // heavy hitter
    }
    for (ui32 i = 0; i < 700; ++i) {
        builder.Add(MakeKey(static_cast<i32>(1000 + i))); // unique keys
    }

    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);

    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT(result->BucketsSize() > 0);
    TEqHeightHistogram hist(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);

    TString key42 = MakeKey(42);
    bool found = false;
    for (size_t i = 0; i < hist.GetNumBuckets(); ++i) {
        const auto& bkt = hist.GetBucket(i);
        if (bkt.UpperBound == key42) {
            ui64 prev = (i == 0) ? 0 : hist.GetBucket(i - 1).CumulativeCount;
            ui64 bucketSize = bkt.CumulativeCount - prev;
            UNIT_ASSERT_VALUES_EQUAL(bucketSize, 300);
            found = true;
            break;
        }
    }
    UNIT_ASSERT_C(found, "heavy hitter key not found as a bucket boundary");

    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(builder).GetMaxRankError() == 0,
                  "sorted skew must be exact (RankUncertainty == 0), MaxRankError = "
                      << TEqHeightHistogramBuilderTestApi(builder).GetMaxRankError());
}

// Duplicate across a group boundary
Y_UNIT_TEST(DuplicateAcrossGroupBoundary) {
    const ui32 numBuckets = 5;
    const ui64 totalCount = 1000;
    auto params = MakeParams(numBuckets);

    // Key 0 repeated 50 times (CountCap = 25), spanning two groups.
    TEqHeightHistogramBuilder builder(params);
    for (ui32 i = 0; i < 50; ++i) {
        builder.Add(MakeKey(0));
    }
    for (ui32 i = 1; i < 951; ++i) {
        builder.Add(MakeKey(static_cast<i32>(i)));
    }

    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);

    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT(result->BucketsSize() > 0);
    TEqHeightHistogram hist(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);

    for (size_t i = 1; i < hist.GetNumBuckets(); ++i) {
        UNIT_ASSERT_C(hist.GetBucket(i).UpperBound > hist.GetBucket(i - 1).UpperBound,
                      "bucket bounds not strictly increasing at " << i);
    }

    // Mid-key-split regression: duplicate key must not close an entry mid-run.
    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(builder).IsExact(),
                  "sorted duplicate-across-boundary must be exact (RankUncertainty == 0), "
                  "MaxRankError = "
                      << TEqHeightHistogramBuilderTestApi(builder).GetMaxRankError());
    UNIT_ASSERT_C(hist.IsExact(),
                  "finalized result must be exact for sorted duplicates, "
                  "MaxRankError = "
                      << hist.GetMaxRankError());
}

// Small domain — MIN_ENTRIES fires on BudgetForced, not on small data.
Y_UNIT_TEST(SmallDomain) {
    const ui32 numBuckets = 100;
    const ui64 totalCount = 30;
    auto params = MakeParams(numBuckets);

    TEqHeightHistogramBuilder builder(params);
    for (ui32 i = 0; i < 10; ++i) {
        builder.Add(MakeKey(0));
    }
    for (ui32 i = 0; i < 10; ++i) {
        builder.Add(MakeKey(1));
    }
    for (ui32 i = 0; i < 10; ++i) {
        builder.Add(MakeKey(2));
    }

    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT_C(result->BucketsSize() > 0,
                  "small domain must produce a histogram, not nullopt; "
                  "MIN_ENTRIES should only reject budget-starved states");
    TEqHeightHistogram hist(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetNumBuckets(), 3u);
    UNIT_ASSERT(hist.IsExact());
    UNIT_ASSERT_VALUES_EQUAL(hist.GetBucket(hist.GetNumBuckets() - 1).CumulativeCount, totalCount);
}

// Byte budget — 4 KB keys with small budget
Y_UNIT_TEST(ByteBudget) {
    const ui32 numBuckets = 10;
    const ui64 totalCount = 1000;
    const ui64 maxBytes = 4096;
    auto params = MakeParams(numBuckets, maxBytes);

    TEqHeightHistogramBuilder builder(params);
    for (ui32 i = 0; i < totalCount; ++i) {
        TString bigKey(4095, 'x');
        bigKey[0] = static_cast<char>(i & 0xFF);
        bigKey[1] = static_cast<char>((i >> 8) & 0xFF);
        builder.Add(bigKey);
    }

    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);

    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(builder).GetBudgetForced(),
                  "byte budget should have forced compaction (BudgetForced must be set)");

    auto result = builder.Finalize();
    // 4095-byte keys vs SoftBudget=2048: compaction bottoms out at one entry, Finalize rejects.
    UNIT_ASSERT_C(!result.Defined(),
                  "Finalize should return Nothing: BudgetForced with a single "
                  "entry is rejected (< MIN_ENTRIES)");
}

// Degenerate budget — Finalize returns nullopt
Y_UNIT_TEST(DegenerateBudget) {
    const ui32 numBuckets = 10;
    const ui64 totalCount = 1000;
    const ui64 maxBytes = 100;
    auto params = MakeParams(numBuckets, maxBytes);

    TEqHeightHistogramBuilder builder(params);
    for (ui32 i = 0; i < totalCount; ++i) {
        TString key = MakeKey(static_cast<i32>(i));
        builder.Add(key);
    }

    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);

    auto result = builder.Finalize();
    UNIT_ASSERT(!result.Defined());
}

// Degenerate budget — one key exceeds MaxStateBytes
Y_UNIT_TEST(OneKeyExceedsBudget) {
    const ui32 numBuckets = 10;
    const ui64 totalCount = 100;
    const ui64 maxBytes = 50;
    auto params = MakeParams(numBuckets, maxBytes);

    TEqHeightHistogramBuilder builder(params);
    TString hugeKey(100, 'z');
    builder.Add(hugeKey);
    for (ui32 i = 0; i < totalCount - 1; ++i) {
        builder.Add(MakeKey(static_cast<i32>(i)));
    }

    auto result = builder.Finalize();
    UNIT_ASSERT(!result.Defined());
}

// Single key larger than MaxStateBytes — no crash, no corruption
Y_UNIT_TEST(SingleKeyLargerThanMaxStateBytes) {
    const ui32 numBuckets = 10;
    const ui64 maxBytes = DEFAULT_MAX_STATE_BYTES; // 4 MB
    auto params = MakeParams(numBuckets, maxBytes);

    TEqHeightHistogramBuilder builder(params);
    // 10 MB key — 2.5x larger than the entire MaxStateBytes budget.
    TString hugeKey(HUGE_KEY_BYTES, 'Z');
    hugeKey[0] = '\x01'; // below MakeKey range for sorted ordering
    builder.Add(hugeKey);

    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), 1u);

    // The huge key survives: AddSorted skips Compact on the first key.
    const auto& entries = TEqHeightHistogramBuilderTestApi(builder).GetEntries();
    UNIT_ASSERT_VALUES_EQUAL(entries.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(entries[0].UpperBound, hugeKey);
    UNIT_ASSERT_VALUES_EQUAL(entries[0].Count, 1u);
    UNIT_ASSERT(entries[0].SingleKey);

    // Round-trip must not crash and must preserve TotalCount and the key.
    auto roundTrip = RoundTrip(builder);
    UNIT_ASSERT_VALUES_EQUAL(roundTrip.GetTotalCount(), 1u);
    const auto& roundTripEntries = TEqHeightHistogramBuilderTestApi(roundTrip).GetEntries();
    UNIT_ASSERT_VALUES_EQUAL(roundTripEntries.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(roundTripEntries[0].UpperBound, hugeKey);

    // Finalize rejects: 1 entry < MIN_ENTRIES.
    auto result = builder.Finalize();
    UNIT_ASSERT_C(!result.Defined(),
                  "Finalize must return Nothing for a single over-budget entry");
}

// Single key larger than MaxStateBytes plus small keys — no crash
Y_UNIT_TEST(SingleKeyLargerThanMaxStateBytesWithSmallKeys) {
    const ui32 numBuckets = 10;
    const ui64 totalCount = 1000;
    const ui64 maxBytes = DEFAULT_MAX_STATE_BYTES; // 4 MB
    auto params = MakeParams(numBuckets, maxBytes);

    TEqHeightHistogramBuilder builder(params);
    // 10 MB MinKey: overhead keeps StateBytes > SoftBudget, so Compact fuses to 1 entry on every Add.
    TString hugeKey(HUGE_KEY_BYTES, 'Z');
    hugeKey[0] = '\x01'; // below MakeKey range, so sorted: hugeKey, MakeKey(0), ...
    builder.Add(hugeKey);
    for (ui32 i = 0; i < totalCount - 1; ++i) {
        builder.Add(MakeKey(static_cast<i32>(i)));
    }

    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);

    // Round-trip must not crash and must preserve TotalCount.
    auto roundTrip = RoundTrip(builder);
    UNIT_ASSERT_VALUES_EQUAL(roundTrip.GetTotalCount(), totalCount);

    // Finalize rejects: huge MinKey overhead forces 1 entry (< MIN_ENTRIES).
    auto result = builder.Finalize();
    UNIT_ASSERT_C(!result.Defined(),
                  "Finalize must return Nothing: huge MinKey overhead prevents "
                  "keeping enough entries");
}

// Round-trip — intermediate state
Y_UNIT_TEST(RoundTripIntermediateState) {
    const ui32 numBuckets = 20;
    auto params = MakeParams(numBuckets);

    auto keys = MakeSortedStream(100, 10);
    auto builder = BuildFromStream(keys, params);

    TEqHeightHistogramIntermediateState serialized = builder.Serialize();
    TEqHeightHistogramBuilder deserialized(serialized);

    UNIT_ASSERT_VALUES_EQUAL(deserialized.GetTotalCount(), builder.GetTotalCount());
    UNIT_ASSERT_VALUES_EQUAL(TEqHeightHistogramBuilderTestApi(deserialized).GetCountCap(), TEqHeightHistogramBuilderTestApi(builder).GetCountCap());

    const auto& origEntries = TEqHeightHistogramBuilderTestApi(builder).GetEntries();
    const auto& deserEntries = TEqHeightHistogramBuilderTestApi(deserialized).GetEntries();
    UNIT_ASSERT_VALUES_EQUAL(deserEntries.size(), origEntries.size());

    for (size_t i = 0; i < origEntries.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(deserEntries[i].UpperBound, origEntries[i].UpperBound);
        UNIT_ASSERT_VALUES_EQUAL(deserEntries[i].Count, origEntries[i].Count);
        UNIT_ASSERT_VALUES_EQUAL(deserEntries[i].RankUncertainty, origEntries[i].RankUncertainty);
        UNIT_ASSERT_VALUES_EQUAL(deserEntries[i].SingleKey, origEntries[i].SingleKey);
    }

    auto result1 = builder.Finalize();
    auto result2 = deserialized.Finalize();
    UNIT_ASSERT(result1.Defined());
    UNIT_ASSERT(result2.Defined());
    UNIT_ASSERT(result1->BucketsSize() > 0);
    UNIT_ASSERT(result2->BucketsSize() > 0);
    UNIT_ASSERT_VALUES_EQUAL(result1->SerializeAsString(), result2->SerializeAsString());
}

// Round-trip — final result
Y_UNIT_TEST(RoundTripFinalResult) {
    const ui32 numBuckets = 20;
    const ui64 totalCount = 1000;
    auto params = MakeParams(numBuckets);

    auto keys = MakeSortedStream(100, 10);
    auto builder = BuildFromStream(keys, params);
    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT(result->BucketsSize() > 0);

    TEqHeightHistogram hist1(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist1.GetTotalCount(), totalCount);
    UNIT_ASSERT_VALUES_EQUAL(hist1.GetMaxRankError(), 0);
    UNIT_ASSERT(hist1.IsExact());

    TEqHeightHistogram hist2(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist2.GetTotalCount(), hist1.GetTotalCount());
    UNIT_ASSERT_VALUES_EQUAL(hist2.GetNumBuckets(), hist1.GetNumBuckets());
    for (size_t i = 0; i < hist1.GetNumBuckets(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(hist2.GetBucket(i).UpperBound, hist1.GetBucket(i).UpperBound);
        UNIT_ASSERT_VALUES_EQUAL(hist2.GetBucket(i).CumulativeCount, hist1.GetBucket(i).CumulativeCount);
    }
}

// EstimateLessOrEqual
Y_UNIT_TEST(EstimateLessOrEqual) {
    const ui32 numBuckets = 10;
    const ui64 totalCount = 1000;
    auto params = MakeParams(numBuckets);

    auto keys = MakeSortedStream(100, 10);
    auto builder = BuildFromStream(keys, params);
    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT(result->BucketsSize() > 0);
    TEqHeightHistogram hist(*result);

    // Key 50 (true rank 510): bucket boundary at key 49 (cumulative 500), a lower bound within totalCount/numBuckets.
    const ui64 trueRank50 = 510;
    ui64 est = hist.EstimateLessOrEqual(MakeKey(50));
    UNIT_ASSERT_C(est <= trueRank50, "estimate " << est << " > true rank " << trueRank50);
    UNIT_ASSERT_C(trueRank50 - est <= totalCount / numBuckets,
                  "estimate " << est << " deviates from true rank " << trueRank50
                              << " by " << (trueRank50 - est) << " > totalCount/numBuckets " << (totalCount / numBuckets));

    UNIT_ASSERT_VALUES_EQUAL(hist.EstimateLessOrEqual(MakeKey(-1)), 0);
    UNIT_ASSERT_VALUES_EQUAL(hist.EstimateLessOrEqual(MakeKey(200)), totalCount);
}

// String keys — order preservation
Y_UNIT_TEST(StringKeys) {
    const ui32 numBuckets = 10;
    const ui64 totalCount = 300;
    auto params = MakeParams(numBuckets);

    TEqHeightHistogramBuilder builder(params);
    // 2-byte keys "XY": X advances every 10, Y cycles within.  Printable ASCII only.
    for (int i = 0; i < 100; ++i) {
        TString key;
        key.push_back(static_cast<char>('a' + (i / 10)));
        key.push_back(static_cast<char>('a' + (i % 10)));
        for (int repIdx = 0; repIdx < 3; ++repIdx) {
            builder.Add(key);
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);
    UNIT_ASSERT(TEqHeightHistogramBuilderTestApi(builder).IsExact());

    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT(result->BucketsSize() > 0);
    TEqHeightHistogram hist(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);
    UNIT_ASSERT(hist.IsExact());

    // "ea" (index 40, true rank 123): lower bound within totalCount/numBuckets.
    const ui64 trueRankEA = 123;
    ui64 est = hist.EstimateLessOrEqual("ea");
    UNIT_ASSERT_C(est <= trueRankEA, "estimate " << est << " > true rank " << trueRankEA);
    UNIT_ASSERT_C(trueRankEA - est <= totalCount / numBuckets,
                  "estimate " << est << " deviates from true rank " << trueRankEA
                              << " by " << (trueRankEA - est) << " > totalCount/numBuckets " << (totalCount / numBuckets));
}

// Variable-length string keys on the sorted path — EntriesBytes underflow
// AddSorted must not underflow when a shorter key follows a longer one ("bb" -> "c").
Y_UNIT_TEST(VariableLengthStringKeysSorted) {
    TEqHeightHistogramBuilder::TParams params;
    params.NumBuckets = 10;
    params.EmissionRate = 80;
    params.MaxStateBytes = DEFAULT_MAX_STATE_BYTES;

    TEqHeightHistogramBuilder builder(params);
    // Pad Cap() so "bb" extends rather than PushEntry: 1000 "aaa" → Cap = 12.
    for (int i = 0; i < 1000; ++i) {
        builder.Add("aaa");
    }
    builder.Add("bb"); // Count 1, SingleKey
    // "c" is shorter than "bb" and last.Count < Cap, so this extends.
    // Regression: old add-then-subtract wrapped Bytes to ~2^64.
    builder.Add("c");

    const auto& entries = TEqHeightHistogramBuilderTestApi(builder).GetEntries();
    UNIT_ASSERT_VALUES_EQUAL(entries.size(), 2U);
    UNIT_ASSERT_VALUES_EQUAL(entries[0].UpperBound, "aaa");
    UNIT_ASSERT_VALUES_EQUAL(entries[0].Count, 1000U);
    UNIT_ASSERT_VALUES_EQUAL(entries[1].UpperBound, "c");
    UNIT_ASSERT_VALUES_EQUAL(entries[1].Count, 2U);
    UNIT_ASSERT(!entries[1].SingleKey);

    const ui32 perKey = 10;
    TVector<TString> rest = {
        "cc",
        "ccc",
        "d",
        "dd",
        "ddd",
        "e",
        "ee",
        "eee",
        "f",
        "ff",
        "fff",
        "g",
        "gg",
        "ggg",
        "h",
    };
    for (const auto& key : rest) {
        for (ui32 repIdx = 0; repIdx < perKey; ++repIdx) {
            builder.Add(key);
        }
    }

    const ui64 totalCount = builder.GetTotalCount();
    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(builder).IsExact(), "variable-length sorted string keys must stay exact");

    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT_C(result->BucketsSize() > 0,
                  "Finalize must produce a histogram for valid sorted string-keyed data");
    TEqHeightHistogram hist(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);
    UNIT_ASSERT(hist.IsExact());

    for (size_t i = 1; i < hist.GetNumBuckets(); ++i) {
        UNIT_ASSERT_C(hist.GetBucket(i).UpperBound > hist.GetBucket(i - 1).UpperBound,
                      "bucket bounds not strictly increasing at " << i);
    }
    UNIT_ASSERT_VALUES_EQUAL(hist.GetBucket(hist.GetNumBuckets() - 1).CumulativeCount, totalCount);
}

// BudgetForced with enough entries — Finalize produces a usable histogram
Y_UNIT_TEST(BudgetForcedEnoughEntries) {
    const ui32 numBuckets = 10;
    const ui64 totalCount = 100000;
    // SoftBudget=2048, ~32 bytes/entry -> ~80 before compaction, ~40 survive.
    const ui64 maxBytes = 4096;
    auto params = MakeParams(numBuckets, maxBytes);

    TEqHeightHistogramBuilder builder(params);
    auto keys = MakeSortedStream(10000, 10);
    for (const auto& key : keys) {
        builder.Add(key);
    }

    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);

    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(builder).GetBudgetForced(),
                  "byte budget should have forced compaction (BudgetForced must be set)");

    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(builder).GetEntries().size() >= TEqHeightHistogramBuilder::MIN_ENTRIES,
                  "expected >= MIN_ENTRIES entries, got " << TEqHeightHistogramBuilderTestApi(builder).GetEntries().size());

    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT_C(result->BucketsSize() > 0,
                  "Finalize must produce a histogram when BudgetForced but >= MIN_ENTRIES entries survive");

    TEqHeightHistogram hist(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);

    // Sorted-path compaction stays exact: RankUncertainty_fused = 0.
    UNIT_ASSERT_C(hist.IsExact(),
                  "sorted-path compaction must stay exact (RankUncertainty == 0), "
                  "MaxRankError = "
                      << hist.GetMaxRankError());
    UNIT_ASSERT_VALUES_EQUAL(hist.GetMaxRankError(), 0);

    UNIT_ASSERT_VALUES_EQUAL(hist.GetBucket(hist.GetNumBuckets() - 1).CumulativeCount, totalCount);
}

// Round-trip preserves BudgetForced flag
// Flags must survive serialization: partial states ship between merge stages.
Y_UNIT_TEST(RoundTripPreservesFlags) {
    const ui32 numBuckets = 10;
    const ui64 totalCount = 1000;

    // --- BudgetForced: huge key → single entry → nullopt ---
    {
        const ui64 maxBytes = 50;
        auto params = MakeParams(numBuckets, maxBytes);

        TEqHeightHistogramBuilder builder(params);
        TString hugeKey(100, 'z');
        builder.Add(hugeKey);
        for (ui32 i = 0; i < totalCount - 1; ++i) {
            builder.Add(MakeKey(static_cast<i32>(i)));
        }

        UNIT_ASSERT(!builder.Finalize().Defined());

        auto roundTrip = RoundTrip(builder);
        UNIT_ASSERT_C(!roundTrip.Finalize().Defined(),
                      "BudgetForced flag lost in round-trip: Finalize produced a histogram");
    }

    // --- BudgetForced (< MIN_ENTRIES → empty) ---
    {
        const ui64 maxBytes = 200;
        auto params = MakeParams(numBuckets, maxBytes);

        TEqHeightHistogramBuilder builder(params);
        for (ui32 i = 0; i < totalCount; ++i) {
            builder.Add(MakeKey(static_cast<i32>(i)));
        }

        UNIT_ASSERT(!builder.Finalize().Defined());

        auto roundTrip = RoundTrip(builder);
        UNIT_ASSERT_C(!roundTrip.Finalize().Defined(),
                      "BudgetForced flag lost in round-trip: Finalize produced a histogram");
    }
}

// Byte-path forced fusion in Compact
// When no pair is admissible, Compact fuses the lightest pair regardless.

Y_UNIT_TEST(BytePathForcedFusion) {
    const ui32 numBuckets = 10;
    const ui64 totalCount = 10000;
    // Budget sized so >= MIN_ENTRIES survive after forced fusion.
    const ui64 maxBytes = 1200;
    auto params = MakeParams(numBuckets, maxBytes);

    TEqHeightHistogramBuilder builder(params);
    // Extreme skew: 2 heavy hitters + 3000 light keys. Light entries fuse first, then heavy entries force FuseLightestPair().
    for (ui32 i = 0; i < 4000; ++i) {
        builder.Add(MakeKey(0));
    }
    for (ui32 i = 0; i < 3000; ++i) {
        builder.Add(MakeKey(1));
    }
    for (ui32 i = 0; i < 3000; ++i) {
        builder.Add(MakeKey(static_cast<i32>(100 + i)));
    }

    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);

    const auto& entries = TEqHeightHistogramBuilderTestApi(builder).GetEntries();
    UNIT_ASSERT_C(!entries.empty(), "compaction should leave at least one entry");

    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(builder).GetBudgetForced(),
                  "byte budget should have forced compaction (BudgetForced must be set)");

    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT_C(result->BucketsSize() > 0,
                  "Finalize must produce a histogram after byte-path forced fusion");

    TEqHeightHistogram hist(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);

    // Sorted-path forced fusion stays exact: RankUncertainty_fused = 0.
    UNIT_ASSERT_C(hist.IsExact(),
                  "sorted-path forced fusion must stay exact (RankUncertainty == 0), "
                  "MaxRankError = "
                      << hist.GetMaxRankError());
    UNIT_ASSERT_VALUES_EQUAL(hist.GetMaxRankError(), 0);

    UNIT_ASSERT_VALUES_EQUAL(hist.GetBucket(hist.GetNumBuckets() - 1).CumulativeCount, totalCount);
}

// Malformed result — EmissionRate == 0 must be rejected
// EmissionRate is a divisor in Cap(); zero must throw, not SIGFPE later.
Y_UNIT_TEST(MalformedResultZeroEmissionRate) {
    const ui32 numBuckets = 10;
    auto params = MakeParams(numBuckets);

    auto keys = MakeSortedStream(100, 10);
    auto builder = BuildFromStream(keys, params);
    TEqHeightHistogramIntermediateState state = builder.Serialize();

    // Corrupt EmissionRate to zero — the constructor must reject it.
    state.MutableParams()->SetEmissionRate(0);
    UNIT_ASSERT_EXCEPTION(TEqHeightHistogramBuilder(state), yexception);
}

// Unsorted input forces fusion — RankUncertainty machinery exercised
Y_UNIT_TEST(UnsortedFusionProducesRankUncertainty) {
    const ui32 numBuckets = 10;
    const ui32 numKeys = 20000;
    const ui64 totalCount = numKeys;
    const ui64 maxBytes = 64 * 1024;
    auto params = MakeParams(numBuckets, maxBytes);

    TFastRng64 rng(7);
    auto keys = MakeSortedStream(numKeys, 1);
    Shuffle(keys.begin(), keys.end(), rng);

    auto builder = BuildFromStream(keys, params);
    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);

    const auto& entries = TEqHeightHistogramBuilderTestApi(builder).GetEntries();
    UNIT_ASSERT_C(entries.size() < numKeys,
                  "expected fusion under a tight budget, got " << entries.size()
                                                               << " entries for " << numKeys << " keys");

    // At least one multi-key entry (the only way RankUncertainty accrues).
    bool hasMultiKey = false;
    for (const auto& entry : entries) {
        if (!entry.SingleKey) {
            hasMultiKey = true;
            break;
        }
    }
    UNIT_ASSERT_C(hasMultiKey,
                  "expected at least one multi-key entry after fusion, "
                  "but all entries have SingleKey == true");

    // RankUncertainty must be nonzero somewhere.
    bool hasRankUncertainty = false;
    for (const auto& entry : entries) {
        if (entry.RankUncertainty > 0) {
            hasRankUncertainty = true;
            break;
        }
    }
    UNIT_ASSERT_C(hasRankUncertainty,
                  "expected RankUncertainty > 0 after unsorted fusion, but all entries "
                  "have RankUncertainty == 0 — the approximation mechanism is unexercised");

    const ui64 maxRankError = TEqHeightHistogramBuilderTestApi(builder).GetMaxRankError();
    UNIT_ASSERT_C(maxRankError > 0,
                  "expected MaxRankError > 0 when RankUncertainty > 0, got 0");
    UNIT_ASSERT_C(maxRankError <= totalCount / numBuckets,
                  "MaxRankError " << maxRankError << " > total/numBuckets " << (totalCount / numBuckets));

    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT(result->BucketsSize() > 0);
    TEqHeightHistogram hist(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);
    UNIT_ASSERT_C(!hist.IsExact(),
                  "unsorted fusion must be approximate, not exact");

    AssertTrueRanks(hist, keys, hist.GetMaxRankError(), totalCount / (4 * numBuckets), "UnsortedFusionProducesRankUncertainty");
}

// Finalize guard rail — rejects inexact small tables
// With total < numBuckets, total/numBuckets == 0, so any nonzero error rejects.
Y_UNIT_TEST(FinalizeGuardRailSmallTable) {
    const ui32 numBuckets = 100;
    const ui32 numKeys = 40;
    const ui64 totalCount = numKeys;
    const ui64 maxBytes = 256;
    auto params = MakeParams(numBuckets, maxBytes);

    TFastRng64 rng(31);
    auto keys = MakeSortedStream(numKeys, 1);
    Shuffle(keys.begin(), keys.end(), rng);

    auto builder = BuildFromStream(keys, params);
    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);

    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(builder).GetBudgetForced(),
                  "byte budget should have forced compaction");

    // Rejected by either the guard rail or BudgetForced + too-few-entries check.
    auto result = builder.Finalize();
    UNIT_ASSERT_C(!result.Defined(),
                  "small inexact table under tight budget must not finalize");
}

// Finalize guard rail: corrupt RankUncertainty to exceed total/numBuckets, verify Finalize rejects.
Y_UNIT_TEST(FinalizeGuardRailLargeError) {
    const ui32 numBuckets = 10;
    const ui32 numKeys = 20;
    const ui32 perKey = 50;
    const ui64 totalCount = static_cast<ui64>(numKeys) * perKey;
    auto params = MakeParams(numBuckets);

    auto keys = MakeSortedStream(numKeys, perKey);
    auto builder = BuildFromStream(keys, params);
    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);
    UNIT_ASSERT_C(!TEqHeightHistogramBuilderTestApi(builder).GetBudgetForced(), "BudgetForced must be false");
    UNIT_ASSERT_VALUES_EQUAL(TEqHeightHistogramBuilderTestApi(builder).GetEntries().size(), numKeys);

    TEqHeightHistogramIntermediateState state = builder.Serialize();

    // Corrupt every entry's RankUncertainty to exceed total/numBuckets.
    const ui64 totalOverB = totalCount / numBuckets;
    const ui64 largeRankUncertainty = totalOverB + 50;
    for (auto& entry : *state.MutableEntries()) {
        entry.SetRankUncertainty(largeRankUncertainty);
    }

    TEqHeightHistogramBuilder result(state);
    UNIT_ASSERT_VALUES_EQUAL(result.GetTotalCount(), totalCount);
    UNIT_ASSERT_C(!TEqHeightHistogramBuilderTestApi(result).GetBudgetForced(), "BudgetForced must be false");
    const ui32 entryThreshold = Max(TEqHeightHistogramBuilder::MIN_ENTRIES, numBuckets);
    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(result).GetEntries().size() >= entryThreshold,
                  "need >= " << entryThreshold << " entries, got " << TEqHeightHistogramBuilderTestApi(result).GetEntries().size());
    UNIT_ASSERT_C(TEqHeightHistogramBuilderTestApi(result).GetMaxRankError() > totalOverB,
                  "need MaxRankError > total/numBuckets = " << totalOverB
                                                            << ", got " << TEqHeightHistogramBuilderTestApi(result).GetMaxRankError());

    auto finalized = result.Finalize();
    UNIT_ASSERT_C(!finalized.Defined(),
                  "guard rail must reject: MaxRankError " << TEqHeightHistogramBuilderTestApi(result).GetMaxRankError()
                                                          << " > total/numBuckets " << totalOverB);
}

// Malformed results — TEqHeightHistogramBuilder deserializer
Y_UNIT_TEST(MalformedResultBuilder) {
    const ui32 numBuckets = 10;
    auto params = MakeParams(numBuckets);
    auto keys = MakeSortedStream(100, 10);
    auto builder = BuildFromStream(keys, params);
    TEqHeightHistogramIntermediateState state = builder.Serialize();
    TString result = state.SerializeAsString();

    // Truncated: cut off the last byte.  Protobuf ParseFromString returns false (no throw), so we check and throw.
    {
        TString truncated(result.data(), result.size() - 1);
        TEqHeightHistogramIntermediateState corrupted;
        UNIT_ASSERT_EXCEPTION(
            [&] {
                if (!corrupted.ParseFromString(truncated)) {
                    throw yexception() << "parse failed";
                }
            }(),
            yexception);
    }

    // Trailing bytes: append garbage.
    {
        TString trailing = result + TString("\xFF\xFF\xFF\xFF");
        TEqHeightHistogramIntermediateState corrupted;
        UNIT_ASSERT_EXCEPTION(
            [&] {
                if (!corrupted.ParseFromString(trailing)) {
                    throw yexception() << "parse failed";
                }
            }(),
            yexception);
    }

    // Zero Count: corrupt the first entry's Count field to 0.
    {
        TEqHeightHistogramIntermediateState corrupted = state;
        corrupted.MutableEntries(0)->SetCount(0);
        UNIT_ASSERT_EXCEPTION(TEqHeightHistogramBuilder(corrupted), yexception);
    }

    // Count sum != TotalCount: inflate the first entry's Count.
    {
        TEqHeightHistogramIntermediateState corrupted = state;
        corrupted.MutableEntries(0)->SetCount(1000000000000ULL);
        UNIT_ASSERT_EXCEPTION(TEqHeightHistogramBuilder(corrupted), yexception);
    }

    // MinKey > first UpperBound: overwrite MinKey with 0xFF bytes.
    {
        TEqHeightHistogramIntermediateState corrupted = state;
        corrupted.SetMinKey(TString("\xFF\xFF\xFF\xFF"));
        UNIT_ASSERT_EXCEPTION(TEqHeightHistogramBuilder(corrupted), yexception);
    }
}

// Malformed results — TEqHeightHistogram deserializer
Y_UNIT_TEST(MalformedResultHistogram) {
    const ui32 numBuckets = 10;
    auto params = MakeParams(numBuckets);
    auto keys = MakeSortedStream(100, 10);
    auto builder = BuildFromStream(keys, params);
    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT(result->BucketsSize() > 0);
    TString resultStr = result->SerializeAsString();

    // Empty result: default proto (ByteSizeLong() == 0) yields an empty reader, not an exception.
    {
        TEqHeightHistogramResult emptyResult;
        TEqHeightHistogram emptyHist(emptyResult);
        UNIT_ASSERT_VALUES_EQUAL(emptyHist.GetNumBuckets(), 0u);
    }

    // Truncated: cut off the last byte.  Protobuf ParseFromString returns false (no throw), so we check and throw.
    {
        TString truncated(resultStr.data(), resultStr.size() - 1);
        TEqHeightHistogramResult corrupted;
        UNIT_ASSERT_EXCEPTION(
            [&] {
                if (!corrupted.ParseFromString(truncated)) {
                    throw yexception() << "parse failed";
                }
            }(),
            yexception);
    }

    // Trailing bytes: append garbage.
    {
        TString trailing = resultStr + TString("\xFF\xFF\xFF\xFF");
        TEqHeightHistogramResult corrupted;
        UNIT_ASSERT_EXCEPTION(
            [&] {
                if (!corrupted.ParseFromString(trailing)) {
                    throw yexception() << "parse failed";
                }
            }(),
            yexception);
    }

    // Zero buckets: corrupt NumBuckets to 0.
    {
        TEqHeightHistogramResult corrupted = *result;
        corrupted.SetNumBuckets(0);
        UNIT_ASSERT_EXCEPTION(TEqHeightHistogram(corrupted), yexception);
    }

    // Cumulative count exceeds total: corrupt the first bucket's CumulativeCount.
    {
        TEqHeightHistogramResult corrupted = *result;
        corrupted.MutableBuckets(0)->SetCumulativeCount(999999999ULL);
        UNIT_ASSERT_EXCEPTION(TEqHeightHistogram(corrupted), yexception);
    }

    // Last cumulative != total: corrupt the last bucket's CumulativeCount.
    {
        TEqHeightHistogramResult corrupted = *result;
        corrupted.MutableBuckets(corrupted.BucketsSize() - 1)->SetCumulativeCount(1);
        UNIT_ASSERT_EXCEPTION(TEqHeightHistogram(corrupted), yexception);
    }

    // Non-increasing UpperBound: overwrite bucket[1]'s bound with bucket[0]'s.
    {
        TEqHeightHistogramResult corrupted = *result;
        TString ub0 = corrupted.GetBuckets(0).GetUpperBound();
        corrupted.MutableBuckets(1)->SetUpperBound(ub0);
        UNIT_ASSERT_EXCEPTION(TEqHeightHistogram(corrupted), yexception);
    }

    // Non-increasing cumulative: copy bucket[0]'s count onto bucket[1].
    {
        TEqHeightHistogramResult corrupted = *result;
        ui64 cc0 = corrupted.GetBuckets(0).GetCumulativeCount();
        corrupted.MutableBuckets(1)->SetCumulativeCount(cc0);
        UNIT_ASSERT_EXCEPTION(TEqHeightHistogram(corrupted), yexception);
    }
}

// Wide-key usable byte budget — per-Add invariant
Y_UNIT_TEST(WideKeyUsableByteBudget) {
    const ui32 numBuckets = 10;
    const ui64 totalCount = 100;
    // Budget for >= MIN_ENTRIES of 4 KB keys after compaction.
    const ui64 maxBytes = 256 * 1024;
    auto params = MakeParams(numBuckets, maxBytes);

    TEqHeightHistogramBuilder builder(params);
    for (ui32 i = 0; i < totalCount; ++i) {
        TString bigKey(4090, 'x');
        bigKey[0] = static_cast<char>(i & 0xFF);
        bigKey[1] = static_cast<char>((i >> 8) & 0xFF);
        builder.Add(bigKey);
    }

    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);

    UNIT_ASSERT_C(builder.Serialize().SerializeAsString().size() <= maxBytes,
                  "serialized state " << builder.Serialize().SerializeAsString().size() << " > MaxStateBytes " << maxBytes);

    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT_C(result->BucketsSize() > 0,
                  "wide-key state with sufficient budget must finalize, not nullopt");

    TEqHeightHistogram hist(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);

    // Every bucket boundary must be a whole key (4 KB + 2 byte prefix).
    for (size_t i = 0; i < hist.GetNumBuckets(); ++i) {
        const auto& bkt = hist.GetBucket(i);
        UNIT_ASSERT_C(bkt.UpperBound.size() >= 4090,
                      "bucket " << i << " UpperBound truncated: size "
                                << bkt.UpperBound.size() << " < 4090");
    }
}

// MinKey round-trip
// MinKey must survive round-trip; a wrong MinKey mischarges RankUncertainty in Merge.
Y_UNIT_TEST(MinKeyRoundTrip) {
    const ui32 numBuckets = 20;
    auto params = MakeParams(numBuckets);

    auto keys = MakeSortedStream(100, 10);
    auto builder = BuildFromStream(keys, params);

    TStringBuf origMinKey = TEqHeightHistogramBuilderTestApi(builder).GetMinKey();
    UNIT_ASSERT_C(!origMinKey.empty(), "MinKey must be set after adding keys");

    TEqHeightHistogramIntermediateState serialized = builder.Serialize();
    TEqHeightHistogramBuilder deserialized(serialized);

    UNIT_ASSERT_VALUES_EQUAL(TEqHeightHistogramBuilderTestApi(deserialized).GetMinKey(), origMinKey);

    // After merging two non-adjacent states, MinKey must be the smaller.
    TEqHeightHistogramBuilder builderA(params);
    for (ui32 i = 50; i < 100; ++i) {
        builderA.Add(MakeKey(static_cast<i32>(i)));
    }
    TEqHeightHistogramBuilder builderB(params);
    for (ui32 i = 0; i < 50; ++i) {
        builderB.Add(MakeKey(static_cast<i32>(i)));
    }
    builderA.Merge(builderB);
    UNIT_ASSERT_VALUES_EQUAL(TEqHeightHistogramBuilderTestApi(builderA).GetMinKey(), MakeKey(0));
}

// RankUncertainty > 0 merge
// Tight budget fuses entries; merging in several orders must keep error <= GetMaxRankError().
Y_UNIT_TEST(MergeProducesRankUncertainty) {
    const ui32 numBuckets = 10;
    const ui32 numParts = 16;
    const ui32 keysPerPart = 2000;
    const ui32 perKey = 1;
    const ui64 totalCount = static_cast<ui64>(numParts) * keysPerPart * perKey;
    // Tight budget: forces fusion so entries become multi-key.
    const ui64 maxBytes = 8 * 1024;
    auto params = MakeParams(numBuckets, maxBytes);

    // Each part covers a disjoint key range, shuffled within the part.
    TFastRng64 rng(42);
    TVector<TVector<TString>> parts(numParts);
    for (ui32 partIdx = 0; partIdx < numParts; ++partIdx) {
        for (ui32 keyIdx = 0; keyIdx < keysPerPart; ++keyIdx) {
            parts[partIdx].push_back(MakeKey(static_cast<i32>(partIdx * keysPerPart + keyIdx)));
        }
        Shuffle(parts[partIdx].begin(), parts[partIdx].end(), rng);
    }

    // Build all keys for AssertTrueRanks.
    TVector<TString> allKeys;
    allKeys.reserve(static_cast<size_t>(totalCount));
    for (ui32 partIdx = 0; partIdx < numParts; ++partIdx) {
        for (ui32 keyIdx = 0; keyIdx < keysPerPart; ++keyIdx) {
            allKeys.push_back(MakeKey(static_cast<i32>(partIdx * keysPerPart + keyIdx)));
        }
    }

    auto buildStates = [&] {
        TVector<TEqHeightHistogramBuilder> states;
        for (const auto& part : parts) {
            states.push_back(BuildChunk(part, params));
        }
        return states;
    };

    // Merge in multiple orders and check each.
    for (auto order : {0, 1, 2}) { // 0=linear, 1=balanced, 2=random
        TVector<TEqHeightHistogramBuilder> states = buildStates();
        TEqHeightHistogramBuilder builder(params);
        if (order == 0) {
            builder = MergeLinear(std::move(states), params);
        } else if (order == 1) {
            builder = MergeBalanced(std::move(states), params);
        } else {
            builder = MergeRandom(std::move(states), params, 7);
        }

        UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), totalCount);

        const ui64 maxRankError = TEqHeightHistogramBuilderTestApi(builder).GetMaxRankError();

        // RankUncertainty must be non-zero: tight budget fuses entries, merging interleaves them.
        UNIT_ASSERT_C(maxRankError > 0,
                      "order " << order << ": expected MaxRankError > 0 from "
                               << "merged fused states, got 0");

        auto result = builder.Finalize();
        UNIT_ASSERT(result.Defined());
        UNIT_ASSERT_C(result->BucketsSize() > 0,
                      "order " << order << ": Finalize must produce a histogram");
        TEqHeightHistogram hist(*result);
        UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);

        const ui64 tolerance = totalCount / (4 * numBuckets);
        AssertTrueRanks(hist, allKeys, hist.GetMaxRankError(), tolerance,
                        TStringBuf("MergeProducesRankUncertainty order=") + ToString(order));
    }
}

// Merge-count sweep with overlapping ranges
// Sweep numParts in {2, 8, 32, 128} with overlapping ranges and tight budgets.
Y_UNIT_TEST(MergeCountSweepOverlapping) {
    const ui32 numBuckets = 10;
    const ui32 keysPerPart = 500;
    const ui32 perKey = 2;
    const ui64 maxBytes = 8 * 1024;
    auto params = MakeParams(numBuckets, maxBytes);

    ui64 firstErr = 0;
    ui64 firstN = 0;

    for (ui32 numParts : {2u, 8u, 32u, 128u}) {
        const ui64 totalCount = static_cast<ui64>(numParts) * keysPerPart * perKey;
        TFastRng64 rng(numParts * 17 + 3);

        // Overlapping parts: each part draws from the full key range [0, keysPerPart).
        TVector<TVector<TString>> parts(numParts);
        for (ui32 partIdx = 0; partIdx < numParts; ++partIdx) {
            for (ui32 keyIdx = 0; keyIdx < keysPerPart; ++keyIdx) {
                for (ui32 repIdx = 0; repIdx < perKey; ++repIdx) {
                    parts[partIdx].push_back(MakeKey(static_cast<i32>(keyIdx)));
                }
            }
            Shuffle(parts[partIdx].begin(), parts[partIdx].end(), rng);
        }

        TVector<TEqHeightHistogramBuilder> states;
        for (const auto& part : parts) {
            states.push_back(BuildChunk(part, params));
        }
        auto builder = MergeRandom(std::move(states), params, numParts);

        UNIT_ASSERT_VALUES_EQUAL_C(builder.GetTotalCount(), totalCount, "numParts=" << numParts);

        const ui64 maxRankError = TEqHeightHistogramBuilderTestApi(builder).GetMaxRankError();
        // Honest bound: Finalize's guard rail, not 2*Cap(). Frozen-cap regression: error/totalCount stays comparable across numParts.
        UNIT_ASSERT_C(maxRankError <= totalCount / numBuckets,
                      "numParts=" << numParts << ": MaxRankError " << maxRankError
                                  << " > total/numBuckets " << (totalCount / numBuckets));
        if (numParts == 2) {
            firstErr = maxRankError;
            firstN = totalCount;
            UNIT_ASSERT_C(firstErr > 0, "numParts=2 must produce RankUncertainty so the ratio check has a baseline");
        } else {
            UNIT_ASSERT_C(static_cast<NYql::NDecimal::TUint128>(maxRankError) * firstN <=
                              static_cast<NYql::NDecimal::TUint128>(4) * firstErr * totalCount,
                          "numParts=" << numParts << ": error/totalCount " << maxRankError << "/" << totalCount
                                      << " > 4x numParts=2 " << firstErr << "/" << firstN);
        }

        auto result = builder.Finalize();
        UNIT_ASSERT(result.Defined());
        UNIT_ASSERT_C(result->BucketsSize() > 0, "numParts=" << numParts << ": Finalize must produce a histogram");
        TEqHeightHistogram hist(*result);
        UNIT_ASSERT_VALUES_EQUAL_C(hist.GetTotalCount(), totalCount, "numParts=" << numParts);

        // Build all keys for AssertTrueRanks.
        TVector<TString> allKeys;
        allKeys.reserve(static_cast<size_t>(totalCount));
        for (ui32 partIdx = 0; partIdx < numParts; ++partIdx) {
            for (ui32 keyIdx = 0; keyIdx < keysPerPart; ++keyIdx) {
                for (ui32 repIdx = 0; repIdx < perKey; ++repIdx) {
                    allKeys.push_back(MakeKey(static_cast<i32>(keyIdx)));
                }
            }
        }
        const ui64 tolerance = totalCount / (4 * numBuckets);
        AssertTrueRanks(hist, allKeys, hist.GetMaxRankError(), tolerance,
                        TStringBuf("sweep numParts=") + ToString(numParts));
    }
}

// Golden result — pins the serialized format
// Deterministic finalized result with known values; catches layout changes.
Y_UNIT_TEST(GoldenResult) {
    // Build a small, deterministic histogram: 20 keys, 2 per key, numBuckets=4.
    const ui32 numBuckets = 4;
    const ui32 numKeys = 20;
    const ui32 perKey = 2;
    const ui64 totalCount = static_cast<ui64>(numKeys) * perKey;
    auto params = MakeParams(numBuckets);
    auto keys = MakeSortedStream(numKeys, perKey);
    auto builder = BuildFromStream(keys, params);
    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT(result->BucketsSize() > 0);

    // Parse and verify known properties.
    TEqHeightHistogram hist(*result);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetTotalCount(), totalCount);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetMaxRankError(), 0);
    UNIT_ASSERT(hist.IsExact());
    UNIT_ASSERT_VALUES_EQUAL(hist.GetNumBuckets(), numBuckets);

    // Each bucket holds 10 rows; UpperBounds are MakeKey(4), MakeKey(9), MakeKey(14), MakeKey(19).
    UNIT_ASSERT_VALUES_EQUAL(hist.GetBucket(0).UpperBound, MakeKey(4));
    UNIT_ASSERT_VALUES_EQUAL(hist.GetBucket(0).CumulativeCount, 10u);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetBucket(1).UpperBound, MakeKey(9));
    UNIT_ASSERT_VALUES_EQUAL(hist.GetBucket(1).CumulativeCount, 20u);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetBucket(2).UpperBound, MakeKey(14));
    UNIT_ASSERT_VALUES_EQUAL(hist.GetBucket(2).CumulativeCount, 30u);
    UNIT_ASSERT_VALUES_EQUAL(hist.GetBucket(3).UpperBound, MakeKey(19));
    UNIT_ASSERT_VALUES_EQUAL(hist.GetBucket(3).CumulativeCount, 40u);

    // The result must be deterministic: re-build and compare.
    auto builder2 = BuildFromStream(keys, params);
    auto result2 = builder2.Finalize();
    UNIT_ASSERT(result2.Defined());
    UNIT_ASSERT(result2->BucketsSize() > 0);
    UNIT_ASSERT_VALUES_EQUAL(result->SerializeAsString(), result2->SerializeAsString());

    // Round-trip through the builder's intermediate format.
    TEqHeightHistogramIntermediateState state = builder.Serialize();
    TEqHeightHistogramBuilder roundTrip(state);
    auto roundTripResult = roundTrip.Finalize();
    UNIT_ASSERT(roundTripResult.Defined());
    UNIT_ASSERT(roundTripResult->BucketsSize() > 0);
    UNIT_ASSERT_VALUES_EQUAL(roundTripResult->SerializeAsString(), result->SerializeAsString());
}

// Golden intermediate state — pins the Serialize() wire format
Y_UNIT_TEST(GoldenIntermediateState) {
    auto params = MakeParams(1);
    TEqHeightHistogramBuilder builder(params);
    builder.Add(MakeKey(0));
    TEqHeightHistogramIntermediateState state = builder.Serialize();

    // The state must be deterministic: re-serialize and compare.
    TEqHeightHistogramBuilder roundTrip(state);
    TEqHeightHistogramIntermediateState roundTripState = roundTrip.Serialize();
    UNIT_ASSERT_VALUES_EQUAL(state.SerializeAsString(), roundTripState.SerializeAsString());

    // Verify known properties after round-trip.
    UNIT_ASSERT_VALUES_EQUAL(roundTrip.GetTotalCount(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(TEqHeightHistogramBuilderTestApi(roundTrip).GetMinKey(), MakeKey(0));
    UNIT_ASSERT_VALUES_EQUAL(TEqHeightHistogramBuilderTestApi(roundTrip).GetEntries().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(TEqHeightHistogramBuilderTestApi(roundTrip).GetEntries()[0].Count, 1u);
    UNIT_ASSERT(TEqHeightHistogramBuilderTestApi(roundTrip).GetEntries()[0].SingleKey);
}

// TEqHeightHistogram copyability — smoke test
Y_UNIT_TEST(HistogramCopyable) {
    const ui32 numBuckets = 10;
    const ui64 totalCount = 1000;
    auto params = MakeParams(numBuckets);
    auto keys = MakeSortedStream(totalCount / 10, 10);
    auto builder = BuildFromStream(keys, params);
    auto result = builder.Finalize();
    UNIT_ASSERT(result.Defined());
    UNIT_ASSERT(result->BucketsSize() > 0);

    TEqHeightHistogram orig(*result);
    TEqHeightHistogram copy = orig; // copy ctor

    UNIT_ASSERT_VALUES_EQUAL(copy.GetTotalCount(), orig.GetTotalCount());
    UNIT_ASSERT_VALUES_EQUAL(copy.GetMaxRankError(), orig.GetMaxRankError());
    UNIT_ASSERT_VALUES_EQUAL(copy.GetNumBuckets(), orig.GetNumBuckets());
    for (size_t i = 0; i < orig.GetNumBuckets(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(copy.GetBucket(i).UpperBound, orig.GetBucket(i).UpperBound);
        UNIT_ASSERT_VALUES_EQUAL(copy.GetBucket(i).CumulativeCount, orig.GetBucket(i).CumulativeCount);
    }

    // EstimateLessOrEqual must work on the copy.
    for (size_t i = 0; i < keys.size(); i += 100) {
        UNIT_ASSERT_VALUES_EQUAL(copy.EstimateLessOrEqual(keys[i]),
                                 orig.EstimateLessOrEqual(keys[i]));
    }

    TEqHeightHistogram moved = std::move(copy);
    UNIT_ASSERT_VALUES_EQUAL(moved.GetTotalCount(), orig.GetTotalCount());
    UNIT_ASSERT_VALUES_EQUAL(moved.GetNumBuckets(), orig.GetNumBuckets());
    for (size_t i = 0; i < orig.GetNumBuckets(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(moved.GetBucket(i).UpperBound, orig.GetBucket(i).UpperBound);
        UNIT_ASSERT_VALUES_EQUAL(moved.GetBucket(i).CumulativeCount, orig.GetBucket(i).CumulativeCount);
    }
}

// Merge then Add must not dump half the summary
// Merge and Add share SoftBudget, so one Add must not halve the entry count.
Y_UNIT_TEST(MergeThenAddKeepsResolution) {
    const ui32 numBuckets = 10;
    const ui32 numParts = 8;
    const ui32 keysPerPart = 500;
    const ui32 perKey = 2;
    const ui64 maxBytes = 4 * 1024;
    auto params = MakeParams(numBuckets, maxBytes);

    TFastRng64 rng(11);
    TVector<TVector<TString>> parts(numParts);
    for (ui32 partIdx = 0; partIdx < numParts; ++partIdx) {
        for (ui32 keyIdx = 0; keyIdx < keysPerPart; ++keyIdx) {
            for (ui32 repIdx = 0; repIdx < perKey; ++repIdx) {
                parts[partIdx].push_back(MakeKey(static_cast<i32>(keyIdx)));
            }
        }
        Shuffle(parts[partIdx].begin(), parts[partIdx].end(), rng);
    }
    TVector<TEqHeightHistogramBuilder> states;
    for (const auto& part : parts) {
        states.push_back(BuildChunk(part, params));
    }
    auto result = MergeRandom(std::move(states), params, 5);

    const size_t before = TEqHeightHistogramBuilderTestApi(result).GetEntries().size();
    UNIT_ASSERT_C(before > 1, "merged state must have entries");
    result.Add(MakeKey(12345));
    const size_t after = TEqHeightHistogramBuilderTestApi(result).GetEntries().size();
    UNIT_ASSERT_C(after + 2 >= before,
                  "one Add after merge dropped entries from " << before << " to " << after);
}

// Unsorted Add flush batching — not quadratic
// flushAt = max(EmissionRate, |Entries|) keeps flush count well below totalCount/EmissionRate.
Y_UNIT_TEST(UnsortedAddFlushCount) {
    const ui32 numBuckets = 10;
    const ui32 numKeys = 20000;
    auto params = MakeParams(numBuckets);

    TFastRng64 rng(7);
    auto keys = MakeSortedStream(numKeys, 1);
    Shuffle(keys.begin(), keys.end(), rng);

    auto builder = BuildFromStream(keys, params);
    UNIT_ASSERT_VALUES_EQUAL(builder.GetTotalCount(), numKeys);

    const ui64 flushCount = TEqHeightHistogramBuilderTestApi(builder).GetFlushCount();
    UNIT_ASSERT_C(flushCount * params.EmissionRate < numKeys / 2,
                  "Flush called " << flushCount << " times; naive every-EmissionRate "
                                  << "flushing is ~" << (numKeys / params.EmissionRate));
}

} // Y_UNIT_TEST_SUITE(EqHeightHistogram)
