#include "query_history.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/xrange.h>
#include <util/digest/murmur.h>
#include <util/random/random.h>

namespace NKikimr {
namespace NSysView {

namespace {

auto MakeStatsPtr() {
    return std::make_shared<NKikimrSysView::TQueryStats>();
}

ui64 QueryHash(const TString& queryText) {
    return MurmurHash<ui64>(queryText.data(), queryText.size());
}

void FillStatsWithNodeId(TQueryTextsByHash& texts, NKikimrSysView::TQueryStats* message, ui32 nodeId,
    const TString& text, ui64 duration)
{
    message->SetNodeId(nodeId);

    auto hash = MurmurHash<ui64>(text.data(), text.size());
    texts[hash] = text;
    message->SetQueryText(text);
    message->SetQueryTextHash(hash);

    message->SetDurationMs(duration);
}

void FillStats(TQueryTextsByHash& texts, NKikimrSysView::TQueryStats* message,
    const TString& text, ui64 duration, ui64 endTime = 0, ui64 readBytes = 0)
{
    message->SetNodeId(1);

    auto hash = MurmurHash<ui64>(text.data(), text.size());
    texts[hash] = text;
    message->SetQueryText(text);
    message->SetQueryTextHash(hash);

    message->SetDurationMs(duration);
    message->SetEndTimeMs(endTime);

    auto* stats = message->MutableStats();
    stats->SetReadBytes(readBytes);
}

void FillStats(TQueryTextsByHash& texts, TQueryStatsPtr messagePtr,
    const TString& text, ui64 duration, ui64 endTime = 0, ui64 readBytes = 0)
{
    FillStats(texts, messagePtr.get(), text, duration, endTime, readBytes);
}

template <typename TBucket>
void AddStatsToBucket(TQueryTextsByHash& texts, TBucket& bucket,
    const TString& text, ui64 duration, ui64 endTime = 0, ui64 readBytes = 0)
{
    auto stats = MakeStatsPtr();
    FillStats(texts, stats.get(), text, duration, endTime, readBytes);
    bucket.Add(stats);
}

using TResult = NKikimrSysView::TEvGetQueryStatsResult;
using THistory = TScanQueryHistory<TDurationGreater>;

void FillStatsResult(TQueryTextsByHash& texts, TResult* message, ui64 bucketSizeMs, const TString& text,
    ui64 lastBucket, const std::initializer_list<ui64>& bucketsStatsCount)
{
    message->SetLastBucket(lastBucket);
    message->SetBucketSizeMs(bucketSizeMs);

    auto bucketIndex = lastBucket - bucketsStatsCount.size() + 1;

    for (auto count : bucketsStatsCount) {
        auto* bucket = message->AddBuckets();
        for (size_t i : xrange(count)) {
            auto* stats = bucket->AddStats();
            FillStats(texts, stats, text + ToString(i), i, bucketIndex * bucketSizeMs);
        }
        ++bucketIndex;
    }
}

void ValidateResultBuckets(const THistory& history,
    ui64 lastBucket, const std::initializer_list<ui64>& bucketsStatsCount)
{
    UNIT_ASSERT_EQUAL(history.GetLastBucket(), lastBucket);
    UNIT_ASSERT_EQUAL(history.GetBucketCount(), bucketsStatsCount.size());

    size_t i = history.GetStartBucket();
    for (auto count : bucketsStatsCount) {
        UNIT_ASSERT_EQUAL(history.GetBucketTop(i).StatsSize(), count);
        ++i;
    }
}

void TestMerge(
    ui64 lastBucket1, const std::initializer_list<ui64>& bucketsStatsCount1,
    ui64 lastBucket2, const std::initializer_list<ui64>& bucketsStatsCount2,
    ui64 lastBucketHistory, ui64 historySize,
    ui64 lastBucketResult, const std::initializer_list<ui64>& bucketsStatsCountResult)
{
    constexpr ui64 bucketSizeMs = 10;

    TQueryTextsByHash texts;

    TResult r1;
    FillStatsResult(texts, &r1, bucketSizeMs, "a", lastBucket1, bucketsStatsCount1);

    TResult r2;
    FillStatsResult(texts, &r2, bucketSizeMs, "b", lastBucket2, bucketsStatsCount2);

    THistory history(historySize, TDuration::MilliSeconds(bucketSizeMs),
        TInstant::MilliSeconds(lastBucketHistory * bucketSizeMs));

    history.Merge(std::move(r1));
    history.Merge(std::move(r2));

    ValidateResultBuckets(history, lastBucketResult, bucketsStatsCountResult);
}

} // namespace

Y_UNIT_TEST_SUITE(SysViewQueryHistory) {

    Y_UNIT_TEST(TopDurationAdd) {
        TQueryStatsBucket<TDurationGreater> bucket;

        TQueryTextsByHash texts;

        AddStatsToBucket(texts, bucket, "a", 10);
        AddStatsToBucket(texts, bucket, "b", 20);
        AddStatsToBucket(texts, bucket, "c", 15);

        NKikimrSysView::TTopQueryStats proto;
        bucket.Fill(&proto, texts);

        UNIT_ASSERT_EQUAL(proto.StatsSize(), 3);

        UNIT_ASSERT_EQUAL(proto.GetStats(0).GetDurationMs(), 20);
        UNIT_ASSERT_EQUAL(proto.GetStats(1).GetDurationMs(), 15);
        UNIT_ASSERT_EQUAL(proto.GetStats(2).GetDurationMs(), 10);
    }

    Y_UNIT_TEST(TopReadBytesAdd) {
        TQueryStatsBucket<TReadBytesGreater> bucket;

        TQueryTextsByHash texts;

        AddStatsToBucket(texts, bucket, "a", 10, 0, 7);
        AddStatsToBucket(texts, bucket, "b", 20, 0, 20);
        AddStatsToBucket(texts, bucket, "c", 15, 0, 100);

        NKikimrSysView::TTopQueryStats proto;
        bucket.Fill(&proto, texts);

        UNIT_ASSERT_EQUAL(proto.StatsSize(), 3);

        UNIT_ASSERT_EQUAL(proto.GetStats(0).GetStats().GetReadBytes(), 100);
        UNIT_ASSERT_EQUAL(proto.GetStats(1).GetStats().GetReadBytes(), 20);
        UNIT_ASSERT_EQUAL(proto.GetStats(2).GetStats().GetReadBytes(), 7);
    }

    Y_UNIT_TEST(AddDedup) {
        TQueryStatsDedupBucket<TDurationGreater> bucket;

        TQueryTextsByHash texts;

        AddStatsToBucket(texts, bucket, "a", 10);
        AddStatsToBucket(texts, bucket, "b", 20);
        AddStatsToBucket(texts, bucket, "c", 15);
        AddStatsToBucket(texts, bucket, "b", 5);
        AddStatsToBucket(texts, bucket, "c", 25);

        NKikimrSysView::TTopQueryStats proto;
        bucket.Fill(&proto, texts);

        UNIT_ASSERT_EQUAL(proto.StatsSize(), 3);

        UNIT_ASSERT_EQUAL(proto.GetStats(0).GetDurationMs(), 25);
        UNIT_ASSERT_EQUAL(proto.GetStats(0).GetQueryTextHash(), QueryHash("c"));
        UNIT_ASSERT_EQUAL(proto.GetStats(1).GetDurationMs(), 20);
        UNIT_ASSERT_EQUAL(proto.GetStats(1).GetQueryTextHash(), QueryHash("b"));
        UNIT_ASSERT_EQUAL(proto.GetStats(2).GetDurationMs(), 10);
        UNIT_ASSERT_EQUAL(proto.GetStats(2).GetQueryTextHash(), QueryHash("a"));
    }

    Y_UNIT_TEST(AddDedup2) {
        TQueryStatsDedupBucket<TDurationGreater> bucket;

        TQueryTextsByHash texts;

        for (size_t i : xrange(TOP_QUERIES_COUNT)) {
            AddStatsToBucket(texts, bucket, ToString(i), (i + 1) * 10);
        }
        AddStatsToBucket(texts, bucket, "a", 5);
        AddStatsToBucket(texts, bucket, "b", 15);

        NKikimrSysView::TTopQueryStats proto;
        bucket.Fill(&proto, texts);

        UNIT_ASSERT_EQUAL(proto.StatsSize(), TOP_QUERIES_COUNT);
        auto last = TOP_QUERIES_COUNT - 1;

        UNIT_ASSERT_EQUAL(proto.GetStats(last).GetDurationMs(), 15);
        UNIT_ASSERT_EQUAL(proto.GetStats(last).GetQueryTextHash(), QueryHash("b"));
    }

    Y_UNIT_TEST(AddDedupRandom) {
        TQueryStatsDedupBucket<TDurationGreater> bucket;

        TQueryTextsByHash texts;

        for (size_t i : xrange(10000)) {
            Y_UNUSED(i);
            AddStatsToBucket(texts, bucket, ToString(RandomNumber<ui32>(1000)), RandomNumber<ui64>(5000));
        }

        UNIT_ASSERT_EQUAL(bucket.Top.size(), TOP_QUERIES_COUNT);
        UNIT_ASSERT_EQUAL(bucket.Stats.size(), TOP_QUERIES_COUNT);

        for (auto& top : bucket.Top) {
            auto it = bucket.Stats.find(top.second);
            UNIT_ASSERT(it != bucket.Stats.end());
            UNIT_ASSERT_EQUAL(it->second->GetDurationMs(), top.first);
        }
    }

    Y_UNIT_TEST(AggrMerge) {
        TAggrQueryStatsBucket<TDurationGreater> bucket;

        TQueryTextsByHash texts;

        NKikimrSysView::TTopQueryStats s1;
        FillStats(texts, s1.AddStats(), "a", 30);
        FillStats(texts, s1.AddStats(), "b", 20);
        FillStats(texts, s1.AddStats(), "c", 10);

        NKikimrSysView::TTopQueryStats s2;
        FillStats(texts, s2.AddStats(), "x", 35);
        FillStats(texts, s2.AddStats(), "y", 25);
        FillStats(texts, s2.AddStats(), "z", 22);

        bucket.Merge(std::move(s1), texts);
        bucket.Merge(std::move(s2), texts);

        UNIT_ASSERT_EQUAL(bucket.Top.StatsSize(), 5);

        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(0).GetDurationMs(), 35);
        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(1).GetDurationMs(), 30);
        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(2).GetDurationMs(), 25);
        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(3).GetDurationMs(), 22);
        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(4).GetDurationMs(), 20);
    }

    Y_UNIT_TEST(AggrMergeDedup) {
        TAggrQueryStatsDedupBucket<TDurationGreater> bucket;

        TQueryTextsByHash texts;

        NKikimrSysView::TTopQueryStats s1;
        FillStats(texts, s1.AddStats(), "a", 30);
        FillStats(texts, s1.AddStats(), "b", 20);
        FillStats(texts, s1.AddStats(), "c", 10);
        FillStats(texts, s1.AddStats(), "d", 5);

        NKikimrSysView::TTopQueryStats s2;
        FillStats(texts, s2.AddStats(), "c", 35);
        FillStats(texts, s2.AddStats(), "a", 25);
        FillStats(texts, s2.AddStats(), "b", 25);
        FillStats(texts, s2.AddStats(), "e", 15);

        bucket.Merge(std::move(s1), texts);
        bucket.Merge(std::move(s2), texts);

        UNIT_ASSERT_EQUAL(bucket.Top.StatsSize(), 5);

        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(0).GetDurationMs(), 35);
        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(0).GetQueryText(), "c");

        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(1).GetDurationMs(), 30);
        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(1).GetQueryText(), "a");

        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(2).GetDurationMs(), 25);
        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(2).GetQueryText(), "b");

        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(3).GetDurationMs(), 15);
        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(3).GetQueryText(), "e");

        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(4).GetDurationMs(), 5);
        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(4).GetQueryText(), "d");
    }

    Y_UNIT_TEST(StableMerge) {
        TAggrQueryStatsDedupBucket<TDurationGreater> bucket;

        TQueryTextsByHash texts;

        NKikimrSysView::TTopQueryStats s1;
        FillStatsWithNodeId(texts, s1.AddStats(), 1, "a", 10);
        FillStatsWithNodeId(texts, s1.AddStats(), 1, "b", 10);

        NKikimrSysView::TTopQueryStats s2;
        FillStatsWithNodeId(texts, s2.AddStats(), 2, "x", 10);
        FillStatsWithNodeId(texts, s2.AddStats(), 2, "y", 10);

        bucket.Merge(std::move(s1), texts);

        bucket.Merge(std::move(s2), texts);

        UNIT_ASSERT_EQUAL(bucket.Top.StatsSize(), 4);

        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(0).GetQueryText(), "x");
        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(1).GetQueryText(), "y");
        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(2).GetQueryText(), "a");
        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(3).GetQueryText(), "b");
    }

    Y_UNIT_TEST(StableMerge2) {
        TAggrQueryStatsDedupBucket<TDurationGreater> bucket;

        TQueryTextsByHash texts;

        NKikimrSysView::TTopQueryStats s1;
        FillStatsWithNodeId(texts, s1.AddStats(), 1, "a", 10);
        FillStatsWithNodeId(texts, s1.AddStats(), 1, "b", 10);

        NKikimrSysView::TTopQueryStats s2;
        FillStatsWithNodeId(texts, s2.AddStats(), 2, "x", 10);
        FillStatsWithNodeId(texts, s2.AddStats(), 2, "y", 10);

        bucket.Merge(std::move(s2), texts);

        bucket.Merge(std::move(s1), texts);

        UNIT_ASSERT_EQUAL(bucket.Top.StatsSize(), 4);

        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(0).GetQueryText(), "x");
        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(1).GetQueryText(), "y");
        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(2).GetQueryText(), "a");
        UNIT_ASSERT_EQUAL(bucket.Top.GetStats(3).GetQueryText(), "b");
    }

    Y_UNIT_TEST(ServiceQueryHistoryAdd) {
        TServiceQueryHistory<TDurationGreater> history(5, TDuration::MilliSeconds(10), TInstant::MilliSeconds(75));

        TQueryTextsByHash texts;

        auto s1 = MakeStatsPtr();
        FillStats(texts, s1, "a", 1, 25);
        history.Add(s1);

        auto s2 = MakeStatsPtr();
        FillStats(texts, s2, "b", 5, 35);
        history.Add(s2);

        auto s3 = MakeStatsPtr();
        FillStats(texts, s3, "c", 1, 50);
        history.Add(s3);

        auto s4 = MakeStatsPtr();
        FillStats(texts, s4, "d", 10, 51);
        history.Add(s4);

        NKikimrSysView::TEvGetQueryStatsResult result;
        history.ToProto(3, result);

        UNIT_ASSERT_EQUAL(result.GetLastBucket(), 7);
        UNIT_ASSERT_EQUAL(result.GetBucketSizeMs(), 10);

        UNIT_ASSERT_EQUAL(result.BucketsSize(), 5);

        UNIT_ASSERT_EQUAL(result.GetBuckets(0).StatsSize(), 1);
        UNIT_ASSERT_EQUAL(result.GetBuckets(0).GetStats(0).GetEndTimeMs(), 35);
        UNIT_ASSERT_EQUAL(result.GetBuckets(0).GetStats(0).GetDurationMs(), 5);

        UNIT_ASSERT_EQUAL(result.GetBuckets(1).StatsSize(), 0);

        UNIT_ASSERT_EQUAL(result.GetBuckets(2).StatsSize(), 2);
        UNIT_ASSERT_EQUAL(result.GetBuckets(2).GetStats(0).GetEndTimeMs(), 51);
        UNIT_ASSERT_EQUAL(result.GetBuckets(2).GetStats(0).GetDurationMs(), 10);
        UNIT_ASSERT_EQUAL(result.GetBuckets(2).GetStats(1).GetEndTimeMs(), 50);
        UNIT_ASSERT_EQUAL(result.GetBuckets(2).GetStats(1).GetDurationMs(), 1);

        UNIT_ASSERT_EQUAL(result.GetBuckets(3).StatsSize(), 0);
        UNIT_ASSERT_EQUAL(result.GetBuckets(4).StatsSize(), 0);

        UNIT_ASSERT_EQUAL(result.QueryTextsSize(), 3);

        auto s5 = MakeStatsPtr();
        FillStats(texts, s5, "e", 5, 93);
        history.Add(s5);

        result.Clear();
        history.ToProto(5, result);

        UNIT_ASSERT_EQUAL(result.GetLastBucket(), 9);

        UNIT_ASSERT_EQUAL(result.BucketsSize(), 5);

        UNIT_ASSERT_EQUAL(result.GetBuckets(0).StatsSize(), 2);
        UNIT_ASSERT_EQUAL(result.GetBuckets(1).StatsSize(), 0);
        UNIT_ASSERT_EQUAL(result.GetBuckets(2).StatsSize(), 0);
        UNIT_ASSERT_EQUAL(result.GetBuckets(3).StatsSize(), 0);
        UNIT_ASSERT_EQUAL(result.GetBuckets(4).StatsSize(), 1);

        UNIT_ASSERT_EQUAL(result.GetBuckets(4).GetStats(0).GetQueryTextHash(), QueryHash("e"));

        UNIT_ASSERT_EQUAL(result.QueryTextsSize(), 3);

        result.Clear();
        history.ToProto(7, result);

        UNIT_ASSERT_EQUAL(result.GetLastBucket(), 9);
        UNIT_ASSERT_EQUAL(result.BucketsSize(), 3);

        UNIT_ASSERT_EQUAL(result.GetBuckets(0).StatsSize(), 0);
        UNIT_ASSERT_EQUAL(result.GetBuckets(1).StatsSize(), 0);
        UNIT_ASSERT_EQUAL(result.GetBuckets(2).StatsSize(), 1);

        UNIT_ASSERT_EQUAL(result.GetBuckets(2).GetStats(0).GetQueryTextHash(), QueryHash("e"));

        UNIT_ASSERT_EQUAL(result.QueryTextsSize(), 1);
    }

    Y_UNIT_TEST(ScanQueryHistoryMerge) {
        TestMerge(
            100, {1, 2, 3},
            100, {1, 2},
            100, 3,
            100, {1, 3, 5});

        TestMerge(
            100, {1, 2, 3},
            99, {1, 2},
            100, 3,
            100, {2, 4, 3});

        TestMerge(
            100, {1, 2, 3},
            98, {1, 2},
            100, 3,
            100, {3, 2, 3});

        TestMerge(
            100, {1, 2, 3},
            100, {1, 2},
            90, 5,
            100, {0, 0, 1, 3, 5});

        TestMerge(
            100, {1, 2, 3, 4},
            99, {1, 2},
            101, 5,
            101, {1, 3, 5, 4, 0});

        TestMerge(
            100, {1, 2, 3},
            102, {1, 2, 3, 4},
            90, 5,
            102, {1, 3, 5, 3, 4});

        TestMerge(
            100, {1, 2, 3},
            101, {2, 1, 2, 3},
            90, 2,
            101, {5, 3});
    }
}

} // NSysView
} // NKikimr
