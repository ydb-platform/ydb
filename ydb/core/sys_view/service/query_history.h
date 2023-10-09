#pragma once

#include <ydb/core/protos/sys_view.pb.h>

#include <util/generic/vector.h>
#include <util/datetime/base.h>

namespace NKikimr {
namespace NSysView {

// TODO: configs
constexpr TDuration ONE_MINUTE_BUCKET_SIZE = TDuration::Minutes(1);
constexpr ui64 ONE_MINUTE_BUCKET_COUNT = 6 * 60; // 6 hours

constexpr TDuration ONE_HOUR_BUCKET_SIZE = TDuration::Hours(1);
constexpr ui64 ONE_HOUR_BUCKET_COUNT = 24 * 7 * 2; // 2 weeks

constexpr size_t TOP_QUERIES_COUNT = 5;

using TNodeId = ui32;
using TQueryHash = ui64;

using TQueryStatsPtr = std::shared_ptr<NKikimrSysView::TQueryStats>;

using TQueryTextsByHash = std::unordered_map<TQueryHash, TString>;

struct TReadBytesGreater {
    using TValue = ui64;

    static TValue GetValue(const TQueryStatsPtr stats) {
        return stats->GetStats().GetReadBytes();
    }
    bool operator() (const NKikimrSysView::TQueryStats* a, const NKikimrSysView::TQueryStats* b) const {
        return a->GetStats().GetReadBytes() == b->GetStats().GetReadBytes()
            ? a->GetNodeId() > b->GetNodeId()
            : a->GetStats().GetReadBytes() > b->GetStats().GetReadBytes();
    }
    bool operator() (const TQueryStatsPtr a, const TQueryStatsPtr b) const {
        return operator()(a.get(), b.get());
    }
};

struct TDurationGreater {
    using TValue = ui64;

    static TValue GetValue(const TQueryStatsPtr stats) {
        return stats->GetDurationMs();
    }
    bool operator() (const NKikimrSysView::TQueryStats* a, const NKikimrSysView::TQueryStats* b) const {
        return a->GetDurationMs() == b->GetDurationMs()
            ? a->GetNodeId() > b->GetNodeId()
            : a->GetDurationMs() > b->GetDurationMs();
    }
    bool operator() (const TQueryStatsPtr a, const TQueryStatsPtr b) const {
        return operator()(a.get(), b.get());
    }
};

struct TCpuTimeGreater {
    using TValue = ui64;

    static TValue GetValue(const TQueryStatsPtr stats) {
        return stats->GetTotalCpuTimeUs();
    }
    bool operator() (const NKikimrSysView::TQueryStats* a, const NKikimrSysView::TQueryStats* b) const {
        return a->GetTotalCpuTimeUs() == b->GetTotalCpuTimeUs()
            ? a->GetNodeId() > b->GetNodeId()
            : a->GetTotalCpuTimeUs() > b->GetTotalCpuTimeUs();
    }
    bool operator() (const TQueryStatsPtr a, const TQueryStatsPtr b) const {
        return operator()(a.get(), b.get());
    }
};

struct TRequestUnitsGreater {
    using TValue = ui64;

    static TValue GetValue(const TQueryStatsPtr stats) {
        return stats->GetRequestUnits();
    }
    bool operator() (const NKikimrSysView::TQueryStats* a, const NKikimrSysView::TQueryStats* b) const {
        return a->GetRequestUnits() == b->GetRequestUnits()
            ? a->GetNodeId() > b->GetNodeId()
            : a->GetRequestUnits() > b->GetRequestUnits();
    }
    bool operator() (const TQueryStatsPtr a, const TQueryStatsPtr b) const {
        return operator()(a.get(), b.get());
    }
};

inline void CopyQueryStatsNoText(NKikimrSysView::TQueryStats* to, const NKikimrSysView::TQueryStats& from) {
    to->SetQueryTextHash(from.GetQueryTextHash());
    to->SetDurationMs(from.GetDurationMs());
    to->SetEndTimeMs(from.GetEndTimeMs());
    to->MutableStats()->CopyFrom(from.GetStats());
    if (from.HasUserSID()) {
        to->SetUserSID(from.GetUserSID());
    }
    to->SetParametersSize(from.GetParametersSize());
    if (from.HasCompileDurationMs()) {
        to->SetCompileDurationMs(from.GetCompileDurationMs());
    }
    if (from.HasFromQueryCache()) {
        to->SetFromQueryCache(from.GetFromQueryCache());
    }
    to->SetNodeId(from.GetNodeId());
    to->MutableShardsCpuTimeUs()->CopyFrom(from.GetShardsCpuTimeUs());
    to->MutableComputeCpuTimeUs()->CopyFrom(from.GetComputeCpuTimeUs());
    if (from.HasCompileCpuTimeUs()) {
        to->SetCompileCpuTimeUs(from.GetCompileCpuTimeUs());
    }
    to->SetProcessCpuTimeUs(from.GetProcessCpuTimeUs());
    to->SetTotalCpuTimeUs(from.GetTotalCpuTimeUs());
    to->SetType(from.GetType());
    to->SetRequestUnits(from.GetRequestUnits());
}

template <typename TGreater>
struct TQueryStatsBucket {
    TVector<TQueryStatsPtr> Top;

    void Add(TQueryStatsPtr stats) {
        auto it = std::lower_bound(Top.begin(), Top.end(), stats, TGreater());
        if (Top.size() == TOP_QUERIES_COUNT && it == Top.end()) {
            return;
        }
        Top.insert(it, stats);
        if (Top.size() > TOP_QUERIES_COUNT) {
            Top.resize(TOP_QUERIES_COUNT);
        }
    }

    void Clear() {
        Top.clear();
    }

    void Fill(NKikimrSysView::TTopQueryStats* bucket, TQueryTextsByHash& texts) const {
        for (auto& queryStats : Top) {
            CopyQueryStatsNoText(bucket->AddStats(), *queryStats);
            texts.try_emplace(queryStats->GetQueryTextHash(), queryStats->GetQueryText());
        }
    }
};

template <typename TGreater>
struct TQueryStatsDedupBucket {
    using TValue = typename TGreater::TValue;

    std::unordered_map<TQueryHash, TQueryStatsPtr> Stats; // query hash -> stats
    std::multimap<TValue, TQueryHash> Top; // top by characteristic -> query hash

    void Add(TQueryStatsPtr stats) {
        if (!stats->HasQueryTextHash()) {
            return;
        }

        auto queryHash = stats->GetQueryTextHash();
        auto hashIt = Stats.find(queryHash);

        if (hashIt != Stats.end()) {
            if (!TGreater()(stats, hashIt->second)) {
                return;
            }

            TValue old = TGreater::GetValue(hashIt->second);
            const auto range = Top.equal_range(old);
            auto it = range.first;
            for (; it != range.second; ++it) {
                if (it->second == queryHash) {
                    break;
                }
            }

            Y_ABORT_UNLESS(it != range.second);
            Top.erase(it);

            TValue current = TGreater::GetValue(stats);
            Top.emplace(std::make_pair(current, queryHash));

            hashIt->second = stats;

        } else {
            if (Top.size() == TOP_QUERIES_COUNT) {
                auto it = Top.begin();
                TValue value = TGreater::GetValue(stats);
                if (value <= it->first) {
                    return;
                }
                Stats.erase(it->second);
                Top.erase(it);
            }

            TValue current = TGreater::GetValue(stats);
            Top.emplace(std::make_pair(current, queryHash));
            Stats.emplace(std::make_pair(queryHash, stats));
        }
    }

    void Clear() {
        Stats.clear();
        Top.clear();
    }

    void Swap(TQueryStatsDedupBucket& other) {
        Stats.swap(other.Stats);
        Top.swap(other.Top);
    }

    void Fill(NKikimrSysView::TTopQueryStats* bucket, TQueryTextsByHash& texts) const {
        for (auto it = Top.rbegin(); it != Top.rend(); ++it) {
            auto hashIt = Stats.find(it->second);
            Y_ABORT_UNLESS(hashIt != Stats.end());

            auto* dstStats = bucket->AddStats();
            auto& srcStats = *hashIt->second;
            CopyQueryStatsNoText(dstStats, srcStats);
            texts.try_emplace(srcStats.GetQueryTextHash(), srcStats.GetQueryText());
        }
    }

    void FillSummary(NKikimrSysView::TEvIntervalQuerySummary::TQuerySet& queries) const {
        for (auto it = Top.rbegin(); it != Top.rend(); ++it) {
            queries.AddValues(it->first);
            queries.AddHashes(it->second);
        }
    }

    void FillStats(const NProtoBuf::RepeatedField<ui64>& hashes,
        NProtoBuf::RepeatedPtrField<NKikimrSysView::TQueryStats>& stats) const
    {
        for (auto queryHash : hashes) {
            auto it = Stats.find(queryHash);
            if (it == Stats.end()) {
                continue;
            }
            stats.Add()->CopyFrom(*it->second);
        }
    }
};

template <typename TGreater>
struct TAggrQueryStatsBucket
{
    NKikimrSysView::TTopQueryStats Top;

    void Merge(NKikimrSysView::TTopQueryStats&& src, const TQueryTextsByHash& texts) {
        auto& dst = Top;
        NKikimrSysView::TTopQueryStats result;

        size_t dstIndex = 0;
        size_t srcIndex = 0;

        for (size_t i = 0; i < TOP_QUERIES_COUNT; ++i) {
            auto copySrc = [&result, &texts] (NKikimrSysView::TQueryStats* srcPtr) {
                auto* stats = result.AddStats();
                stats->Swap(srcPtr);
                if (auto it = texts.find(stats->GetQueryTextHash()); it != texts.end()) {
                    stats->SetQueryText(it->second);
                }
            };

            if (dstIndex == dst.StatsSize()) {
                if (srcIndex == src.StatsSize()) {
                    break;
                }
                copySrc(src.MutableStats(srcIndex++));
            } else {
                if (srcIndex == src.StatsSize()) {
                    result.AddStats()->Swap(dst.MutableStats(dstIndex++));
                } else {
                    auto* dstPtr = dst.MutableStats(dstIndex);
                    auto* srcPtr = src.MutableStats(srcIndex);
                    if (TGreater()(dstPtr, srcPtr)) {
                        result.AddStats()->Swap(dstPtr);
                        ++dstIndex;
                    } else {
                        copySrc(srcPtr);
                        ++srcIndex;
                    }
                }
            }
        }

        Top.Swap(&result);
    }

    void Clear() {
        Top.Clear();
    }
};

template <typename TGreater>
struct TAggrQueryStatsDedupBucket
{
    NKikimrSysView::TTopQueryStats Top;

    void Merge(NKikimrSysView::TTopQueryStats&& src, const TQueryTextsByHash& texts) {
        auto& dst = Top;
        NKikimrSysView::TTopQueryStats result;

        size_t dstIndex = 0;
        size_t srcIndex = 0;
        std::unordered_set<TQueryHash> seenHashes;

        while (result.StatsSize() < TOP_QUERIES_COUNT) {
            auto copySrc = [&result, &texts] (NKikimrSysView::TQueryStats* srcPtr) {
                auto* stats = result.AddStats();
                stats->Swap(srcPtr);
                if (auto it = texts.find(stats->GetQueryTextHash()); it != texts.end()) {
                    stats->SetQueryText(it->second);
                }
            };

            if (dstIndex == dst.StatsSize()) {
                if (srcIndex == src.StatsSize()) {
                    break;
                }
                auto* srcPtr = src.MutableStats(srcIndex++);
                auto srcHash = srcPtr->GetQueryTextHash();
                if (seenHashes.find(srcHash) != seenHashes.end()) {
                    continue;
                }
                copySrc(srcPtr);
                seenHashes.insert(srcHash);
            } else {
                auto* dstPtr = dst.MutableStats(dstIndex);
                auto dstHash = dstPtr->GetQueryTextHash();
                if (seenHashes.find(dstHash) != seenHashes.end()) {
                    ++dstIndex;
                    continue;
                }
                if (srcIndex == src.StatsSize()) {
                    result.AddStats()->Swap(dstPtr);
                    seenHashes.insert(dstHash);
                    ++dstIndex;
                    continue;
                }
                auto* srcPtr = src.MutableStats(srcIndex);
                auto srcHash = srcPtr->GetQueryTextHash();
                if (seenHashes.find(srcHash) != seenHashes.end()) {
                    ++srcIndex;
                    continue;
                }
                if (TGreater()(dstPtr, srcPtr)) {
                    result.AddStats()->Swap(dstPtr);
                    seenHashes.insert(dstHash);
                    ++dstIndex;
                } else {
                    copySrc(srcPtr);
                    seenHashes.insert(srcHash);
                    ++srcIndex;
                }
            }
        }

        Top.Swap(&result);
    }

    void Clear() {
        Top.Clear();
    }
};

template <typename TBucket>
class TQueryHistoryBase
{
public:
    TQueryHistoryBase(ui64 bucketCount, const TDuration& bucketSize, const TInstant& now)
        : BucketCount(bucketCount)
        , BucketSizeMs(bucketSize.MilliSeconds())
    {
        Y_ABORT_UNLESS(bucketCount > 0);
        Y_ABORT_UNLESS(BucketSizeMs > 0);

        Buckets.resize(bucketCount);
        LastBucket = now.MilliSeconds() / BucketSizeMs;
    }

    void Clear() {
        for (auto& bucket : Buckets) {
            bucket.Clear();
        }
    }

    ui64 GetBucketCount() const {
        return BucketCount;
    }

    ui64 GetBucketSizeMs() const {
        return BucketSizeMs;
    }

    ui64 GetStartBucket() const {
        return LastBucket > BucketCount - 1 ? LastBucket - BucketCount + 1 : 0;
    }

    ui64 GetLastBucket() const {
        return LastBucket;
    }

    void RefreshBuckets(ui64 bucket) {
        if (bucket > LastBucket) {
            auto toClear = std::min(bucket, LastBucket + BucketCount);
            for (auto b = LastBucket + 1; b <= toClear; ++b) {
                Buckets[b % BucketCount].Clear();
            }
            LastBucket = bucket;
        }
    }

protected:
    const ui64 BucketCount;
    const ui64 BucketSizeMs;

    TVector<TBucket> Buckets;
    ui64 LastBucket = 0;
};

template <typename TGreater>
class TServiceQueryHistory : public TQueryHistoryBase<TQueryStatsDedupBucket<TGreater>>
{
    using TBase = TQueryHistoryBase<TQueryStatsDedupBucket<TGreater>>;

public:
    TServiceQueryHistory(ui64 bucketCount, const TDuration& bucketSize, const TInstant& now)
        : TBase(bucketCount, bucketSize, now)
    {}

    void Add(TQueryStatsPtr stats) {
        ui64 timestampMs = stats->GetEndTimeMs();
        auto bucket = timestampMs / this->BucketSizeMs;
        if (bucket < this->GetStartBucket()) {
            return; // too old
        }
        this->RefreshBuckets(bucket);
        this->Buckets[bucket % this->BucketCount].Add(stats);
    }

    void ToProto(ui64 startBucket, NKikimrSysView::TEvGetQueryStatsResult& message) const {
        ui64 fromBucket = std::max(startBucket, this->GetStartBucket());

        TQueryTextsByHash texts;

        for (ui64 b = fromBucket; b <= this->LastBucket; ++b) {
            const auto& bucket = this->Buckets[b % this->BucketCount];
            bucket.Fill(message.AddBuckets(), texts);
        }

        for (const auto& [hash, text] : texts) {
            auto* hashText = message.AddQueryTexts();
            hashText->SetHash(hash);
            hashText->SetText(text);
        }

        message.SetLastBucket(this->LastBucket);
        message.SetBucketSizeMs(this->BucketSizeMs);
    }
};

template <typename TGreater>
class TScanQueryHistory : public TQueryHistoryBase<TAggrQueryStatsDedupBucket<TGreater>>
{
    using TBase = TQueryHistoryBase<TAggrQueryStatsDedupBucket<TGreater>>;

public:
    TScanQueryHistory(ui64 bucketCount, const TDuration& bucketSize, const TInstant& now)
        : TBase(bucketCount, bucketSize, now)
    {}

    void Merge(NKikimrSysView::TEvGetQueryStatsResult&& message) {
        if (!message.HasLastBucket() || !message.HasBucketSizeMs()) {
            return;
        }
        if (message.GetBucketSizeMs() != this->BucketSizeMs) {
            return;
        }

        TQueryTextsByHash texts;
        for (const auto& hashedText : message.GetQueryTexts()) {
            texts.emplace(std::make_pair(hashedText.GetHash(), hashedText.GetText()));
        }

        ui64 lastBucket = message.GetLastBucket();
        this->RefreshBuckets(lastBucket);

        Y_ABORT_UNLESS(lastBucket + 1 >= message.BucketsSize());

        ui64 startBucket = lastBucket + 1 - message.BucketsSize();
        ui64 thisStartBucket = this->GetStartBucket();

        ui64 shift = thisStartBucket > startBucket ? thisStartBucket - startBucket : 0;
        for (ui64 b = shift; b < message.BucketsSize(); ++b) {
            auto bucket = b + startBucket;
            this->Buckets[bucket % this->BucketCount].Merge(
                std::move(*message.MutableBuckets(b)), texts);
        }
    }

    ui64 GetStartBucketIntervalEndMs() const {
        return (this->GetStartBucket() + 1) * this->BucketSizeMs;
    }

    const NKikimrSysView::TTopQueryStats& GetBucketTop(ui64 bucket) const {
        Y_ABORT_UNLESS(bucket >= this->GetStartBucket());
        Y_ABORT_UNLESS(bucket <= this->GetLastBucket());
        return this->Buckets[bucket % this->BucketCount].Top;
    }
};

} // NSysView
} // NKikimr
