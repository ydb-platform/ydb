#pragma once

#include "detailed_metrics_tree.h"

#include <ydb/core/protos/counters_detailed_datashard.pb.h>
#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/protos/table_metrics_settings.pb.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/tablet/tablet_counters.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {

namespace NDetailedMetricsTests {

/**
 * The public DataShard metrics, which the tests read: the slots of the packed public metric values
 * (see NKikimrSysView::TDbCounters) and the source counters they are computed from.
 */
constexpr ui32 ROW_COUNT = NDataShard::COUNTER_DATASHARD_ROW_COUNT;                                 // SUM(DbUniqueRowsTotal), LeaderOnly
constexpr ui32 SIZE_BYTES = NDataShard::COUNTER_DATASHARD_SIZE_BYTES;                               // SUM(DbUniqueDataBytes), LeaderOnly
constexpr ui32 WRITE_ROWS = NDataShard::COUNTER_DATASHARD_WRITE_ROWS;                               // DataShard/EngineHostRowUpdates, LeaderOnly
constexpr ui32 CONSUMED_CPU_MICROSECONDS = NDataShard::COUNTER_DATASHARD_CONSUMED_CPU_MICROSECONDS; // ConsumedCPU
constexpr ui32 USED_CORE_PERCENTS = NDataShard::COUNTER_DATASHARD_USED_CORE_PERCENTS;               // HIST(ConsumedCPU)

/**
 * Normalizes the given JSON to be well formatted with all keys sorted.
 *
 * @warning This function sorts only maps by the key value. It does not sort
 *          items in arrays at all. Luckily, all counters and groups in TDynamicCounters
 *          are stored in SORTED maps, which means that the array of sensors
 *          is always inherently sorted in a stable order. This makes it safe
 *          to compare sensor arrays directly without sorting them.
 *
 * @param[in] jsonString The JSON to normalize (as a string)
 *
 * @return The corresponding normalized JSON
 */
TString NormalizeJson(const TString& jsonString);

/**
 * The counter names of one bank of a synthetic counter layout, nullptr is an unnamed slot.
 */
struct TTestNames {
    TVector<const char*> Simple;
    TVector<const char*> Cumulative;
    TVector<const char*> Percentile;
};

/**
 * One bank (the Executor or the application counters) of a synthetic counter layout.
 */
class TTestCounters {
public:
    explicit TTestCounters(TTestNames names = {})
        : Names(std::move(names))
        , Counters(MakeHolder<TTabletCountersBase>(
            Names.Simple.size(),
            Names.Cumulative.size(),
            Names.Percentile.size(),
            Names.Simple.data(),
            Names.Cumulative.data(),
            Names.Percentile.data()))
    {
    }

    template <ui32 RangeCount>
    TTestCounters& InitPercentile(
        ui32 slot,
        const TTabletPercentileCounter::TRangeDef (&ranges)[RangeCount],
        bool integral)
    {
        Counters->Percentile()[slot].Initialize(ranges, integral);
        return *this;
    }

    const TTabletCountersBase& Get() const {
        return *Counters;
    }

    TTabletCountersBase& Get() {
        return *Counters;
    }

private:
    // NOTE: The counters keep pointers into the name vectors, which survive a move
    TTestNames Names;
    THolder<TTabletCountersBase> Counters;
};

/**
 * A bucket on the detailed wire: the table (as reported), the level of its entry and the leaf
 * (none for the TABLE bucket).
 */
struct TPackedBucketId {
    using EMetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel;

    TString TablePath;
    EMetricsLevel Level = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelUnspecified;
    NDetailedMetrics::TBucketKey Bucket;

    static TPackedBucketId Table(const TString& tablePath);
    static TPackedBucketId Leaf(const TString& tablePath, ui64 tabletId, ui32 followerId);

    bool operator==(const TPackedBucketId& other) const;

    TString ToString() const;

    struct THash {
        size_t operator()(const TPackedBucketId& id) const;
    };
};

/**
 * The receiving end of the detailed wire in the tests: it folds the reports the way the SysView
 * Processor applies the reports of one node (gauges and level histograms replaced, rate deltas
 * and increment buckets added up). The buckets of the latest report are the live ones (Exists()),
 * the folded values of a bucket outlive it.
 *
 * @note Pack() drains the deltas, so a test reads a sender through one receiver only.
 */
class TPackedReceiver {
public:
    using EMetricsLevel = TPackedBucketId::EMetricsLevel;
    using TTables = NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>;

    void Fold(const TTables& tables);

    /**
     * Pack every source into one report (for example, the aggregators of both roles of a node) and fold it.
     */
    void Refresh(const TVector<NSysView::IDbDetailedCounters*>& sources);

    /**
     * Refresh twice: the first report also carries the final values of the buckets retired since
     * the previous one, so only the buckets, which are still there, are live afterwards.
     */
    void Settle(NSysView::IDbDetailedCounters& source);
    void Settle(const TVector<NSysView::IDbDetailedCounters*>& sources);

    bool Exists(const TPackedBucketId& bucket) const;
    size_t LiveCount() const;
    size_t LiveCount(const TString& tablePath, EMetricsLevel level) const;

    /**
     * @return The folded values of a reported bucket: every gauge, the total of every rate,
     *         every bucket of every histogram
     */
    const NKikimrSysView::TDbCounters& Get(const TPackedBucketId& bucket) const;

    ui64 Gauge(const TPackedBucketId& bucket, ui32 metric) const;
    ui64 Rate(const TPackedBucketId& bucket, ui32 metric) const;
    TVector<ui64> Hist(const TPackedBucketId& bucket, ui32 metric) const;
    ui64 HistTotal(const TPackedBucketId& bucket, ui32 metric) const;

private:
    void Apply(const TPackedBucketId& bucket, const NKikimrSysView::TDbCounters& values);

private:
    THashMap<TPackedBucketId, NKikimrSysView::TDbCounters, TPackedBucketId::THash> State;
    THashSet<TPackedBucketId, TPackedBucketId::THash> Live;
};

} // namespace NDetailedMetricsTests

} // namespace NKikimr
