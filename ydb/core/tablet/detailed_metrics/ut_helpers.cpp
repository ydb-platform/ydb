#include "ut_helpers.h"

#include <ydb/core/sys_view/service/db_counters_codec.h>

#include <library/cpp/json/json_prettifier.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/digest/multi.h>
#include <util/generic/maybe.h>
#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr {

namespace NDetailedMetricsTests {

TString NormalizeJson(const TString& jsonString) {
    NJson::TJsonValue parsedJson;
    UNIT_ASSERT(NJson::ReadJsonTree(TStringBuf(jsonString), &parsedJson));

    // NOTE: The prettifier is needed here to make sure all brackets (both [] and {})
    //       are aligned "Python style" with the opening bracket placed on the starting line.
    //       By default, WriteJson() places opening brackets on a separate line and makes
    //       all the inner strings double-aligned, which takes too many lines
    //       and makes it harder for humans to read.
    return NJson::PrettifyJson(
        NJson::WriteJson(
            parsedJson,
            true /* formatOutput */,
            true /* sortkeys */
        ),
        false /* unquote */,
        2 /* padding */
    );
}

TPackedBucketId TPackedBucketId::Table(const TString& tablePath) {
    return TPackedBucketId{tablePath, NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable, Nothing()};
}

TPackedBucketId TPackedBucketId::Leaf(const TString& tablePath, ui64 tabletId, ui32 followerId) {
    return TPackedBucketId{
        tablePath,
        NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
        NDetailedMetrics::TTabletKey(tabletId, followerId)};
}

bool TPackedBucketId::operator==(const TPackedBucketId& other) const {
    return TablePath == other.TablePath && Level == other.Level && Bucket == other.Bucket;
}

TString TPackedBucketId::ToString() const {
    TStringBuilder result;
    result << TablePath << " " << NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel_Name(Level);
    if (Bucket) {
        result << " " << Bucket->first << ":" << Bucket->second;
    }
    return result;
}

size_t TPackedBucketId::THash::operator()(const TPackedBucketId& id) const {
    return MultiHash(
        id.TablePath,
        static_cast<int>(id.Level),
        id.Bucket.Defined(),
        id.Bucket ? id.Bucket->first : ui64(0),
        id.Bucket ? id.Bucket->second : ui32(0));
}

void TPackedReceiver::Fold(const TTables& tables) {
    Live.clear();

    for (const auto& table : tables) {
        UNIT_ASSERT_C(table.HasTabletType(), "no tablet type of the table " << table.GetTablePath());

        if (table.HasTableMetrics()) {
            Apply(TPackedBucketId{table.GetTablePath(), table.GetLevel(), Nothing()}, table.GetTableMetrics());
        }

        for (const auto& leaf : table.GetLeaves()) {
            const TPackedBucketId bucket{
                table.GetTablePath(),
                table.GetLevel(),
                NDetailedMetrics::TTabletKey(leaf.GetTabletId(), leaf.GetFollowerId())};
            UNIT_ASSERT_C(leaf.HasMetrics(), "no public metric values of the leaf " << bucket.ToString());
            Apply(bucket, leaf.GetMetrics());
        }
    }
}

void TPackedReceiver::Refresh(const TVector<NSysView::IDbDetailedCounters*>& sources) {
    TTables tables;
    for (auto* source : sources) {
        source->Pack(tables);
    }
    Fold(tables);
}

void TPackedReceiver::Settle(NSysView::IDbDetailedCounters& source) {
    Settle(TVector<NSysView::IDbDetailedCounters*>{&source});
}

void TPackedReceiver::Settle(const TVector<NSysView::IDbDetailedCounters*>& sources) {
    Refresh(sources);
    Refresh(sources);
}

bool TPackedReceiver::Exists(const TPackedBucketId& bucket) const {
    return Live.contains(bucket);
}

size_t TPackedReceiver::LiveCount() const {
    return Live.size();
}

size_t TPackedReceiver::LiveCount(const TString& tablePath, EMetricsLevel level) const {
    return std::count_if(Live.begin(), Live.end(), [&](const TPackedBucketId& bucket) {
        return bucket.TablePath == tablePath && bucket.Level == level;
    });
}

const NKikimrSysView::TDbCounters& TPackedReceiver::Get(const TPackedBucketId& bucket) const {
    auto it = State.find(bucket);
    UNIT_ASSERT_C(it != State.end(), "the bucket " << bucket.ToString() << " was never reported");
    return it->second;
}

ui64 TPackedReceiver::Gauge(const TPackedBucketId& bucket, ui32 metric) const {
    const auto& values = Get(bucket);
    UNIT_ASSERT_C(metric < static_cast<ui32>(values.SimpleSize()), "no gauge " << metric << " of " << bucket.ToString());
    return values.GetSimple(metric);
}

ui64 TPackedReceiver::Rate(const TPackedBucketId& bucket, ui32 metric) const {
    const auto& values = Get(bucket);
    UNIT_ASSERT_C(metric < static_cast<ui32>(values.CumulativeSize()), "no rate " << metric << " of " << bucket.ToString());
    return values.GetCumulative(metric);
}

TVector<ui64> TPackedReceiver::Hist(const TPackedBucketId& bucket, ui32 metric) const {
    const auto& values = Get(bucket);
    UNIT_ASSERT_C(metric < static_cast<ui32>(values.HistogramSize()), "no histogram " << metric << " of " << bucket.ToString());
    const auto& buckets = values.GetHistogram(metric).GetBuckets();
    return TVector<ui64>(buckets.begin(), buckets.end());
}

ui64 TPackedReceiver::HistTotal(const TPackedBucketId& bucket, ui32 metric) const {
    ui64 total = 0;
    for (ui64 value : Hist(bucket, metric)) {
        total += value;
    }
    return total;
}

void TPackedReceiver::Apply(const TPackedBucketId& bucket, const NKikimrSysView::TDbCounters& values) {
    // A bucket listed twice would count its deltas twice
    UNIT_ASSERT_C(Live.insert(bucket).second, "the bucket " << bucket.ToString() << " is reported twice");

    auto& state = State[bucket];

    state.MutableSimple()->CopyFrom(values.GetSimple());

    NSysView::TAggregateCumulative<false>::Apply(&state, values);

    for (size_t i = 0; i < values.HistogramSize(); ++i) {
        const auto& histogram = values.GetHistogram(i);
        if (!histogram.GetNonDerivative()) {
            continue;
        }

        const ui64 bucketCount = histogram.GetBucketsCount();
        auto* buckets = state.MutableHistogram(i)->MutableBuckets();
        buckets->Clear();
        buckets->Resize(static_cast<int>(bucketCount), 0);

        const auto& encoded = histogram.GetBuckets();
        UNIT_ASSERT_VALUES_EQUAL_C(encoded.size() % 2, 0, "an odd encoded histogram " << i << " of " << bucket.ToString());
        for (int b = 0; b + 1 < encoded.size(); b += 2) {
            UNIT_ASSERT_C(encoded.Get(b) < bucketCount,
                "bucket " << encoded.Get(b) << " of the histogram " << i << " of " << bucket.ToString() << " is out of range");
            (*buckets)[static_cast<int>(encoded.Get(b))] = encoded.Get(b + 1);
        }
    }
}

} // namespace NDetailedMetricsTests

} // namespace NKikimr
