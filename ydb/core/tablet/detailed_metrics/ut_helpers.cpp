#include "ut_helpers.h"

#include <library/cpp/json/json_prettifier.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/array_size.h>

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

////////////////////////////////////////////////////////////////////////////////

const TString TABLE_PATH = "/Root/db/dir/table";
const TString RELATIVE_TABLE_PATH = "dir/table";
const TTabletTypes::EType TABLET_TYPE = TTabletTypes::DataShard;

namespace {

constexpr const char* EXECUTOR_SIMPLE_COUNTER_NAMES[] = {
    "DbUniqueRowsTotal",
    "DbUniqueDataBytes",
    "NotInTheAllowList",
};

constexpr const char* EXECUTOR_CUMULATIVE_COUNTER_NAMES[] = {
    "ConsumedCPU",
};

constexpr const char* EXECUTOR_PERCENTILE_COUNTER_NAMES[] = {
    // A histogram aggregate: it is NOT filled by the tablet, it collects
    // one observation per tablet from the "ConsumedCPU" cumulative counter.
    // It is DataShard's only percentile: there is no ordinary one in the allow-list.
    "HIST(ConsumedCPU)",
};

constexpr const char* APP_CUMULATIVE_COUNTER_NAMES[] = {
    "DataShard/EngineHostRowUpdates",
    "DataShard/EngineHostRowUpdateBytes",
};

constexpr TTabletPercentileCounter::TRangeDef PERCENTILE_RANGES[] = {
	    {      0,   "0%"},
	    { 100000,  "10%"},
	    { 200000,  "20%"},
	    { 300000,  "30%"},
	    { 400000,  "40%"},
	    { 500000,  "50%"},
	    { 600000,  "60%"},
	    { 700000,  "70%"},
	    { 800000,  "80%"},
	    { 900000,  "90%"},
	    {1000000, "100%"},
};

} // namespace

TFakeTablet::TFakeTablet(ui64 tabletId, ui32 followerId)
    : TabletId(tabletId)
    , FollowerId(followerId)
    , ExecutorCounters(
        Y_ARRAY_SIZE(EXECUTOR_SIMPLE_COUNTER_NAMES),
        Y_ARRAY_SIZE(EXECUTOR_CUMULATIVE_COUNTER_NAMES),
        Y_ARRAY_SIZE(EXECUTOR_PERCENTILE_COUNTER_NAMES),
        EXECUTOR_SIMPLE_COUNTER_NAMES,
        EXECUTOR_CUMULATIVE_COUNTER_NAMES,
        EXECUTOR_PERCENTILE_COUNTER_NAMES
    )
    , AppCounters(
        0,
        Y_ARRAY_SIZE(APP_CUMULATIVE_COUNTER_NAMES),
        0,
        nullptr,
        APP_CUMULATIVE_COUNTER_NAMES,
        nullptr
    )
{
    for (ui32 i = 0; i < Y_ARRAY_SIZE(EXECUTOR_PERCENTILE_COUNTER_NAMES); ++i) {
        ExecutorCounters.Percentile()[i].Initialize(PERCENTILE_RANGES, true /* integral */);
    }
}

TFakeTablet& TFakeTablet::SetSimple(ESimpleCounter counter, ui64 value) {
    ExecutorCounters.Simple()[counter].Set(value);
    return *this;
}

TFakeTablet& TFakeTablet::AddCumulative(ECumulativeCounter counter, ui64 delta) {
    ExecutorCounters.Cumulative()[counter] += delta;
    return *this;
}

TFakeTablet& TFakeTablet::AddAppCumulative(EAppCumulativeCounter counter, ui64 delta) {
    AppCounters.Cumulative()[counter] += delta;
    return *this;
}

void TFakeTablet::Report(
    const TNodeDatabaseMetricsAggregatorPtr& aggregator,
    EDetailedMetricsLevel level,
    TInstant now,
    const TString& tablePath,
    TTabletTypes::EType tabletType
) {
    // An empty baseline (the very first report) makes the diff a plain copy
    auto appDiff = AppCounters.MakeDiffForAggr(AppBaseline);
    auto executorDiff = ExecutorCounters.MakeDiffForAggr(ExecutorBaseline);

    aggregator->AddCounters(
        tablePath,
        level,
        TabletId,
        FollowerId,
        tabletType,
        *executorDiff,
        *appDiff,
        now
    );

    AppCounters.RememberCurrentStateAsBaseline(AppBaseline);
    ExecutorCounters.RememberCurrentStateAsBaseline(ExecutorBaseline);
}

////////////////////////////////////////////////////////////////////////////////

THolder<TTabletCountersBase> MakeFakeExecutorCountersTemplate() {
    auto counters = MakeHolder<TTabletCountersBase>(
        Y_ARRAY_SIZE(EXECUTOR_SIMPLE_COUNTER_NAMES),
        Y_ARRAY_SIZE(EXECUTOR_CUMULATIVE_COUNTER_NAMES),
        Y_ARRAY_SIZE(EXECUTOR_PERCENTILE_COUNTER_NAMES),
        EXECUTOR_SIMPLE_COUNTER_NAMES,
        EXECUTOR_CUMULATIVE_COUNTER_NAMES,
        EXECUTOR_PERCENTILE_COUNTER_NAMES
    );

    for (ui32 i = 0; i < Y_ARRAY_SIZE(EXECUTOR_PERCENTILE_COUNTER_NAMES); ++i) {
        counters->Percentile()[i].Initialize(PERCENTILE_RANGES, true /* integral */);
    }

    return counters;
}

////////////////////////////////////////////////////////////////////////////////

NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> PackOnce(
    const TNodeDatabaseMetricsAggregatorPtr& aggregator,
    ui64 generation
) {
    NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> out;
    aggregator->Pack(out, generation);
    return out;
}

const NKikimrSysView::TDetailedTableCounters* FindPackedTable(
    const NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& tables,
    const TString& tablePath
) {
    for (const auto& table : tables) {
        if (table.GetTablePath() == tablePath) {
            return &table;
        }
    }
    return nullptr;
}

const NKikimrSysView::TDetailedTableCounters::TLeaf* FindPackedLeaf(
    const NKikimrSysView::TDetailedTableCounters& table,
    ui64 tabletId,
    ui32 followerId
) {
    for (const auto& leaf : table.GetLeaves()) {
        if (leaf.GetTabletId() == tabletId && leaf.GetFollowerId() == followerId) {
            return &leaf;
        }
    }
    return nullptr;
}

} // namespace NDetailedMetricsTests

} // namespace NKikimr
