#include "detailed_metrics_counter_set.h"

#include <ydb/core/protos/counters_detailed_datashard.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>

namespace NKikimr {

namespace {

void InsertCounterName(THashSet<TString>& names, TStringBuf sourceName) {
    names.insert(TString(sourceName));

    for (const TStringBuf prefix : {TStringBuf("SUM("), TStringBuf("MAX(")}) {
        if (sourceName.StartsWith(prefix) && sourceName.EndsWith(")")) {
            names.insert(TString(sourceName.SubStr(prefix.size(), sourceName.size() - prefix.size() - 1)));
        }
    }
}

template <class TOpts>
void CollectCounterNames(const TOpts* opts, TDetailedMetricsCounterNames& names) {
    for (size_t i = 0; i < opts->Size; ++i) {
        for (const auto& source : opts->GetSourceCounters(i)) {
            auto& target = source.GetCategory() == ESourceCounterCategory::SCC_TABLET
                ? names.AppNames
                : names.ExecutorNames;

            InsertCounterName(target, source.GetName());
        }
    }
}

const TDetailedMetricsCounterNames* GetDataShardCounterNames() {
    static const TDetailedMetricsCounterNames names = [] {
        TDetailedMetricsCounterNames result;

        CollectCounterNames(
            NAux::GetAppOpts<NDataShard::ESimpleDetailedCounters_descriptor, true /* ParseSourceCounters */>(),
            result);
        CollectCounterNames(
            NAux::GetAppOpts<NDataShard::ECumulativeDetailedCounters_descriptor, true /* ParseSourceCounters */>(),
            result);
        CollectCounterNames(
            NAux::GetAppOpts<NDataShard::EPercentileDetailedCounters_descriptor, true /* ParseSourceCounters */>(),
            result);

        return result;
    }();

    return &names;
}

} // namespace

const TDetailedMetricsCounterNames* GetDetailedMetricsCounterNames(TTabletTypes::EType tabletType) {
    switch (tabletType) {
    case TTabletTypes::DataShard:
        return GetDataShardCounterNames();

    default:
        return nullptr;
    }
}

} // namespace NKikimr
