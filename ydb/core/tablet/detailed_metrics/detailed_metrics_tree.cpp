#include "detailed_metrics_tree.h"

#include "detailed_counters_diff.h"
#include "detailed_metrics_counter_set.h"

#include <ydb/core/tablet/tablet_counters_app.h>

namespace NKikimr {

TStringBuf ChopTrailingSlash(const TStringBuf path) {
    TStringBuf chopped(path);
    chopped.ChopSuffix("/");
    return chopped;
}

TStringBuf MakeRelativeTablePath(const TStringBuf databasePrefix, const TString& tablePath) {
    TStringBuf relativePath(tablePath);

    // The "/" is required, so that /Root/db10/table is NOT stripped down to "0/table"
    // within the database /Root/db1
    if (relativePath.SkipPrefix(databasePrefix) && relativePath.SkipPrefix("/") && !relativePath.empty()) {
        return relativePath;
    }

    return TStringBuf(tablePath);
}

TCountersBucket::TCountersBucket(
    NMonitoring::TDynamicCounterPtr bucketGroup,
    TTabletTypes::EType tabletType,
    const TDetailedMetricsCounterNames& counterNames,
    NMonitoring::TCountableBase::EVisibility visibility
)
    : TabletType(tabletType)
    , TypeGroup(bucketGroup->GetSubgroup(TYPE_LABEL, TTabletTypes::TypeToStr(tabletType)))
    , ExecutorCounters(TypeGroup->GetSubgroup(CATEGORY_LABEL, EXECUTOR_CATEGORY), visibility)
    , AppCounters(TypeGroup->GetSubgroup(CATEGORY_LABEL, APP_CATEGORY), visibility)
    , CounterNames(&counterNames)
{}

void TCountersBucket::Apply(
    const TTabletKey& tablet,
    const TTabletCountersBase& executorCounters,
    const TTabletCountersBase& appCounters,
    TInstant now
) {
    // The aggregates identify their sources by a single ui64, while a bucket may hold
    // several followers of the same tablet, hence the synthetic source IDs
    auto [it, inserted] = SourceIds.try_emplace(tablet, NextSourceId);
    if (inserted) {
        ++NextSourceId;
    }

    if (!ExecutorCounters.IsInitialized) {
        ExecutorCounters.Initialize(&executorCounters, &CounterNames->ExecutorNames);
    }
    if (!AppCounters.IsInitialized) {
        AppCounters.Initialize(&appCounters, &CounterNames->AppNames);
    }

    ExecutorCounters.Apply(it->second, &executorCounters, TabletType, now);
    AppCounters.Apply(it->second, &appCounters, TabletType, now);
}

void TCountersBucket::Forget(const TTabletKey& tablet) {
    auto it = SourceIds.find(tablet);
    if (it == SourceIds.end()) {
        return;
    }

    if (ExecutorCounters.IsInitialized) {
        ExecutorCounters.Forget(it->second);
    }
    if (AppCounters.IsInitialized) {
        AppCounters.Forget(it->second);
    }

    SourceIds.erase(it);
}

bool TCountersBucket::IsEmpty() const {
    return SourceIds.empty();
}

void TCountersBucket::RecalcAll() {
    if (ExecutorCounters.IsInitialized) {
        ExecutorCounters.RecalcAll();
    }
    if (AppCounters.IsInitialized) {
        AppCounters.RecalcAll();
    }
}

void TCountersBucket::Pack(NKikimrSysView::TDbTabletCounters& out, ui64 generation) {
    if (!HasPacked || generation != LastPackedGeneration) {
        Confirmed.Swap(&Current);

        RecalcAll();

        Current.Clear();
        Current.SetType(TabletType);
        if (ExecutorCounters.IsInitialized) {
            ExecutorCounters.ToProto(*Current.MutableExecutorCounters(), *Current.MutableMaxExecutorCounters());
        }
        if (AppCounters.IsInitialized) {
            AppCounters.ToProto(*Current.MutableAppCounters(), *Current.MutableMaxAppCounters());
        }

        LastPackedGeneration = generation;
        HasPacked = true;
    }

    out.Clear();
    CalculateCountersDiff(&out, Current, Confirmed);
}

void TCountersBucket::EnsureInitialized(
    TTabletTypes::EType tabletType,
    const TTabletCountersBase* executorCountersTemplate
) {
    Y_DEBUG_ABORT_UNLESS(tabletType == TabletType);

    if (!ExecutorCounters.IsInitialized) {
        ExecutorCounters.Initialize(executorCountersTemplate, &CounterNames->ExecutorNames);
    }
    if (!AppCounters.IsInitialized) {
        auto appTemplate = CreateAppCountersByTabletType(tabletType);
        AppCounters.Initialize(appTemplate.Get(), &CounterNames->AppNames);
    }
}

void TCountersBucket::FromProto(
    NKikimrSysView::TDbTabletCounters& proto,
    const TTabletCountersBase* executorCountersTemplate
) {
    EnsureInitialized(TabletType, executorCountersTemplate);

    if (ExecutorCounters.IsInitialized) {
        ExecutorCounters.FromProto(*proto.MutableExecutorCounters(), *proto.MutableMaxExecutorCounters());
    }
    if (AppCounters.IsInitialized) {
        AppCounters.FromProto(*proto.MutableAppCounters(), *proto.MutableMaxAppCounters());
    }
}

} // namespace NKikimr
