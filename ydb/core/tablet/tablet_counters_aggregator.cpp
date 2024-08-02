#include "tablet_counters_aggregator.h"
#include "tablet_counters_app.h"
#include "labeled_counters_merger.h"
#include "labeled_db_counters.h"
#include "private/aggregated_counters.h"
#include "private/labeled_db_counters.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/time_provider/time_provider.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/service/db_counters.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/util/wildcard.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/monlib/dynamic_counters/encode.h>

#include <util/generic/xrange.h>
#include <util/string/vector.h>
#include <util/string/split.h>

#ifdef _darwin_
#pragma GCC diagnostic ignored "-Wformat"
#endif

////////////////////////////////////////////
namespace NKikimr {

const ui32 WAKEUP_TIMEOUT_SECONDS = 4;

TStringBuf GetHistogramAggregateSimpleName(TStringBuf name) {
    TStringBuf buffer(name);
    if (buffer.SkipPrefix("HIST(")) {
        return buffer.Before(')');
    }
    return TStringBuf();
}

bool IsHistogramAggregateSimpleName(TStringBuf name) {
    return !GetHistogramAggregateSimpleName(name).empty();
}

TTabletLabeledCountersResponseContext::TTabletLabeledCountersResponseContext(NKikimrLabeledCounters::TEvTabletLabeledCountersResponse& response)
    : Response(response)
{}

ui32 TTabletLabeledCountersResponseContext::GetNameId(TStringBuf name) {
    auto it = NamesToId.find(name);
    if (it != NamesToId.end()) {
        return it->second;
    }
    Response.AddCounterNames(TString(name));
    ui32 id = Response.CounterNamesSize() - 1;
    NamesToId[name] = id;
    return id;
}

TActorId MakeTabletCountersAggregatorID(ui32 node, bool follower) {
    if (!follower) {
        char x[12] = {'t','a','b','l','c','o','u','n','t','a','g','g'};
        return TActorId(node, TStringBuf(x, 12));
    } else {
        char x[12] ={'s','l','a','v','c','o','u','n','t','a','g','g'};
        return TActorId(node, TStringBuf(x, 12));
    }
}

////////////////////////////////////////////
class TTabletMon {
public:
    //
    TTabletMon(::NMonitoring::TDynamicCounterPtr counters, bool isFollower, TActorId dbWatcherActorId)
        : Counters(GetServiceCounters(counters, isFollower ? "followers" : "tablets"))
        , AllTypes(MakeIntrusive<TTabletCountersForTabletType>(Counters.Get(), "type", "all"))
        , IsFollower(isFollower)
        , DbWatcherActorId(dbWatcherActorId)
    {
        if (!IsFollower) {
            YdbCounters = MakeIntrusive<TYdbTabletCounters>(GetServiceCounters(counters, "ydb"));
        }
    }

    void Apply(ui64 tabletId, TTabletTypes::EType tabletType, TPathId tenantPathId,
        const TTabletCountersBase* executorCounters, const TTabletCountersBase* appCounters,
        const TActorContext& ctx)
    {
        AllTypes->Apply(tabletId, executorCounters, nullptr, tabletType);
        //
        auto typeCounters = GetOrAddCountersByTabletType(tabletType, CountersByTabletType, Counters);
        if (typeCounters) {
            typeCounters->Apply(tabletId, executorCounters, appCounters, tabletType);
        }
        //
        if (!IsFollower && AppData(ctx)->FeatureFlags.GetEnableDbCounters() && tenantPathId) {
            auto dbCounters = GetDbCounters(tenantPathId, ctx);
            if (dbCounters) {
                auto* limitedAppCounters = GetOrAddLimitedAppCounters(tabletType);
                dbCounters->Apply(tabletId, executorCounters, appCounters, tabletType, limitedAppCounters);
            }
        }

        //
        auto& quietStats = QuietTabletCounters[tabletId];

        if (executorCounters) {
            if (quietStats.first == nullptr)
                quietStats.first = new TTabletCountersBase();
            quietStats.first->Populate(*executorCounters);
        }

        if (appCounters) {
            if (quietStats.second == nullptr)
                quietStats.second = new TTabletCountersBase();
            quietStats.second->Populate(*appCounters);
        }
    }

    void ApplyLabeledCounters(ui64 tabletId, TTabletTypes::EType tabletType, const TTabletLabeledCountersBase* labeledCounters) {

        auto iterTabletType = LabeledCountersByTabletTypeAndGroup.find(std::make_pair(tabletType, labeledCounters->GetGroup()));

        if (labeledCounters->GetDrop() ) {
            if (iterTabletType != LabeledCountersByTabletTypeAndGroup.end()) {
                LabeledCountersByTabletTypeAndGroup.erase(iterTabletType);
            }
            return;
        }

        if (iterTabletType == LabeledCountersByTabletTypeAndGroup.end()) {
            TString tabletTypeStr = TTabletTypes::TypeToStr(tabletType);
            TString groupNames;
            TVector<TString> rr;
            StringSplitter(labeledCounters->GetGroup()).Split('/').SkipEmpty().Collect(&rr); // TODO: change here to "|"
            for (ui32 i = 0; i < rr.size(); ++i) {
                if (i > 0)
                    groupNames += '/';
                groupNames += labeledCounters->GetGroupName(i);
            }
            iterTabletType = LabeledCountersByTabletTypeAndGroup.emplace(
                                            std::make_pair(tabletType, labeledCounters->GetGroup()),
                                            new NPrivate::TAggregatedLabeledCounters(
                                                labeledCounters->GetCounters().Size(),
                                                labeledCounters->GetAggrFuncs(),
                                                labeledCounters->GetNames(),
                                                labeledCounters->GetTypes(), groupNames)
                        ).first;

        }

        for (ui32 i = 0, e = labeledCounters->GetCounters().Size(); i < e; ++i) {
            if(!strlen(labeledCounters->GetCounterName(i))) 
                continue;
            const ui64& value = labeledCounters->GetCounters()[i].Get();
            const ui64& id = labeledCounters->GetIds()[i].Get();
            iterTabletType->second->SetValue(tabletId, i, value, id);
        }
    }

    void ApplyLabeledDbCounters(const TString& dbName, ui64 tabletId,
                                const TTabletLabeledCountersBase* labeledCounters, const TActorContext& ctx) {
        auto iterDbLabeled = GetLabeledDbCounters(dbName, ctx);
        iterDbLabeled->Apply(tabletId, labeledCounters);
    }

    void ForgetTablet(ui64 tabletId, TTabletTypes::EType tabletType, TPathId tenantPathId) {
        AllTypes->Forget(tabletId);
        // and now erase from every other path
        auto iterTabletType = CountersByTabletType.find(tabletType);
        if (iterTabletType != CountersByTabletType.end()) {
            iterTabletType->second->Forget(tabletId);
        }
        // from db counters
        if (auto itPath = CountersByPathId.find(tenantPathId); itPath != CountersByPathId.end()) {
            itPath->second->Forget(tabletId, tabletType);
        }

        for (auto iter = LabeledDbCounters.begin(); iter != LabeledDbCounters.end(); ++iter) {
            iter->second->ForgetTablet(tabletId);
        }
        // and from all labeledCounters that could have this tablet
        auto iterTabletTypeAndGroup =
                LabeledCountersByTabletTypeAndGroup.lower_bound(std::make_pair(tabletType, TString()));
        for (; iterTabletTypeAndGroup != LabeledCountersByTabletTypeAndGroup.end() &&
               iterTabletTypeAndGroup->first.first == tabletType; ) {
            bool empty = iterTabletTypeAndGroup->second->ForgetTablet(tabletId);
            if (empty) {
                iterTabletTypeAndGroup = LabeledCountersByTabletTypeAndGroup.erase(iterTabletTypeAndGroup);
            } else {
                ++iterTabletTypeAndGroup;
            }
        }

        QuietTabletCounters.erase(tabletId);

        TString tabletIdStr = Sprintf("%" PRIu64, tabletId);
        Counters->RemoveSubgroup("tabletid", tabletIdStr.data());
    }

    void Query(const NKikimrTabletCountersAggregator::TEvTabletCountersRequest& request, NKikimrTabletCountersAggregator::TEvTabletCountersResponse& response) {
        TVector<ui64> tabletIds(request.GetTabletIds().begin(), request.GetTabletIds().end());
        if (tabletIds.empty()) {
            for (const auto& pr : QuietTabletCounters) {
                auto& countersInfo = *response.AddCountersInfo();
                countersInfo.SetTabletId(pr.first);
                if (pr.second.first) { // executor counters
                    auto& executorCounters = *countersInfo.MutableExecutorCounters();
                    const auto& simple = pr.second.first->Simple();
                    for (ui32 i = 0; i < simple.Size(); ++i) {
                        executorCounters.AddSimpleCounters(simple[i].Get());
                    }
                    const auto& cumulative = pr.second.first->Cumulative();
                    for (ui32 i = 0; i < cumulative.Size(); ++i) {
                        executorCounters.AddCumulativeCounters(cumulative[i].Get());
                    }
                }
                if (pr.second.second) { // app counters
                    auto& appCounters = *countersInfo.MutableAppCounters();
                    const auto& simple = pr.second.second->Simple();
                    for (ui32 i = 0; i < simple.Size(); ++i) {
                        appCounters.AddSimpleCounters(simple[i].Get());
                    }
                    const auto& cumulative = pr.second.second->Cumulative();
                    for (ui32 i = 0; i < cumulative.Size(); ++i) {
                        appCounters.AddCumulativeCounters(cumulative[i].Get());
                    }
                }
            }
        } else {
            for (ui64 tabletId : tabletIds) {
                auto it = QuietTabletCounters.find(tabletId);
                if (it != QuietTabletCounters.end()) {
                    auto& countersInfo = *response.AddCountersInfo();
                    countersInfo.SetTabletId(it->first);
                    if (it->second.first) { // executor counters
                        auto& executorCounters = *countersInfo.MutableExecutorCounters();
                        const auto& simple = it->second.first->Simple();
                        for (ui32 i = 0; i < simple.Size(); ++i) {
                            executorCounters.AddSimpleCounters(simple[i].Get());
                        }
                        const auto& cumulative = it->second.first->Cumulative();
                        for (ui32 i = 0; i < cumulative.Size(); ++i) {
                            executorCounters.AddCumulativeCounters(cumulative[i].Get());
                        }
                    }
                    if (it->second.second) { // app counters
                        auto& appCounters = *countersInfo.MutableAppCounters();
                        const auto& simple = it->second.second->Simple();
                        for (ui32 i = 0; i < simple.Size(); ++i) {
                            appCounters.AddSimpleCounters(simple[i].Get());
                        }
                        const auto& cumulative = it->second.second->Cumulative();
                        for (ui32 i = 0; i < cumulative.Size(); ++i) {
                            appCounters.AddCumulativeCounters(cumulative[i].Get());
                        }
                    }
                }
            }
        }
    }

    void QueryLabeledCounters(const NKikimrLabeledCounters::TEvTabletLabeledCountersRequest& request, NKikimrLabeledCounters::TEvTabletLabeledCountersResponse& response, const TActorContext& ctx) {

        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "got request v" << request.GetVersion());

        TString group = request.HasGroup() ? request.GetGroup() : "";
        TTabletTypes::EType tabletType = request.GetTabletType();
        ui32 cc = 0;

        if (request.GetVersion() == 1) {
            auto iter = LabeledCountersByTabletTypeAndGroup.lower_bound(std::make_pair(tabletType, group));
            for (; iter != LabeledCountersByTabletTypeAndGroup.end() && iter->first.first == tabletType &&
                    (group.empty() || iter->first.second == group); ++iter) {

                ui32 s = 0, e = iter->second->Size();
                if (request.HasLabeledCounterId()) {
                    s = request.GetLabeledCounterId();
                    e = s + 1;
                }
                if (s >= iter->second->Size())
                    continue;
                auto labeledCountersByGroup = response.AddLabeledCountersByGroup();

                iter->second->FillGetRequestV1(labeledCountersByGroup, iter->first.second, s, e);
                ++cc;
            }
        } else if (request.GetVersion() >= 2) {
            TTabletLabeledCountersResponseContext context(response);
            auto iter = LabeledCountersByTabletTypeAndGroup.lower_bound({tabletType, TString()});
            for (; iter != LabeledCountersByTabletTypeAndGroup.end()
                   && iter->first.first == tabletType; ++iter) {
                if (group.empty() || IsMatchesWildcards(iter->first.second, group)) {
                    iter->second->FillGetRequestV2(&context, iter->first.second);
                }
                ++cc;
            }
        }
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "request processed, " << cc << " groups processed");
    }

    void RecalcAll() {
        AllTypes->RecalcAll();
        for (auto& [_, counters] : CountersByTabletType) {
            counters->RecalcAll();
        }

        if (YdbCounters) {
            auto hasDatashard = (bool)FindCountersByTabletType(
                TTabletTypes::DataShard, CountersByTabletType);
            auto hasSchemeshard = (bool)FindCountersByTabletType(
                TTabletTypes::SchemeShard, CountersByTabletType);
            bool hasColumnShard = static_cast<bool>(FindCountersByTabletType(TTabletTypes::ColumnShard, CountersByTabletType));
            YdbCounters->Initialize(Counters, hasDatashard, hasSchemeshard, hasColumnShard);
            YdbCounters->Transform();
        }
    }

    void RemoveTabletsByPathId(TPathId pathId) {
        CountersByPathId.erase(pathId);
    }

    void RemoveTabletsByDbPath(const TString& dbPath) {
        LabeledDbCounters.erase(dbPath);
    }

private:
    // subgroups
    class TTabletCountersForTabletType : public TThrRefBase {
    public:
        //
        TTabletCountersForTabletType(::NMonitoring::TDynamicCounters* owner, const char* category, const char* name)
            : TabletCountersSection(owner->GetSubgroup(category, name))
            , TabletExecutorCountersSection(TabletCountersSection->GetSubgroup("category", "executor"))
            , TabletAppCountersSection(TabletCountersSection->GetSubgroup("category", "app"))
            , TabletExecutorCounters(TabletExecutorCountersSection)
            , TabletAppCounters(TabletAppCountersSection)
        {}

        void Apply(ui64 tabletId,
            const TTabletCountersBase* executorCounters,
            const TTabletCountersBase* appCounters,
            TTabletTypes::EType tabletType,
            const TTabletCountersBase* limitedAppCounters = {})
        {
            Y_ABORT_UNLESS(executorCounters);

            if (executorCounters) {
                if (!TabletExecutorCounters.IsInitialized) {
                    TabletExecutorCounters.Initialize(executorCounters);
                }
                TabletExecutorCounters.Apply(tabletId, executorCounters, tabletType);
            }

            if (appCounters) {
                if (!TabletAppCounters.IsInitialized) {
                    TabletAppCounters.Initialize(limitedAppCounters ? limitedAppCounters : appCounters);
                }
                TabletAppCounters.Apply(tabletId, appCounters, tabletType);
            }
        }

        void Forget(ui64 tabletId) {
            if (TabletExecutorCounters.IsInitialized) {
                TabletExecutorCounters.Forget(tabletId);
            }
            if (TabletAppCounters.IsInitialized) {
                TabletAppCounters.Forget(tabletId);
            }
        }

        void RecalcAll() {
            if (TabletExecutorCounters.IsInitialized) {
                TabletExecutorCounters.RecalcAll();
            }
            if (TabletAppCounters.IsInitialized) {
                TabletAppCounters.RecalcAll();
            }
        }

        // db counters

        bool IsInitialized() const {
            return TabletExecutorCounters.IsInitialized;
        }

        void Initialize(const TTabletCountersBase* executorCounters, const TTabletCountersBase* appCounters) {
            Y_ABORT_UNLESS(executorCounters);

            if (!TabletExecutorCounters.IsInitialized) {
                TabletExecutorCounters.Initialize(executorCounters);
            }

            if (appCounters && !TabletAppCounters.IsInitialized) {
                TabletAppCounters.Initialize(appCounters);
            }
        }

        void ToProto(NKikimrSysView::TDbTabletCounters& tabletCounters) {
            if (TabletExecutorCounters.IsInitialized) {
                TabletExecutorCounters.RecalcAll();
                TabletExecutorCounters.ToProto(*tabletCounters.MutableExecutorCounters(),
                    *tabletCounters.MutableMaxExecutorCounters());
            }
            if (TabletAppCounters.IsInitialized) {
                TabletAppCounters.RecalcAll();
                TabletAppCounters.ToProto(*tabletCounters.MutableAppCounters(),
                    *tabletCounters.MutableMaxAppCounters());
            }
        }

        void FromProto(NKikimrSysView::TDbTabletCounters& tabletCounters) {
            if (TabletExecutorCounters.IsInitialized) {
                TabletExecutorCounters.FromProto(*tabletCounters.MutableExecutorCounters(),
                    *tabletCounters.MutableMaxExecutorCounters());
            }
            if (TabletAppCounters.IsInitialized) {
                TabletAppCounters.FromProto(*tabletCounters.MutableAppCounters(),
                    *tabletCounters.MutableMaxAppCounters());
            }
        }

    private:
        //
        class TSolomonCounters {
        public:
            //
            bool IsInitialized;

            TSolomonCounters(::NMonitoring::TDynamicCounterPtr counterGroup)
                : IsInitialized(false)
                , AggregatedSimpleCounters(counterGroup)
                , AggregatedCumulativeCounters(counterGroup)
                , AggregatedHistogramCounters(counterGroup)
                , CounterGroup(counterGroup)
            {}

            void Initialize(const TTabletCountersBase* counters) {
                Y_ABORT_UNLESS(!IsInitialized);

                if (counters) {
                    THashMap<TString, THolder<NPrivate::THistogramCounter>> histogramAggregates;

                    // percentile counters
                    FullSizePercentile = counters->Percentile().Size();
                    AggregatedHistogramCounters.Reserve(FullSizePercentile);
                    for (ui32 i = 0; i < FullSizePercentile; ++i) {
                        if (!counters->PercentileCounterName(i)) {
                            DeprecatedPercentile.insert(i);
                            continue;
                        }

                        auto& percentileCounter = counters->Percentile()[i];
                        const char* percentileCounterName = counters->PercentileCounterName(i);
                        AggregatedHistogramCounters.AddCounter(
                            percentileCounterName,
                            percentileCounter,
                            histogramAggregates);
                    }

                    // simple counters
                    FullSizeSimple = counters->Simple().Size();
                    AggregatedSimpleCounters.Reserve(FullSizeSimple);
                    for (ui32 i = 0; i < FullSizeSimple; ++i) {
                        const char* name = counters->SimpleCounterName(i);
                        if (!name) {
                            DeprecatedSimple.insert(i);
                            continue;
                        }
                        auto itHistogramAggregate = histogramAggregates.find(name);
                        if (itHistogramAggregate != histogramAggregates.end()) {
                            AggregatedSimpleCounters.AddSimpleCounter(name, std::move(itHistogramAggregate->second));
                        } else {
                            AggregatedSimpleCounters.AddSimpleCounter(name);
                        }
                    }

                    // cumulative counters
                    FullSizeCumulative = counters->Cumulative().Size();
                    AggregatedCumulativeCounters.Reserve(FullSizeCumulative);
                    for (ui32 i = 0; i < FullSizeCumulative; ++i) {
                        const char* name = counters->CumulativeCounterName(i);
                        if (!name) {
                            DeprecatedCumulative.insert(i);
                            continue;
                        }
                        auto itHistogramAggregate = histogramAggregates.find(name);
                        if (itHistogramAggregate != histogramAggregates.end()) {
                            AggregatedCumulativeCounters.AddCumulativeCounter(name, std::move(itHistogramAggregate->second));
                        } else {
                            AggregatedCumulativeCounters.AddCumulativeCounter(name);
                        }
                        auto counter = CounterGroup->GetCounter(name, true);
                        CumulativeCounters.push_back(counter);
                    }
                }

                //
                IsInitialized = true;
            }

            void Apply(ui64 tabletId, const TTabletCountersBase* counters, TTabletTypes::EType tabletType) {
                Y_ABORT_UNLESS(counters);

                TInstant now = TInstant::Now();
                auto it = LastAggregateUpdateTime.find(tabletId);
                TDuration diff;
                if (it != LastAggregateUpdateTime.end()) {
                    diff = now - it->second;
                    it->second = now;
                } else {
                    LastAggregateUpdateTime.emplace(tabletId, now);
                }

                // simple counters
                ui32 nextSimpleOffset = 0;
                TVector<ui64> simpleValues;
                simpleValues.resize(FullSizeSimple); // more than needed
                for (ui32 i = 0; i < FullSizeSimple; ++i) {
                    if (!counters->SimpleCounterName(i)) {
                        continue;
                    }
                    const ui32 offset = nextSimpleOffset++;
                    const ui64 value = counters->Simple()[i].Get();
                    simpleValues[offset] = value;
                }
                AggregatedSimpleCounters.SetValues(tabletId, simpleValues, tabletType);

                // cumulative counters
                ui32 nextCumulativeOffset = 0;
                TVector<ui64> cumulativeValues;
                cumulativeValues.resize(FullSizeCumulative, 0);
                for (ui32 i = 0; i < FullSizeCumulative; ++i) {
                    if (!counters->CumulativeCounterName(i)) {
                        continue;
                    }
                    const ui32 offset = nextCumulativeOffset++;
                    const ui64 valueDiff = counters->Cumulative()[i].Get();
                    if (diff) {
                        cumulativeValues[offset] = valueDiff * 1000000 / diff.MicroSeconds(); // differentiate value to per second rate
                    }
                    Y_ABORT_UNLESS(offset < CumulativeCounters.size(), "inconsistent counters for tablet type %s", TTabletTypes::TypeToStr(tabletType));
                    *CumulativeCounters[offset] += valueDiff;
                }
                AggregatedCumulativeCounters.SetValues(tabletId, cumulativeValues, tabletType);

                // percentile counters
                ui32 nextPercentileOffset = 0;
                for (ui32 i = 0; i < FullSizePercentile; ++i) {
                    if (!counters->PercentileCounterName(i)) {
                        continue;
                    }

                    const ui32 offset = nextPercentileOffset++;
                    AggregatedHistogramCounters.SetValue(
                        tabletId,
                        offset,
                        counters->Percentile()[i],
                        counters->PercentileCounterName(i),
                        tabletType);
                }

            }

            void Forget(ui64 tabletId) {
                Y_ABORT_UNLESS(IsInitialized);

                AggregatedSimpleCounters.ForgetTablet(tabletId);
                AggregatedCumulativeCounters.ForgetTablet(tabletId);
                AggregatedHistogramCounters.ForgetTablet(tabletId);
                LastAggregateUpdateTime.erase(tabletId);
            }

            void RecalcAll() {
                AggregatedSimpleCounters.RecalcAll();
                AggregatedCumulativeCounters.RecalcAll();
            }

            template <bool IsSaving>
            void Convert(NKikimrSysView::TDbCounters& sumCounters,
                NKikimrSysView::TDbCounters& maxCounters)
            {
                // simple counters
                auto* simpleSum = sumCounters.MutableSimple();
                auto* simpleMax = maxCounters.MutableSimple();
                simpleSum->Resize(FullSizeSimple, 0);
                simpleMax->Resize(FullSizeSimple, 0);
                ui32 nextSimpleOffset = 0;
                for (ui32 i = 0; i < FullSizeSimple; ++i) {
                    if (DeprecatedSimple.find(i) != DeprecatedSimple.end()) {
                        if constexpr (IsSaving) {
                            (*simpleSum)[i] = 0;
                            (*simpleMax)[i] = 0;
                        }
                        continue;
                    }
                    const ui32 offset = nextSimpleOffset++;
                    if constexpr (IsSaving) {
                        (*simpleSum)[i] = AggregatedSimpleCounters.GetSum(offset);
                        (*simpleMax)[i] = AggregatedSimpleCounters.GetMax(offset);
                    } else {
                        AggregatedSimpleCounters.SetSum(offset, (*simpleSum)[i]);
                        AggregatedSimpleCounters.SetMax(offset, (*simpleMax)[i]);
                    }
                }
                // cumulative counters
                auto* cumulativeSum = sumCounters.MutableCumulative();
                auto* cumulativeMax = maxCounters.MutableCumulative();
                cumulativeSum->Resize(FullSizeCumulative, 0);
                cumulativeMax->Resize(FullSizeCumulative, 0);
                ui32 nextCumulativeOffset = 0;
                for (ui32 i = 0; i < FullSizeCumulative; ++i) {
                    if (DeprecatedCumulative.find(i) != DeprecatedCumulative.end()) {
                        if constexpr (IsSaving) {
                            (*cumulativeSum)[i] = 0;
                            (*cumulativeMax)[i] = 0;
                        }
                        continue;
                    }
                    const ui32 offset = nextCumulativeOffset++;
                    Y_ABORT_UNLESS(offset < CumulativeCounters.size(),
                        "inconsistent cumulative counters %u >= %lu", offset, CumulativeCounters.size());
                    if constexpr (IsSaving) {
                        (*cumulativeSum)[i] = *CumulativeCounters[offset];
                        (*cumulativeMax)[i] = AggregatedCumulativeCounters.GetMax(offset);
                    } else {
                        *CumulativeCounters[offset] = (*cumulativeSum)[i];
                        AggregatedCumulativeCounters.SetMax(offset, (*cumulativeMax)[i]);
                    }
                }
                // percentile counters
                auto* histogramSum = sumCounters.MutableHistogram();
                if (sumCounters.HistogramSize() < FullSizePercentile) {
                    auto missing = FullSizePercentile - sumCounters.HistogramSize();
                    for (; missing > 0; --missing) {
                        sumCounters.AddHistogram();
                    }
                }
                ui32 nextPercentileOffset = 0;
                for (ui32 i = 0; i < FullSizePercentile; ++i) {
                    if (DeprecatedPercentile.find(i) != DeprecatedPercentile.end()) {
                        continue;
                    }
                    auto* buckets = (*histogramSum)[i].MutableBuckets();
                    const ui32 offset = nextPercentileOffset++;
                    auto histogram = AggregatedHistogramCounters.GetHistogram(offset);
                    auto snapshot = histogram->Snapshot();
                    auto count = snapshot->Count();
                    buckets->Resize(count, 0);
                    if constexpr (!IsSaving) {
                        histogram->Reset();
                    }
                    for (ui32 r = 0; r < count; ++r) {
                        if constexpr (IsSaving) {
                            (*buckets)[r] = snapshot->Value(r);
                        } else {
                            histogram->Collect(snapshot->UpperBound(r), (*buckets)[r]);
                        }
                    }
                }
            }

            void ToProto(NKikimrSysView::TDbCounters& sumCounters,
                NKikimrSysView::TDbCounters& maxCounters)
            {
                Convert<true>(sumCounters, maxCounters);
            }

            void FromProto(NKikimrSysView::TDbCounters& sumCounters,
                NKikimrSysView::TDbCounters& maxCounters)
            {
                Convert<false>(sumCounters, maxCounters);
            }

        private:
            ui32 FullSizeSimple = 0;
            THashSet<ui32> DeprecatedSimple;
            ui32 FullSizeCumulative = 0;
            THashSet<ui32> DeprecatedCumulative;
            ui32 FullSizePercentile = 0;
            THashSet<ui32> DeprecatedPercentile;
            //
            NPrivate::TAggregatedSimpleCounters AggregatedSimpleCounters;
            NPrivate::TCountersVector CumulativeCounters;
            NPrivate::TAggregatedCumulativeCounters AggregatedCumulativeCounters;
            NPrivate::TAggregatedHistogramCounters AggregatedHistogramCounters;

            THashMap<ui64, TInstant> LastAggregateUpdateTime;

            ::NMonitoring::TDynamicCounterPtr CounterGroup;
        };

        //
        ::NMonitoring::TDynamicCounterPtr TabletCountersSection;

        ::NMonitoring::TDynamicCounterPtr TabletExecutorCountersSection;
        ::NMonitoring::TDynamicCounterPtr TabletAppCountersSection;

        TSolomonCounters TabletExecutorCounters;
        TSolomonCounters TabletAppCounters;
    };

    using TTabletCountersForTabletTypePtr = TIntrusivePtr<TTabletCountersForTabletType>;
    typedef TMap<TTabletTypes::EType, TTabletCountersForTabletTypePtr> TCountersByTabletType;

    static TTabletCountersForTabletTypePtr FindCountersByTabletType(
        TTabletTypes::EType tabletType,
        TCountersByTabletType& countersByTabletType)
    {
        auto iterTabletType = countersByTabletType.find(tabletType);
        if (iterTabletType != countersByTabletType.end()) {
            return iterTabletType->second;
        }
        return {};
    }

    static TTabletCountersForTabletTypePtr GetOrAddCountersByTabletType(
        TTabletTypes::EType tabletType,
        TCountersByTabletType& countersByTabletType,
        ::NMonitoring::TDynamicCounterPtr counters)
    {
        auto typeCounters = FindCountersByTabletType(tabletType, countersByTabletType);
        if (!typeCounters) {
            TString tabletTypeStr = TTabletTypes::TypeToStr(tabletType);
            typeCounters = new TTabletCountersForTabletType(
                counters.Get(), "type", tabletTypeStr.data());
            countersByTabletType.emplace(tabletType, typeCounters);
        }
        return typeCounters;
    }

    const TTabletCountersBase* GetOrAddLimitedAppCounters(TTabletTypes::EType tabletType) {
        if (auto it = LimitedAppCounters.find(tabletType); it != LimitedAppCounters.end()) {
            return it->second.Get();
        }
        auto appCounters = CreateAppCountersByTabletType(tabletType);
        return LimitedAppCounters.emplace(tabletType, std::move(appCounters)).first->second.Get();
    }

    class TYdbTabletCounters : public TThrRefBase {
        using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
        using THistogramPtr = NMonitoring::THistogramPtr;

    private:
        TCounterPtr WriteRowCount;
        TCounterPtr WriteBytes;
        TCounterPtr ReadRowCount;
        TCounterPtr ReadBytes;
        TCounterPtr EraseRowCount;
        TCounterPtr EraseBytes;
        TCounterPtr BulkUpsertRowCount;
        TCounterPtr BulkUpsertBytes;
        TCounterPtr ScanRowCount;
        TCounterPtr ScanBytes;
        TCounterPtr DatashardRowCount;
        TCounterPtr DatashardSizeBytes;
        TCounterPtr DatashardCacheHitBytes;
        TCounterPtr DatashardCacheMissBytes;
        TCounterPtr ColumnShardReadRows_;
        TCounterPtr ColumnShardReadBytes_;
        TCounterPtr ColumnShardScanRows_;
        TCounterPtr ColumnShardScanBytes_;
        TCounterPtr ColumnShardWriteRows_;
        TCounterPtr ColumnShardBulkUpsertRows_;
        TCounterPtr ColumnShardBulkUpsertBytes_;
        TCounterPtr ColumnShardEraseRows_;
        TCounterPtr ColumnShardEraseBytes_;
        TCounterPtr ResourcesStorageUsedBytes;
        TCounterPtr ResourcesStorageUsedBytesOnSsd;
        TCounterPtr ResourcesStorageUsedBytesOnHdd;
        TCounterPtr ResourcesStorageLimitBytes;
        TCounterPtr ResourcesStorageLimitBytesOnSsd;
        TCounterPtr ResourcesStorageLimitBytesOnHdd;
        TCounterPtr ResourcesStorageTableUsedBytes;
        TCounterPtr ResourcesStorageTableUsedBytesOnSsd;
        TCounterPtr ResourcesStorageTableUsedBytesOnHdd;
        TCounterPtr ResourcesStorageTopicUsedBytes;
        TCounterPtr ResourcesStreamUsedShards;
        TCounterPtr ResourcesStreamLimitShards;
        //TCounterPtr ResourcesStreamUsedShardsPercents;
        TCounterPtr ResourcesStreamReservedThroughput;
        TCounterPtr ResourcesStreamReservedStorage;
        TCounterPtr ResourcesStreamReservedStorageLimit;

        THistogramPtr ShardCpuUtilization;
        THistogramPtr ColumnShardCpuUtilization;

        TCounterPtr RowUpdates;
        TCounterPtr RowUpdateBytes;
        TCounterPtr RowReads;
        TCounterPtr RangeReadRows;
        TCounterPtr RowReadBytes;
        TCounterPtr RangeReadBytes;
        TCounterPtr RowErases;
        TCounterPtr RowEraseBytes;
        TCounterPtr UploadRows;
        TCounterPtr UploadRowsBytes;
        TCounterPtr ScannedRows;
        TCounterPtr ScannedBytes;
        TCounterPtr DbUniqueRowsTotal;
        TCounterPtr DbUniqueDataBytes;
        THistogramPtr ConsumedCpuHistogram;
        TCounterPtr TxCachedBytes;
        TCounterPtr TxReadBytes;

        TCounterPtr ColumnShardScannedBytes_;
        TCounterPtr ColumnShardScannedRows_;
        TCounterPtr ColumnShardUpsertBlobsWritten_;
        TCounterPtr ColumnShardUpsertBytesWritten_;
        TCounterPtr ColumnShardErasedBytes_;
        TCounterPtr ColumnShardErasedRows_;
        THistogramPtr ColumnShardConsumedCpuHistogram;

        TCounterPtr DiskSpaceTablesTotalBytes;
        TCounterPtr DiskSpaceTablesTotalBytesOnSsd;
        TCounterPtr DiskSpaceTablesTotalBytesOnHdd;
        TCounterPtr DiskSpaceTopicsTotalBytes;
        TCounterPtr DiskSpaceSoftQuotaBytes;
        TCounterPtr DiskSpaceSoftQuotaBytesOnSsd;
        TCounterPtr DiskSpaceSoftQuotaBytesOnHdd;

        TCounterPtr StreamShardsCount;
        TCounterPtr StreamShardsQuota;
        TCounterPtr StreamReservedThroughput;
        TCounterPtr StreamReservedStorage;
        TCounterPtr StreamReservedStorageLimit;


    public:
        explicit TYdbTabletCounters(const ::NMonitoring::TDynamicCounterPtr& ydbGroup) {
            WriteRowCount = ydbGroup->GetNamedCounter("name",
                "table.datashard.write.rows", true);
            WriteBytes = ydbGroup->GetNamedCounter("name",
                "table.datashard.write.bytes", true);
            ReadRowCount = ydbGroup->GetNamedCounter("name",
                "table.datashard.read.rows", true);
            ReadBytes = ydbGroup->GetNamedCounter("name",
                "table.datashard.read.bytes", true);
            EraseRowCount = ydbGroup->GetNamedCounter("name",
                "table.datashard.erase.rows", true);
            EraseBytes = ydbGroup->GetNamedCounter("name",
                "table.datashard.erase.bytes", true);
            BulkUpsertRowCount = ydbGroup->GetNamedCounter("name",
                "table.datashard.bulk_upsert.rows", true);
            BulkUpsertBytes = ydbGroup->GetNamedCounter("name",
                "table.datashard.bulk_upsert.bytes", true);
            ScanRowCount = ydbGroup->GetNamedCounter("name",
                "table.datashard.scan.rows", true);
            ScanBytes = ydbGroup->GetNamedCounter("name",
                "table.datashard.scan.bytes", true);

            DatashardRowCount = ydbGroup->GetNamedCounter("name",
                "table.datashard.row_count", false);
            DatashardSizeBytes = ydbGroup->GetNamedCounter("name",
                "table.datashard.size_bytes", false);

            DatashardCacheHitBytes = ydbGroup->GetNamedCounter("name",
                "table.datashard.cache_hit.bytes", true);
            DatashardCacheMissBytes = ydbGroup->GetNamedCounter("name",
                "table.datashard.cache_miss.bytes", true);

            ColumnShardReadRows_ = ydbGroup->GetNamedCounter("name",
                "table.columnshard.read.rows", true);   // TODO: set to 0
            ColumnShardReadBytes_ = ydbGroup->GetNamedCounter("name",
                "table.columnshard.read.bytes", true);  // TODO: set to 0
            ColumnShardScanBytes_ = ydbGroup->GetNamedCounter("name",
                "table.columnshard.scan.bytes", true);
            ColumnShardScanRows_ = ydbGroup->GetNamedCounter("name",
                "table.columnshard.scan.rows", true);
            ColumnShardWriteRows_ = ydbGroup->GetNamedCounter("name",
                "table.columnshard.write.rows", true);
            ColumnShardBulkUpsertRows_ = ydbGroup->GetNamedCounter("name",
                "table.columnshard.bulk_upsert.rows", true);
            ColumnShardBulkUpsertBytes_ = ydbGroup->GetNamedCounter("name",
                "table.columnshard.bulk_upsert.bytes", true);
            ColumnShardEraseRows_ = ydbGroup->GetNamedCounter("name",
                "table.columnshard.erase.rows", true);
            ColumnShardEraseBytes_ = ydbGroup->GetNamedCounter("name",
                "table.columnshard.erase.bytes", true);

            ResourcesStorageUsedBytes = ydbGroup->GetNamedCounter("name",
                "resources.storage.used_bytes", false);
            ResourcesStorageUsedBytesOnSsd = ydbGroup->GetNamedCounter("name",
                "resources.storage.used_bytes.ssd", false);
            ResourcesStorageUsedBytesOnHdd = ydbGroup->GetNamedCounter("name",
                "resources.storage.used_bytes.hdd", false);
            ResourcesStorageLimitBytes = ydbGroup->GetNamedCounter("name",
                "resources.storage.limit_bytes", false);
            ResourcesStorageLimitBytesOnSsd = ydbGroup->GetNamedCounter("name",
                "resources.storage.limit_bytes.ssd", false);
            ResourcesStorageLimitBytesOnHdd = ydbGroup->GetNamedCounter("name",
                "resources.storage.limit_bytes.hdd", false);
            ResourcesStorageTableUsedBytes = ydbGroup->GetNamedCounter("name",
                "resources.storage.table.used_bytes", false);
            ResourcesStorageTableUsedBytesOnSsd = ydbGroup->GetNamedCounter("name",
                "resources.storage.table.used_bytes.ssd", false);
            ResourcesStorageTableUsedBytesOnHdd = ydbGroup->GetNamedCounter("name",
                "resources.storage.table.used_bytes.hdd", false);
            ResourcesStorageTopicUsedBytes = ydbGroup->GetNamedCounter("name",
                "resources.storage.topic.used_bytes", false);

            ResourcesStreamUsedShards = ydbGroup->GetNamedCounter("name",
                "resources.stream.used_shards", false);
            ResourcesStreamLimitShards = ydbGroup->GetNamedCounter("name",
                "resources.stream.limit_shards", false);

            //ResourcesStreamUsedShardsPercents = ydbGroup->GetNamedCounter("name",
            //    "resources.stream.used_shards_percents", false);

            ResourcesStreamReservedThroughput = ydbGroup->GetNamedCounter("name",
                "resources.stream.throughput.limit_bytes_per_second", false);

            ResourcesStreamReservedStorage = ydbGroup->GetNamedCounter("name",
                "resources.stream.storage.reserved_bytes", false);

            ResourcesStreamReservedStorageLimit = ydbGroup->GetNamedCounter("name",
                "resources.stream.storage.limit_bytes", false);

            ShardCpuUtilization = ydbGroup->GetNamedHistogram("name",
                "table.datashard.used_core_percents", NMonitoring::LinearHistogram(12, 0, 10), false);
            ColumnShardCpuUtilization = ydbGroup->GetNamedHistogram("name",
                "table.columnshard.used_core_percents", NMonitoring::LinearHistogram(12, 0, 10), false);
        };

        void Initialize(::NMonitoring::TDynamicCounterPtr counters, bool hasDatashard, bool hasSchemeshard, bool hasColumnShard) {
            if (hasDatashard && !RowUpdates) {
                auto datashardGroup = counters->GetSubgroup("type", "DataShard");
                auto appGroup = datashardGroup->GetSubgroup("category", "app");

                RowUpdates = appGroup->GetCounter("DataShard/EngineHostRowUpdates");
                RowUpdateBytes = appGroup->GetCounter("DataShard/EngineHostRowUpdateBytes");
                RowReads = appGroup->GetCounter("DataShard/EngineHostRowReads");
                RangeReadRows = appGroup->GetCounter("DataShard/EngineHostRangeReadRows");
                RowReadBytes = appGroup->GetCounter("DataShard/EngineHostRowReadBytes");
                RangeReadBytes = appGroup->GetCounter("DataShard/EngineHostRangeReadBytes");
                RowErases = appGroup->GetCounter("DataShard/EngineHostRowErases");
                RowEraseBytes = appGroup->GetCounter("DataShard/EngineHostRowEraseBytes");
                UploadRows = appGroup->GetCounter("DataShard/UploadRows");
                UploadRowsBytes = appGroup->GetCounter("DataShard/UploadRowsBytes");
                ScannedRows = appGroup->GetCounter("DataShard/ScannedRows");
                ScannedBytes = appGroup->GetCounter("DataShard/ScannedBytes");

                auto execGroup = datashardGroup->GetSubgroup("category", "executor");

                DbUniqueRowsTotal = execGroup->GetCounter("SUM(DbUniqueRowsTotal)");
                DbUniqueDataBytes = execGroup->GetCounter("SUM(DbUniqueDataBytes)");
                ConsumedCpuHistogram = execGroup->FindHistogram("HIST(ConsumedCPU)");
                TxCachedBytes = execGroup->GetCounter("TxCachedBytes");
                TxReadBytes = execGroup->GetCounter("TxReadBytes");
            }

            if (hasColumnShard && !ColumnShardScannedBytes_) {
                auto columnshardGroup = counters->GetSubgroup("type", "ColumnShard");
                auto appGroup = columnshardGroup->GetSubgroup("category", "app");

                ColumnShardScannedBytes_ = appGroup->GetCounter("ColumnShard/ScannedBytes");
                ColumnShardScannedRows_ = appGroup->GetCounter("ColumnShard/ScannedRows");
                ColumnShardUpsertBlobsWritten_ = appGroup->GetCounter("ColumnShard/UpsertBlobsWritten");
                ColumnShardUpsertBytesWritten_ = appGroup->GetCounter("ColumnShard/UpsertBytesWritten");
                ColumnShardErasedBytes_ = appGroup->GetCounter("ColumnShard/BytesErased");
                ColumnShardErasedRows_ = appGroup->GetCounter("ColumnShard/RowsErased");
                ColumnShardConsumedCpuHistogram = appGroup->FindHistogram("HIST(ConsumedCPU)");
            }

            if (hasSchemeshard && !DiskSpaceTablesTotalBytes) {
                auto schemeshardGroup = counters->GetSubgroup("type", "SchemeShard");
                auto appGroup = schemeshardGroup->GetSubgroup("category", "app");

                DiskSpaceTablesTotalBytes = appGroup->GetCounter("SUM(SchemeShard/DiskSpaceTablesTotalBytes)");
                DiskSpaceTablesTotalBytesOnSsd = appGroup->GetCounter("SUM(SchemeShard/DiskSpaceTablesTotalBytesOnSsd)");
                DiskSpaceTablesTotalBytesOnHdd = appGroup->GetCounter("SUM(SchemeShard/DiskSpaceTablesTotalBytesOnHdd)");
                DiskSpaceTopicsTotalBytes = appGroup->GetCounter("SUM(SchemeShard/DiskSpaceTopicsTotalBytes)");
                DiskSpaceSoftQuotaBytes = appGroup->GetCounter("SUM(SchemeShard/DiskSpaceSoftQuotaBytes)");
                DiskSpaceSoftQuotaBytesOnSsd = appGroup->GetCounter("SUM(SchemeShard/DiskSpaceSoftQuotaBytesOnSsd)");
                DiskSpaceSoftQuotaBytesOnHdd = appGroup->GetCounter("SUM(SchemeShard/DiskSpaceSoftQuotaBytesOnHdd)");

                StreamShardsCount = appGroup->GetCounter("SUM(SchemeShard/StreamShardsCount)");
                StreamShardsQuota = appGroup->GetCounter("SUM(SchemeShard/StreamShardsQuota)");
                StreamReservedThroughput = appGroup->GetCounter("SUM(SchemeShard/StreamReservedThroughput)");
                StreamReservedStorage = appGroup->GetCounter("SUM(SchemeShard/StreamReservedStorage)");
                StreamReservedStorageLimit = appGroup->GetCounter("SUM(SchemeShard/StreamReservedStorageQuota)");
            }
        }

        void Transform() {
            if (RowUpdates) {
                WriteRowCount->Set(RowUpdates->Val());
                WriteBytes->Set(RowUpdateBytes->Val());
                ReadRowCount->Set(RowReads->Val() + RangeReadRows->Val());
                ReadBytes->Set(RowReadBytes->Val() + RangeReadBytes->Val());
                EraseRowCount->Set(RowErases->Val());
                EraseBytes->Set(RowEraseBytes->Val());
                BulkUpsertRowCount->Set(UploadRows->Val());
                BulkUpsertBytes->Set(UploadRowsBytes->Val());
                ScanRowCount->Set(ScannedRows->Val());
                ScanBytes->Set(ScannedBytes->Val());
                DatashardRowCount->Set(DbUniqueRowsTotal->Val());
                DatashardSizeBytes->Set(DbUniqueDataBytes->Val());
                DatashardCacheHitBytes->Set(TxCachedBytes->Val());
                DatashardCacheMissBytes->Set(TxReadBytes->Val());

                if (ConsumedCpuHistogram) {
                    TransferBuckets(ShardCpuUtilization, ConsumedCpuHistogram);
                }
            }

            if (ColumnShardScannedBytes_) {
                ColumnShardReadRows_->Set(ColumnShardScannedRows_->Val());
                ColumnShardReadBytes_->Set(ColumnShardScannedBytes_->Val());
                ColumnShardScanRows_->Set(ColumnShardScannedRows_->Val());
                ColumnShardScanBytes_->Set(ColumnShardScannedBytes_->Val());
                ColumnShardWriteRows_->Set(ColumnShardUpsertBlobsWritten_->Val());
                ColumnShardBulkUpsertRows_->Set(ColumnShardUpsertBlobsWritten_->Val());
                ColumnShardBulkUpsertBytes_->Set(ColumnShardUpsertBytesWritten_->Val());
                ColumnShardEraseRows_->Set(ColumnShardErasedRows_->Val());
                ColumnShardEraseBytes_->Set(ColumnShardErasedBytes_->Val());

                if (ColumnShardConsumedCpuHistogram) {
                    TransferBuckets(ColumnShardCpuUtilization, ColumnShardConsumedCpuHistogram);
                }
            }

            if (DiskSpaceTablesTotalBytes) {
                ResourcesStorageLimitBytes->Set(DiskSpaceSoftQuotaBytes->Val());
                ResourcesStorageLimitBytesOnSsd->Set(DiskSpaceSoftQuotaBytesOnSsd->Val());
                ResourcesStorageLimitBytesOnHdd->Set(DiskSpaceSoftQuotaBytesOnHdd->Val());
                ResourcesStorageTableUsedBytes->Set(DiskSpaceTablesTotalBytes->Val());
                ResourcesStorageTableUsedBytesOnSsd->Set(DiskSpaceTablesTotalBytesOnSsd->Val());
                ResourcesStorageTableUsedBytesOnHdd->Set(DiskSpaceTablesTotalBytesOnHdd->Val());
                ResourcesStorageTopicUsedBytes->Set(DiskSpaceTopicsTotalBytes->Val());

                if (AppData()->FeatureFlags.GetEnableTopicDiskSubDomainQuota()) {
                    ResourcesStorageUsedBytes->Set(ResourcesStorageTableUsedBytes->Val() + ResourcesStorageTopicUsedBytes->Val());
                    ResourcesStorageUsedBytesOnSsd->Set(ResourcesStorageTableUsedBytesOnSsd->Val());
                    ResourcesStorageUsedBytesOnHdd->Set(ResourcesStorageTableUsedBytesOnHdd->Val());
                } else {
                    ResourcesStorageUsedBytes->Set(ResourcesStorageTableUsedBytes->Val());
                    ResourcesStorageUsedBytesOnSsd->Set(ResourcesStorageTableUsedBytesOnSsd->Val());
                    ResourcesStorageUsedBytesOnHdd->Set(ResourcesStorageTableUsedBytesOnHdd->Val());
                }

                auto quota = StreamShardsQuota->Val();
                ResourcesStreamUsedShards->Set(StreamShardsCount->Val());
                ResourcesStreamLimitShards->Set(quota);
                /*if (quota > 0) {
                    ResourcesStreamUsedShardsPercents->Set(StreamShardsCount->Val() * 100.0 / (quota + 0.0));
                } else {
                    ResourcesStreamUsedShardsPercents->Set(0.0);
                }*/

                ResourcesStreamReservedThroughput->Set(StreamReservedThroughput->Val());
                ResourcesStreamReservedStorage->Set(StreamReservedStorage->Val());
                ResourcesStreamReservedStorageLimit->Set(StreamReservedStorageLimit->Val());
            }
        }

        void TransferBuckets(THistogramPtr dst, THistogramPtr src) {
            auto srcSnapshot = src->Snapshot();
            auto srcCount = srcSnapshot->Count();
            auto dstSnapshot = dst->Snapshot();
            auto dstCount = dstSnapshot->Count();

            dst->Reset();
            for (ui32 b = 0; b < std::min(srcCount, dstCount); ++b) {
                dst->Collect(dstSnapshot->UpperBound(b), srcSnapshot->Value(b));
            }
        }
    };

    using TYdbTabletCountersPtr = TIntrusivePtr<TYdbTabletCounters>;

public:
    class TTabletCountersForDb : public NSysView::IDbCounters {
    public:
        TTabletCountersForDb()
            : SolomonCounters(new ::NMonitoring::TDynamicCounters)
        {}

        TTabletCountersForDb(::NMonitoring::TDynamicCounterPtr externalGroup,
            ::NMonitoring::TDynamicCounterPtr internalGroup,
            THolder<TTabletCountersBase> executorCounters)
            : SolomonCounters(internalGroup)
            , ExecutorCounters(std::move(executorCounters))
        {
            YdbCounters = MakeIntrusive<TYdbTabletCounters>(externalGroup);
        }

        void ToProto(NKikimr::NSysView::TDbServiceCounters& counters) override {
            for (auto& bucket : CountersByTabletType.Buckets) {
                TWriteGuard guard(bucket.GetLock());
                for (auto& [type, tabletCounters] : bucket.GetMap()) {
                    auto* proto = counters.FindOrAddTabletCounters(type);
                    tabletCounters->ToProto(*proto);
                }
            }
        }

        void FromProto(NKikimr::NSysView::TDbServiceCounters& counters) override {
            for (auto& proto : *counters.Proto().MutableTabletCounters()) {
                auto type = proto.GetType();
                auto tabletCounters = GetOrAddCounters(type);
                if (tabletCounters) {
                    if (!tabletCounters->IsInitialized()) {
                        Y_ABORT_UNLESS(ExecutorCounters.Get());
                        auto appCounters = CreateAppCountersByTabletType(type);
                        tabletCounters->Initialize(ExecutorCounters.Get(), appCounters.Get());
                    }
                    tabletCounters->FromProto(proto);
                }
            }
            if (YdbCounters) {
                auto hasDatashard = (bool)GetCounters(TTabletTypes::DataShard);
                auto hasSchemeshard = (bool)GetCounters(TTabletTypes::SchemeShard);
                auto hasColumnshard = static_cast<bool>(GetCounters(TTabletTypes::ColumnShard));
                YdbCounters->Initialize(SolomonCounters, hasDatashard, hasSchemeshard, hasColumnshard);
                YdbCounters->Transform();
            }
        }

        void Apply(ui64 tabletId, const TTabletCountersBase* executorCounters,
            const TTabletCountersBase* appCounters, TTabletTypes::EType type,
            const TTabletCountersBase* limitedAppCounters)
        {
            auto allTypes = GetOrAddCounters(TTabletTypes::Unknown);
            {
                TWriteGuard guard(CountersByTabletType.GetBucketForKey(TTabletTypes::Unknown).GetLock());
                allTypes->Apply(tabletId, executorCounters, nullptr, type);
            }
            auto typeCounters = GetOrAddCounters(type);
            {
                TWriteGuard guard(CountersByTabletType.GetBucketForKey(type).GetLock());
                typeCounters->Apply(tabletId, executorCounters, appCounters, type, limitedAppCounters);
            }
        }

        void Forget(ui64 tabletId, TTabletTypes::EType type) {
            auto allTypes = GetCounters(TTabletTypes::Unknown);
            if (allTypes) {
                TWriteGuard guard(CountersByTabletType.GetBucketForKey(TTabletTypes::Unknown).GetLock());
                allTypes->Forget(tabletId);
            }
            auto typeCounters = GetCounters(type);
            if (typeCounters) {
                TWriteGuard guard(CountersByTabletType.GetBucketForKey(type).GetLock());
                typeCounters->Forget(tabletId);
            }
        }

        void RecalcAll() {
            for (auto& bucket : CountersByTabletType.Buckets) {
                TWriteGuard guard(bucket.GetLock());
                for (auto& [_, tabletCounters] : bucket.GetMap()) {
                    tabletCounters->RecalcAll();
                }
            }
        }

    private:
        TTabletCountersForTabletTypePtr GetCounters(TTabletTypes::EType tabletType) {
            TTabletCountersForTabletTypePtr res;
            CountersByTabletType.Get(tabletType, res);
            return res;
        }

        TTabletCountersForTabletTypePtr GetOrAddCounters(TTabletTypes::EType tabletType) {
            auto res = GetCounters(tabletType);
            if (res) {
                return res;
            }
            res = CountersByTabletType.InsertIfAbsentWithInit(tabletType, [this, tabletType] {
                TString type = (tabletType == TTabletTypes::Unknown) ?
                    "all" : TTabletTypes::TypeToStr(tabletType);
                return MakeIntrusive<TTabletCountersForTabletType>(
                    SolomonCounters.Get(), "type", type.data());
            });
            return res;
        }

    private:
        ::NMonitoring::TDynamicCounterPtr SolomonCounters;
        THolder<TTabletCountersBase> ExecutorCounters;

        TConcurrentRWHashMap<TTabletTypes::EType, TTabletCountersForTabletTypePtr, 16> CountersByTabletType;

        TYdbTabletCountersPtr YdbCounters;
    };

    class TTabletsDbWatcherCallback : public NKikimr::NSysView::TDbWatcherCallback {
        TActorSystem* ActorSystem = {};

    public:
        explicit TTabletsDbWatcherCallback(TActorSystem* actorSystem)
            : ActorSystem(actorSystem)
        {}

        void OnDatabaseRemoved(const TString& dbPath, TPathId pathId) override {
            auto evRemove = MakeHolder<TEvTabletCounters::TEvRemoveDatabase>(dbPath, pathId);
            auto aggregator = MakeTabletCountersAggregatorID(ActorSystem->NodeId, false);
            ActorSystem->Send(aggregator, evRemove.Release());
        }
    };

private:
    TIntrusivePtr<TTabletCountersForDb> GetDbCounters(TPathId pathId, const TActorContext& ctx) {
        auto it = CountersByPathId.find(pathId);
        if (it != CountersByPathId.end()) {
            return it->second;
        }

        auto dbCounters = MakeIntrusive<TTabletMon::TTabletCountersForDb>();
        CountersByPathId[pathId] = dbCounters;

        auto evRegister = MakeHolder<NSysView::TEvSysView::TEvRegisterDbCounters>(
            NKikimrSysView::TABLETS, pathId, dbCounters);
        ctx.Send(NSysView::MakeSysViewServiceID(ctx.SelfID.NodeId()), evRegister.Release());

        if (DbWatcherActorId) {
            auto evWatch = MakeHolder<NSysView::TEvSysView::TEvWatchDatabase>(pathId);
            ctx.Send(DbWatcherActorId, evWatch.Release());
        }

        return dbCounters;
    }

    NPrivate::TDbLabeledCounters::TPtr GetLabeledDbCounters(const TString& dbName, const TActorContext& ctx) {
        auto it = LabeledDbCounters.find(dbName);
        if (it != LabeledDbCounters.end()) {
            return it->second;
        }

        auto dbCounters = MakeIntrusive<NPrivate::TDbLabeledCounters>();
        LabeledDbCounters[dbName] = dbCounters;

        auto evRegister = MakeHolder<NSysView::TEvSysView::TEvRegisterDbCounters>(
            NKikimrSysView::LABELED, dbName, dbCounters);
        ctx.Send(NSysView::MakeSysViewServiceID(ctx.SelfID.NodeId()), evRegister.Release());

        if (DbWatcherActorId) {
            auto evWatch = MakeHolder<NSysView::TEvSysView::TEvWatchDatabase>(dbName);
            ctx.Send(DbWatcherActorId, evWatch.Release());
        }

        return dbCounters;
    }

private:
    ::NMonitoring::TDynamicCounterPtr Counters;
    TTabletCountersForTabletTypePtr AllTypes;
    bool IsFollower = false;

    typedef THashMap<TPathId, TIntrusivePtr<TTabletCountersForDb>> TCountersByPathId;
    typedef TMap<TTabletTypes::EType, THolder<TTabletCountersBase>> TAppCountersByTabletType;
    typedef THashMap<TString, TIntrusivePtr<NPrivate::TDbLabeledCounters>> TLabeledCountersByDbPath;
    typedef TMap<std::pair<TTabletTypes::EType, TString>, TAutoPtr<NPrivate::TAggregatedLabeledCounters>> TLabeledCountersByTabletTypeAndGroup;
    typedef THashMap<ui64, std::pair<TAutoPtr<TTabletCountersBase>, TAutoPtr<TTabletCountersBase>>> TQuietTabletCounters;

    TCountersByTabletType CountersByTabletType;
    TCountersByPathId CountersByPathId;
    TActorId DbWatcherActorId;
    TAppCountersByTabletType LimitedAppCounters; // without txs
    TYdbTabletCountersPtr YdbCounters;
    TLabeledCountersByDbPath LabeledDbCounters;
    TLabeledCountersByTabletTypeAndGroup LabeledCountersByTabletTypeAndGroup;
    TQuietTabletCounters QuietTabletCounters;
};


TIntrusivePtr<NSysView::IDbCounters> CreateTabletDbCounters(
    ::NMonitoring::TDynamicCounterPtr externalGroup,
    ::NMonitoring::TDynamicCounterPtr internalGroup,
    THolder<TTabletCountersBase> executorCounters)
{
    return MakeIntrusive<TTabletMon::TTabletCountersForDb>(
        externalGroup, internalGroup, std::move(executorCounters));
}

////////////////////////////////////////////
/// The TTabletCountersAggregatorActor class
////////////////////////////////////////////
class TTabletCountersAggregatorActor : public TActorBootstrapped<TTabletCountersAggregatorActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_COUNTERS_AGGREGATOR;
    }

    //
    TTabletCountersAggregatorActor(bool follower);
    virtual ~TTabletCountersAggregatorActor();

    //
    void Bootstrap(const TActorContext &ctx);

    //
    STFUNC(StateWork);

private:
    //
    void HandleWork(TEvTabletCounters::TEvTabletAddCounters::TPtr &ev, const TActorContext &ctx);
    void HandleWork(TEvTabletCounters::TEvTabletCountersForgetTablet::TPtr &ev, const TActorContext &ctx);
    void HandleWork(TEvTabletCounters::TEvTabletCountersRequest::TPtr &ev, const TActorContext &ctx);
    void HandleWork(TEvTabletCounters::TEvTabletAddLabeledCounters::TPtr &ev, const TActorContext &ctx);
    void HandleWork(TEvTabletCounters::TEvTabletLabeledCountersRequest::TPtr &ev, const TActorContext &ctx);
    void HandleWork(TEvTabletCounters::TEvTabletLabeledCountersResponse::TPtr &ev, const TActorContext &ctx);//from cluster aggregator
    void HandleWork(NMon::TEvHttpInfo::TPtr& ev, const TActorContext &ctx);
    void HandleWakeup(const TActorContext &ctx);
    void HandleWork(TEvTabletCounters::TEvRemoveDatabase::TPtr& ev);

    //
    TAutoPtr<TTabletMon> TabletMon;
    TActorId DbWatcherActorId;
    THashMap<TActorId, std::pair<TActorId, TAutoPtr<NMon::TEvHttpInfo>>> HttpRequestHandlers;
    THashSet<ui32> TabletTypeOfReceivedLabeledCounters;
    bool Follower;
};

////////////////////////////////////////////
/// The TTabletCountersAggregatorActor class
////////////////////////////////////////////
TTabletCountersAggregatorActor::TTabletCountersAggregatorActor(bool follower)
    : Follower(follower)
{}

////////////////////////////////////////////
TTabletCountersAggregatorActor::~TTabletCountersAggregatorActor()
{}

////////////////////////////////////////////
void
TTabletCountersAggregatorActor::Bootstrap(const TActorContext &ctx) {
    Become(&TThis::StateWork);

    TAppData* appData = AppData(ctx);
    Y_ABORT_UNLESS(!TabletMon);

    if (AppData(ctx)->FeatureFlags.GetEnableDbCounters() && !Follower) {
        auto callback = MakeIntrusive<TTabletMon::TTabletsDbWatcherCallback>(ctx.ActorSystem());
        DbWatcherActorId = ctx.Register(NSysView::CreateDbWatcherActor(callback));
    }

    TabletMon = new TTabletMon(appData->Counters, Follower, DbWatcherActorId);
    auto mon = appData->Mon;
    if (mon) {
        if (!Follower)
            mon->RegisterActorPage(nullptr, "labeledcounters", "Labeled Counters", false, TlsActivationContext->ExecutorThread.ActorSystem, SelfId(), false);
        else
            mon->RegisterActorPage(nullptr, "followercounters", "Follower Counters", false, TlsActivationContext->ExecutorThread.ActorSystem, SelfId(), false);
    }

    ctx.Schedule(TDuration::Seconds(WAKEUP_TIMEOUT_SECONDS), new TEvents::TEvWakeup());
}


////////////////////////////////////////////
void
TTabletCountersAggregatorActor::HandleWork(TEvTabletCounters::TEvTabletAddCounters::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    TEvTabletCounters::TEvTabletAddCounters* msg = ev->Get();
    TabletMon->Apply(msg->TabletID, msg->TabletType, msg->TenantPathId, msg->ExecutorCounters.Get(), msg->AppCounters.Get(), ctx);
}

////////////////////////////////////////////
void
TTabletCountersAggregatorActor::HandleWork(TEvTabletCounters::TEvTabletAddLabeledCounters::TPtr &ev, const TActorContext &ctx) {
    TEvTabletCounters::TEvTabletAddLabeledCounters* msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TABLET_AGGREGATOR,
                "got labeledCounters " << msg->LabeledCounters.Get()->GetDatabasePath() << " " <<
                msg->LabeledCounters.Get()->GetGroup() << " " <<
                (msg->TabletType == TTabletTypes::PersQueue ? "PQ" : "different"));
    if (msg->LabeledCounters.Get()->GetDatabasePath()) {
        if (msg->TabletType == TTabletTypes::PersQueue) {
            LOG_DEBUG_S(ctx, NKikimrServices::TABLET_AGGREGATOR,
                        "got labeledCounters from db: " << msg->LabeledCounters.Get()->GetDatabasePath() <<
                        "; tablet: " << msg->TabletID);
            TabletMon->ApplyLabeledDbCounters(msg->LabeledCounters.Get()->GetDatabasePath().GetRef(), msg->TabletID, msg->LabeledCounters.Get(), ctx);
        } else {
            LOG_ERROR_S(ctx, NKikimrServices::TABLET_AGGREGATOR,
                        "got labeledCounters from unknown Tablet Type: " << msg->TabletType <<
                        "; db: " << msg->LabeledCounters.Get()->GetDatabasePath() <<
                        "; tablet: " << msg->TabletID);
            return;
        }
    } else {
        TabletMon->ApplyLabeledCounters(msg->TabletID, msg->TabletType, msg->LabeledCounters.Get());
    }

    TabletTypeOfReceivedLabeledCounters.insert(msg->TabletType);
}

////////////////////////////////////////////
void
TTabletCountersAggregatorActor::HandleWork(TEvTabletCounters::TEvTabletCountersForgetTablet::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    TEvTabletCounters::TEvTabletCountersForgetTablet* msg = ev->Get();
    TabletMon->ForgetTablet(msg->TabletID, msg->TabletType, msg->TenantPathId);
}

////////////////////////////////////////////
void
TTabletCountersAggregatorActor::HandleWork(TEvTabletCounters::TEvTabletCountersRequest::TPtr &ev, const TActorContext &ctx) {
    TEvTabletCounters::TEvTabletCountersRequest* msg = ev->Get();
    TAutoPtr<TEvTabletCounters::TEvTabletCountersResponse> resp = new TEvTabletCounters::TEvTabletCountersResponse();
    TabletMon->Query(msg->Record, resp->Record);
    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

////////////////////////////////////////////
void
TTabletCountersAggregatorActor::HandleWork(TEvTabletCounters::TEvTabletLabeledCountersRequest::TPtr &ev, const TActorContext &ctx) {
    TEvTabletCounters::TEvTabletLabeledCountersRequest* msg = ev->Get();
    TAutoPtr<TEvTabletCounters::TEvTabletLabeledCountersResponse> resp = new TEvTabletCounters::TEvTabletLabeledCountersResponse();
    TabletMon->QueryLabeledCounters(msg->Record, resp->Record, ctx);
    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

////////////////////////////////////////////
void
TTabletCountersAggregatorActor::HandleWork(TEvTabletCounters::TEvTabletLabeledCountersResponse::TPtr &ev, const TActorContext &ctx) {
    auto& response = ev->Get()->Record;

    auto it = HttpRequestHandlers.find(ev->Sender);
    if (it == HttpRequestHandlers.end())
        return;
    TString html;
    TStringOutput oss(html);
    ::NMonitoring::TDynamicCounters counters;
    const auto& params = it->second.second->Request.GetParams();
    TString reqTabletType = params.Get("type");

    auto mainGroup = counters.GetSubgroup("user_counters", reqTabletType);

    bool parsePQTopic = params.Has("parse_pq");
    ui32 version = 0;
    if (parsePQTopic) {
        TryFromString(params.Get("parse_pq"), version);
    }

    for (ui32 i = 0; i < response.LabeledCountersByGroupSize(); ++i) {
        const auto ucByGroup = response.GetLabeledCountersByGroup(i);
        TVector<TString> groups;
        TVector<TString> groupNames;
        Y_ABORT_UNLESS(ucByGroup.GetDelimiter() == "/");
        StringSplitter(ucByGroup.GetGroup()).Split('/').SkipEmpty().Collect(&groups);

        if (parsePQTopic) {
            bool skip = false;
            for (ui32 j = 0; j < groups.size(); ++j) {
                if (groups[j] == "total") {
                    skip = true;
                    break;
                }
            }
            if (skip)
                continue;
        }

        StringSplitter(ucByGroup.GetGroupNames()).Split('/').SkipEmpty().Collect(&groupNames);
        Y_ABORT_UNLESS(groups.size() == groupNames.size(), "%s and %s", ucByGroup.GetGroup().c_str(), ucByGroup.GetGroupNames().c_str());
        auto group = mainGroup;
        for (ui32 j = 0; j < groups.size(); ++j) {
            if (parsePQTopic) {
                groupNames[j] = TString(1, toupper(groupNames[j][0])) + groupNames[j].substr(1);
                if (groupNames[j] == "Topic") {
                    if (NPersQueue::CorrectName(groups[j])) {
                        TString dc = to_title(NPersQueue::GetDC(groups[j]));
                        TString producer = NPersQueue::GetProducer(groups[j]);
                        TString topic = NPersQueue::GetRealTopic(groups[j]);
                        group = group->GetSubgroup("OriginDC", dc);
                        if (version > 1) {
                            topic = NPersQueue::GetTopicPath(groups[j]);
                            producer = NPersQueue::GetAccount(groups[j]);
                            group = group->GetSubgroup("Account", producer);
                            group = group->GetSubgroup("TopicPath", topic);
                        } else {
                            group = group->GetSubgroup("Producer", producer);
                            group = group->GetSubgroup("Topic", topic);
                        }
                    } else {
                        if (version > 1) {
                            group = group->GetSubgroup("OriginDC", "unknown");
                            group = group->GetSubgroup("Account", "unknown");
                            group = group->GetSubgroup("TopicPath", groups[j]);
                        } else {
                            group = group->GetSubgroup("OriginDC", "unknown");
                            group = group->GetSubgroup("Producer", "unknown");
                            group = group->GetSubgroup("Topic", groups[j]);
                        }
                    }
                    continue;
                }
                if (groupNames[j] == "Client") {
                    group = group->GetSubgroup("ConsumerPath", NPersQueue::ConvertOldConsumerName(groups[j], ctx));
                    continue;
                }
            }
            group = group->GetSubgroup(groupNames[j], groups[j]);
        }
        for (ui32 j = 0; j < ucByGroup.LabeledCounterSize(); ++j) {
            const auto& uc = ucByGroup.GetLabeledCounter(j);
            bool deriv = (uc.GetType() == TLabeledCounterOptions::CT_DERIV);
            TString counterName = uc.GetName();
            if (parsePQTopic && counterName.StartsWith("PQ/")) {
                counterName = counterName.substr(3);
            }
            *group->GetCounter(counterName, deriv).Get() = uc.GetValue();
        }
    }

    bool json = params.Has("json");
    bool spack = params.Has("spack");
    if (json) {
        oss << NMonitoring::HTTPOKJSON;
        auto encoder = NMonitoring::CreateEncoder(&oss, NMonitoring::EFormat::JSON);
        counters.Accept(TString(), TString(), *encoder);
    } else if (spack) {
        oss.Write(NMonitoring::HTTPOKSPACK);
        auto encoder = NMonitoring::CreateEncoder(&oss, NMonitoring::EFormat::SPACK);
        counters.Accept(TString(), TString(), *encoder);
    } else {
        counters.OutputHtml(oss);
    }
    oss.Flush();
    ctx.Send(it->second.first, new NMon::TEvHttpInfoRes(html, 0, (json || spack) ? NMon::TEvHttpInfoRes::Custom : NMon::TEvHttpInfoRes::Html));
    HttpRequestHandlers.erase(it);
}


////////////////////////////////////////////
void TTabletCountersAggregatorActor::HandleWork(TEvTabletCounters::TEvRemoveDatabase::TPtr& ev) {
    TabletMon->RemoveTabletsByPathId(ev->Get()->PathId);
    TabletMon->RemoveTabletsByDbPath(ev->Get()->DbPath);
}

////////////////////////////////////////////
void
TTabletCountersAggregatorActor::HandleWork(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {

    TString reqTabletType = ev->Get()->Request.GetParams().Get("type");
    ui32 workers = 0;
    TryFromString(ev->Get()->Request.GetParams().Get("workers"), workers);
    for (ui32 tabletType = 0; tabletType < TTabletTypes::UserTypeStart; ++tabletType) {
        if (!NKikimrTabletBase::TTabletTypes::EType_IsValid(tabletType))
            continue;
        TString tabletTypeStr = TTabletTypes::TypeToStr((TTabletTypes::EType)tabletType);
        if (tabletTypeStr == reqTabletType) {
            TActorId handler = CreateClusterLabeledCountersAggregator(ctx.SelfID, (TTabletTypes::EType)tabletType, ctx, 1, "", workers);
            HttpRequestHandlers.insert(std::make_pair(handler, std::make_pair(ev->Sender, ev->Release())));
            return;
        }
    }
    // reaching this point means that this tablet type is unknown, response with nothing
    TString html;
    for (const auto& tabletType: TabletTypeOfReceivedLabeledCounters) {
        TString tabletTypeStr = TTabletTypes::TypeToStr((TTabletTypes::EType)tabletType);
        html += "<a href=\"?type=" + tabletTypeStr + "\">" + tabletTypeStr + " labeled counters</a><br>";
        html += "<a href=\"?type=" + tabletTypeStr + "&json=1\">" + tabletTypeStr + " labeled counters as json</a><br>";
        html += "<a href=\"?type=" + tabletTypeStr + "&spack=1\">" + tabletTypeStr + " labeled counters as spack</a><br>";
    }
    ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(html));
}

////////////////////////////////////////////
void
TTabletCountersAggregatorActor::HandleWakeup(const TActorContext &ctx) {

    TabletMon->RecalcAll();
    ctx.Schedule(TDuration::Seconds(WAKEUP_TIMEOUT_SECONDS), new TEvents::TEvWakeup());
}

////////////////////////////////////////////
/// public state functions
////////////////////////////////////////////
STFUNC(TTabletCountersAggregatorActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTabletCounters::TEvTabletAddCounters, HandleWork);
        HFunc(TEvTabletCounters::TEvTabletCountersForgetTablet, HandleWork);
        HFunc(TEvTabletCounters::TEvTabletCountersRequest, HandleWork);
        HFunc(TEvTabletCounters::TEvTabletAddLabeledCounters, HandleWork);
        HFunc(TEvTabletCounters::TEvTabletLabeledCountersRequest, HandleWork);
        HFunc(TEvTabletCounters::TEvTabletLabeledCountersResponse, HandleWork); //from cluster aggregator, for http requests
        hFunc(TEvTabletCounters::TEvRemoveDatabase, HandleWork);
        HFunc(NMon::TEvHttpInfo, HandleWork);
        CFunc(TEvents::TSystem::Wakeup, HandleWakeup);

        // HFunc(TEvents::TEvPoisonPill, Handle); // we do not need PoisonPill for the actor
    }
}

////////////////////////////////////////////

static ui32 AGGREGATOR_TIMEOUT_SECONDS = 60;

IActor*
CreateTabletCountersAggregator(bool follower) {
    return new TTabletCountersAggregatorActor(follower);
}

void TabletCountersForgetTablet(ui64 tabletId, TTabletTypes::EType tabletType, TPathId tenantPathId, bool follower, TActorIdentity identity) {
    const TActorId countersAggregator = MakeTabletCountersAggregatorID(identity.NodeId(), follower);
    identity.Send(countersAggregator, new TEvTabletCounters::TEvTabletCountersForgetTablet(tabletId, tabletType, tenantPathId));
}

///////////////////////////////////////////

TString ReplaceDelimiter(const TString& groups) {
    TString res;
    for (const auto& c : groups) {
        if (c == '|') {
            res += '/';
        } else {
            res += c;
        }
    }
    return res;
}

void PreProcessResponse(TEvTabletCounters::TEvTabletLabeledCountersResponse* response) {
    auto& record = response->Record;
    for (auto & lc : (*record.MutableLabeledCountersByGroup())) {
        if (lc.GetDelimiter() == "/") continue;
        if (lc.GetDelimiter() == "|") { //convert
            TString group = ReplaceDelimiter(lc.GetGroup());
            lc.SetGroup(group);
            if (lc.HasGroupNames()) {
                TString groupNames = ReplaceDelimiter(lc.GetGroupNames());
                lc.SetGroupNames(groupNames);
            }
        }
        lc.SetDelimiter("/");
    }
}


class TClusterLabeledCountersAggregatorActorV1 : public TActorBootstrapped<TClusterLabeledCountersAggregatorActorV1> {
private:
    using TBase = TActorBootstrapped<TClusterLabeledCountersAggregatorActorV1>;
    TActorId Initiator;
    TTabletTypes::EType TabletType;
    ui32 NodesRequested;
    ui32 NodesReceived;
    TVector<ui32> Nodes;
    THashMap<ui32, TAutoPtr<TEvTabletCounters::TEvTabletLabeledCountersResponse>> PerNodeResponse;
    ui32 NumWorkers;
    ui32 WorkerId;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_COUNTERS_AGGREGATOR;
    }

    //
    TClusterLabeledCountersAggregatorActorV1(const TActorId& parentActor, const TTabletTypes::EType tabletType, ui32 numWorkers = 0, ui32 workerId = 0)
        : Initiator(parentActor)
        , TabletType(tabletType)
        , NodesRequested(0)
        , NodesReceived(0)
        , NumWorkers(numWorkers)
        , WorkerId(workerId) //if NumWorkers is zero, then WorkerId is treat as desired number of workers
    {
    }

    void SendRequest(ui32 nodeId, const TActorContext &ctx) {
        TActorId aggregatorServiceId = MakeTabletCountersAggregatorID(nodeId);
        TAutoPtr<TEvTabletCounters::TEvTabletLabeledCountersRequest> request(new TEvTabletCounters::TEvTabletLabeledCountersRequest());
        request->Record.SetTabletType(TabletType);
        request->Record.SetVersion(1);
        ctx.Send(aggregatorServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        Nodes.emplace_back(nodeId);
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor request to node " << nodeId << " " << ctx.SelfID);
        ++NodesRequested;
    }

    void Die(const TActorContext& ctx) override {
        for (const ui32 node : Nodes) {
            ctx.Send(TActivationContext::InterconnectProxy(node), new TEvents::TEvUnsubscribe());
        }
        TBase::Die(ctx);
    }

    void Bootstrap(const TActorContext& ctx) {
        if (NumWorkers > 0) {
            const TActorId nameserviceId = GetNameserviceActorId();
            ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
            TBase::Become(&TThis::StateRequestedBrowse);
            ctx.Schedule(TDuration::Seconds(AGGREGATOR_TIMEOUT_SECONDS), new TEvents::TEvWakeup());
            LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator new request V1 Initiator " << Initiator << " self " << ctx.SelfID << " worker " << WorkerId);
        } else {
            LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator new request V1 " << ctx.SelfID);
            for (ui32 i = 0; i < WorkerId; ++i) {
                ctx.Register(new TClusterLabeledCountersAggregatorActorV1(ctx.SelfID, TabletType, WorkerId, i));
            }
            NodesRequested = WorkerId;
            TBase::Become(&TThis::StateRequested);
        }
    }

    STFUNC(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, HandleBrowse);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    STFUNC(StateRequested) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletCounters::TEvTabletLabeledCountersResponse, HandleResponse);
            HFunc(TEvents::TEvUndelivered, Undelivered);
            HFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void HandleBrowse(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx) {
        const TEvInterconnect::TEvNodesInfo* nodesInfo = ev->Get();
        Y_ABORT_UNLESS(!nodesInfo->Nodes.empty());
        Nodes.reserve(nodesInfo->Nodes.size());
        ui32 i = 0;
        for (const auto& ni : nodesInfo->Nodes) {
            ++i;
            if (i % NumWorkers == WorkerId) {
                SendRequest(ni.NodeId, ctx);
            }
        }
        if (NodesRequested > 0) {
            TBase::Become(&TThis::StateRequested);
        } else {
            ReplyAndDie(ctx);
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        ui32 nodeId = ev.Get()->Cookie;
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor undelivered node " << nodeId << " " << ctx.SelfID);
        if (PerNodeResponse.emplace(nodeId, nullptr).second) {
            NodeResponseReceived(ctx);
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev, const TActorContext &ctx) {
        ui32 nodeId = ev->Get()->NodeId;
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor disconnected node " << nodeId << " " << ctx.SelfID);
        if (PerNodeResponse.emplace(nodeId, nullptr).second) {
            NodeResponseReceived(ctx);
        }
    }

    void HandleResponse(TEvTabletCounters::TEvTabletLabeledCountersResponse::TPtr &ev, const TActorContext &ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor got response node " << nodeId << " " << ctx.SelfID);
        PreProcessResponse(ev->Get());
        PerNodeResponse[nodeId] = ev->Release();
        NodeResponseReceived(ctx);
    }

    void NodeResponseReceived(const TActorContext &ctx) {
        ++NodesReceived;
        if (NodesReceived >= NodesRequested) {
            ReplyAndDie(ctx);
        }
    }

    void HandleTimeout(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor got TIMEOUT");
        ReplyAndDie(ctx);
    }

    void ReplyAndDie(const TActorContext& ctx) {
        TAutoPtr<TEvTabletCounters::TEvTabletLabeledCountersResponse> response(new TEvTabletCounters::TEvTabletLabeledCountersResponse);

        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator all answers recieved - replying " << ctx.SelfID);

        TVector<ui8> types;
        TVector<ui8> aggrFuncs;
        TVector<const char*> names;
        ui32 metaInfoCount = 0;
        THashMap<TString, TAutoPtr<TTabletLabeledCountersBase>> groupsToLabeledCounter;
        THashMap<TString, std::pair<ui32,ui32>> startPos;
        THashMap<TString, TString> groupToNames;
        for (auto& resp : PerNodeResponse) {
            if (!resp.second)
                continue;
            for (ui32 i = 0; i < resp.second->Record.LabeledCountersByGroupSize(); ++i) {
                const auto& labeledCounterByGroup = resp.second->Record.GetLabeledCountersByGroup(i);
                const TString& group = labeledCounterByGroup.GetGroup();
                if (startPos.find(group) != startPos.end())
                    continue;
                ui32 count = labeledCounterByGroup.LabeledCounterSize();
                for (ui32 j = 0; j < count; ++j) {
                    const auto& labeledCounter = labeledCounterByGroup.GetLabeledCounter(j);
                    names.push_back(labeledCounter.GetName().c_str());
                    aggrFuncs.push_back(labeledCounter.GetAggregateFunc());
                    types.push_back(labeledCounter.GetType());
                }
                startPos[group] = std::make_pair(metaInfoCount, count);
                groupToNames[group] = labeledCounterByGroup.GetGroupNames();
                metaInfoCount += count;
            }
        }
        for (auto& resp : PerNodeResponse) {
            if (!resp.second)
                continue;
            // TODO: labeledCounterByGroup must have as key group + groupToNames[group] -
            // in case of aggregation (changing group to total/total/total)
            // keys may be equal for different metrics types
            for (ui32 i = 0; i < resp.second->Record.LabeledCountersByGroupSize(); ++i) {
                const auto& labeledCounterByGroup = resp.second->Record.GetLabeledCountersByGroup(i);
                const TString& originalGroup = labeledCounterByGroup.GetGroup();
                ui32 count = Min<ui32>(startPos[originalGroup].second, labeledCounterByGroup.LabeledCounterSize());
                ui32 sp = startPos[originalGroup].first;
                TAutoPtr<TTabletLabeledCountersBase> labeledCounters(
                    new TTabletLabeledCountersBase(count, names.begin() + sp, types.begin() + sp,
                                                   aggrFuncs.begin() + sp, originalGroup, nullptr, 0));
                for (ui32 j = 0; j < count; ++j) {
                    const auto& labeledCounter = labeledCounterByGroup.GetLabeledCounter(j);
                    labeledCounters->GetCounters()[j] = labeledCounter.GetValue();
                    labeledCounters->GetIds()[j] = labeledCounter.GetId();
                }

                TVector<TString> aggrGroups;
                TVector<TString> groupParts, groupParts2;
                StringSplitter(originalGroup).Split('/').SkipEmpty().Collect(&groupParts);
                Y_ABORT_UNLESS(groupParts.size() > 0);
                groupParts2 = groupParts;
                ui32 changePos = groupParts.size();
                TString group = originalGroup;
                do { //repors for user/topic  user/total; total;total
                    auto& aggr = groupsToLabeledCounter[group];
                    groupToNames[group] = groupToNames[originalGroup];
                    if (aggr == nullptr) {
                        aggr = new TTabletLabeledCountersBase(*labeledCounters);
                    } else {
                        aggr->AggregateWith(*labeledCounters);
                    }
                    if (changePos > 0) {
                        --changePos;
                        groupParts[changePos] = "total";
                        group = "";
                        for (auto& g: groupParts) {
                            if (!group.empty()) group += '/';
                            group += g;
                        }
                    } else
                        break;
                } while (true);
                for (changePos = 0; changePos + 1 < groupParts.size(); ++changePos) {
                    groupParts2[changePos] = "total";
                    group = "";
                    for (auto& g: groupParts2) {
                        if (!group.empty()) group += '/';
                        group += g;
                    }
                    auto& aggr = groupsToLabeledCounter[group];
                    groupToNames[group] = groupToNames[originalGroup];
                    if (aggr == nullptr) {
                        aggr = new TTabletLabeledCountersBase(*labeledCounters);
                    } else {
                        aggr->AggregateWith(*labeledCounters);
                    }
                }

            }
            response->Record.AddNodes(resp.first);
        }
        ui32 numGroups = 0, numCounters = 0;
        for (auto& g : groupsToLabeledCounter) {
            auto& labeledCounterByGroup = *response->Record.AddLabeledCountersByGroup();
            labeledCounterByGroup.SetGroup(g.first);
            labeledCounterByGroup.SetGroupNames(groupToNames[g.first]);
            labeledCounterByGroup.SetDelimiter("/"); //TODO: change to "|"
            ++numGroups;
            for (ui32 i = 0; i < g.second->GetCounters().Size(); ++i) {
                if (g.second->GetTypes()[i] == TLabeledCounterOptions::CT_TIMELAG && g.second->GetCounters()[i].Get() == Max<i64>()) { //this means no data, do not report it
                    continue;
                }
                ++numCounters;
                auto& labeledCounter = *labeledCounterByGroup.AddLabeledCounter();
                switch(g.second->GetTypes()[i]) {
                    case TLabeledCounterOptions::CT_SIMPLE:
                    case TLabeledCounterOptions::CT_DERIV:
                        labeledCounter.SetValue(g.second->GetCounters()[i].Get());
                        break;
                    case TLabeledCounterOptions::CT_TIMELAG: {
                        ui64 milliSeconds = TAppData::TimeProvider->Now().MilliSeconds();
                        labeledCounter.SetValue(milliSeconds > g.second->GetCounters()[i].Get() ? milliSeconds - g.second->GetCounters()[i].Get() : 0);
                        break;
                        }
                    default:
                        Y_ABORT("unknown type");
                }
                labeledCounter.SetAggregateFunc(NKikimr::TLabeledCounterOptions::EAggregateFunc(g.second->GetAggrFuncs()[i]));
                labeledCounter.SetType(NKikimr::TLabeledCounterOptions::ECounterType(g.second->GetTypes()[i]));
                labeledCounter.SetId(g.second->GetIds()[i].Get());
                labeledCounter.SetName(g.second->GetCounterName(i));
            }
        }
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator request processed  - got " << numGroups << " groups and " << numCounters << " counters " << ctx.SelfID << " Initiator " << Initiator);
        ui64 cookie = NumWorkers ? WorkerId : 0;
        ctx.Send(Initiator, response.Release(), 0, cookie);
        TBase::Die(ctx);
    }
};


class TClusterLabeledCountersAggregatorActorV2 : public TActorBootstrapped<TClusterLabeledCountersAggregatorActorV2> {
protected:
    using TBase = TActorBootstrapped<TClusterLabeledCountersAggregatorActorV2>;
    THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> Response;
    TTabletLabeledCountersResponseContext ResponseContext;
    TActorId Initiator;
    TTabletTypes::EType TabletType;
    ui32 NodesRequested;
    ui32 NodesReceived;
    TVector<ui32> Nodes;
    THashMap<ui32, THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>> PerNodeResponse;
    TString Group;
    ui32 NumWorkers;
    ui32 WorkerId;

    TMerger Merger;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_COUNTERS_AGGREGATOR;
    }

    TClusterLabeledCountersAggregatorActorV2(const TActorId& parentActor, const TTabletTypes::EType tabletType,
                                             const TString& group, ui32 numWorkers = 0, ui32 workerId = 0)
        : Response(new TEvTabletCounters::TEvTabletLabeledCountersResponse())
        , ResponseContext(Response->Record)
        , Initiator(parentActor)
        , TabletType(tabletType)
        , NodesRequested(0)
        , NodesReceived(0)
        , Group(group)
        , NumWorkers(numWorkers)
        , WorkerId(workerId)
        , Merger(Response, ResponseContext)
    {}

    void SendRequest(ui32 nodeId, const TActorContext &ctx) {
        TActorId aggregatorServiceId = MakeTabletCountersAggregatorID(nodeId);
        TAutoPtr<TEvTabletCounters::TEvTabletLabeledCountersRequest> request(new TEvTabletCounters::TEvTabletLabeledCountersRequest());
        request->Record.SetVersion(2);
        request->Record.SetTabletType(TabletType);
        if (!Group.empty()) {
            request->Record.SetGroup(Group);
        }
        // TODO: what if it's empty
        ctx.Send(aggregatorServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        Nodes.emplace_back(nodeId);
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor request to node " << nodeId << " " << ctx.SelfID);
        ++NodesRequested;
    }

    void Die(const TActorContext& ctx) override {
        for (const ui32 node : Nodes) {
            ctx.Send(TActivationContext::InterconnectProxy(node), new TEvents::TEvUnsubscribe());
        }
        TBase::Die(ctx);
    }

    void Bootstrap(const TActorContext& ctx) {
        if (NumWorkers > 0) {
            const TActorId nameserviceId = GetNameserviceActorId();
            ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
            TBase::Become(&TThis::StateRequestedBrowse);
            ctx.Schedule(TDuration::Seconds(AGGREGATOR_TIMEOUT_SECONDS), new TEvents::TEvWakeup());
            LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator new request V2 Initiator " << Initiator << " self " << ctx.SelfID << " worker " << WorkerId);
        } else {
            LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator new request V2 " << ctx.SelfID);
            for (ui32 i = 0; i < WorkerId; ++i) {
                ctx.Register(new TClusterLabeledCountersAggregatorActorV2(ctx.SelfID, TabletType, Group, WorkerId, i));
            }
            NodesRequested = WorkerId;
            TBase::Become(&TThis::StateRequested);
        }
    }

    STFUNC(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, HandleBrowse);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    STFUNC(StateRequested) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletCounters::TEvTabletLabeledCountersResponse, HandleResponse);
            HFunc(TEvents::TEvUndelivered, Undelivered);
            HFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void HandleBrowse(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx) {
        const TEvInterconnect::TEvNodesInfo* nodesInfo = ev->Get();
        Y_ABORT_UNLESS(!nodesInfo->Nodes.empty());
        Nodes.reserve(nodesInfo->Nodes.size());
        ui32 i = 0;
        for (const auto& ni : nodesInfo->Nodes) {
            ++i;
            if (i % NumWorkers == WorkerId) {
                SendRequest(ni.NodeId, ctx);
            }
        }
        if (NodesRequested > 0) {
            TBase::Become(&TThis::StateRequested);
        } else {
            ReplyAndDie(ctx);
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        ui32 nodeId = ev.Get()->Cookie;
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor undelivered node " << nodeId << " "  << ctx.SelfID);
        if (PerNodeResponse.emplace(nodeId, nullptr).second) {
            NodeResponseReceived(ctx);
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev, const TActorContext &ctx) {
        ui32 nodeId = ev->Get()->NodeId;
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor disconnected node " << nodeId << " " << ctx.SelfID);
        if (PerNodeResponse.emplace(nodeId, nullptr).second) {
            NodeResponseReceived(ctx);
        }
    }

    void HandleResponse(TEvTabletCounters::TEvTabletLabeledCountersResponse::TPtr &ev, const TActorContext &ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR,
                   "aggregator actor got response node " << nodeId << " " << ctx.SelfID);
        PreProcessResponse(ev->Get());

        auto [it, emplaced] = PerNodeResponse.emplace(nodeId, ev->Release().Release());
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR,
                   "aggregator actor merged response node " << nodeId << " " << ctx.SelfID);

        if (emplaced) {
            Merger.Merge(it->second->Record);
            NodeResponseReceived(ctx);
        }
    }

    void NodeResponseReceived(const TActorContext &ctx) {
        ++NodesReceived;
        if (NodesReceived >= NodesRequested) {
            ReplyAndDie(ctx);
        }
    }

    void HandleTimeout(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor got TIMEOUT");
        ReplyAndDie(ctx);
    }

    virtual void ReplyAndDie(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR,
                   "aggregator request processed " << ctx.SelfID << " Initiator " << Initiator);
        ui64 cookie = NumWorkers ? WorkerId : 0;
        ctx.Send(Initiator, Response.Release(), 0, cookie);
        TBase::Die(ctx);
    }
};

class TClusterLabeledCountersAggregatorActorV3 : public TClusterLabeledCountersAggregatorActorV2 {
public:
    TClusterLabeledCountersAggregatorActorV3(const TActorId& parentActor, const TTabletTypes::EType tabletType,
                                             const TString& group, ui32 numWorkers = 0, ui32 workerId = 0)
            : TClusterLabeledCountersAggregatorActorV2(parentActor, tabletType, group, numWorkers, workerId)
    {}

    void ReplyAndDie(const TActorContext& ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator request processed " << ctx.SelfID << " Initiator " << Initiator);
        ui64 cookie = NumWorkers ? WorkerId : 0;
        for (auto& counters : *Response->Record.MutableLabeledCountersByGroup()) {
            TVector<TString> groups;
            StringSplitter(counters.GetGroup()).SplitByString(counters.GetDelimiter()).SkipEmpty().Collect(&groups);
            TStringBuf ff;
            TString topic = "";
            TString dc = "";
            TString res;
            if (groups.size() == 1) { //topic case
                ff = groups[0];
            } else if (groups.size() == 3) { //client important topic
                res = NPersQueue::ConvertOldConsumerName(groups[0], ctx) + "|" + groups[1] + "|";
                ff = groups[2];
            } else {
                continue;
            }
            if (ff.empty())
                continue;
            TStringBuf tmp(ff.NextTok('.'));
            if (tmp != "rt3")
                continue;
            dc = TString(ff.NextTok("--"));
            if (dc.empty())
                continue;
            if (ff.empty())
                continue;
            topic = NPersQueue::ConvertOldTopicName(TString(ff));
            res += topic + "|" + dc;
            counters.SetGroup(res);
            counters.SetDelimiter("|");
        }
        ctx.Send(Initiator, Response.Release(), 0, cookie);
        TBase::Die(ctx);
    }
};

IActor* CreateClusterLabeledCountersAggregatorActor(const TActorId& parentActor,
    TTabletTypes::EType tabletType, ui32 version, const TString& group, const ui32 totalWorkersCount) {
    const auto numWorkers = totalWorkersCount == 0 ? 1 : 0;
    switch (version) {
    case 1:
        return new TClusterLabeledCountersAggregatorActorV1(parentActor, tabletType, numWorkers,
                                                            totalWorkersCount);
    case 2:
        return new TClusterLabeledCountersAggregatorActorV2(parentActor, tabletType, group, numWorkers,
                                                            totalWorkersCount);
    case 3:
        return new TClusterLabeledCountersAggregatorActorV3(parentActor, tabletType, group, numWorkers,
                                                            totalWorkersCount);
    }
    return nullptr;
}


TActorId CreateClusterLabeledCountersAggregator(const TActorId& parentActor, TTabletTypes::EType tabletType, const TActorContext& ctx, ui32 version, const TString& group, const ui32 totalWorkersCount) {
    return ctx.Register(CreateClusterLabeledCountersAggregatorActor(parentActor, tabletType, version, group, totalWorkersCount), TMailboxType::HTSwap, AppData(ctx)->BatchPoolId);
}

} // namespace NKikimr
