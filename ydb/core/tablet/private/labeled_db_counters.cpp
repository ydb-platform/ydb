#include "labeled_db_counters.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <util/string/split.h>
#include <ydb/core/sys_view/service/sysview_service.h>

namespace NKikimr::NPrivate {

/*
** class TPQCounters
 */

TPQCounters::TPQCounters(NMonitoring::TDynamicCounterPtr counters) {
    Group = counters;
}

void TPQCounters::Apply(ui64 tabletId, const NKikimr::TTabletLabeledCountersBase* labeledCounters) {
    const TString group = labeledCounters->GetGroup();
    TString groupNames;
    if (labeledCounters->GetDrop()) {
        LabeledCountersByGroup.Erase(group);
        return;
    }
    if (!LabeledCountersByGroup.Has(group)) {
        TVector<TString> rr;
        StringSplitter(group).Split('|').Collect(&rr);
        for (ui32 i = 0; i < rr.size(); ++i) {
            if (i > 0)
                groupNames += '|';
            groupNames += labeledCounters->GetGroupName(i);
        }
    }

    auto el = LabeledCountersByGroup.InsertIfAbsent(group, new TAggregatedLabeledCounters(
        labeledCounters->GetCounters().Size(), labeledCounters->GetAggrFuncs(),
        labeledCounters->GetNames(), labeledCounters->GetTypes(), groupNames));

    for (ui32 i = 0, N = labeledCounters->GetCounters().Size(); i < N; ++i) {
        const ui64& value = labeledCounters->GetCounters()[i].Get();
        // FIXME (?):
        // const ui64& id = labeledCounters->GetIds()[i].Get();
        const ui64 id = i;
        el->SetValue(tabletId, i, value, id);
    }
}

void TPQCounters::ForgetTablet(ui64 tabletId) {
    for (auto& bucket : LabeledCountersByGroup.Buckets) {
        TWriteGuard guard(bucket.GetLock());
        auto& map = bucket.GetMap();
        for (auto iterator = map.begin(); iterator != map.end();) {
            bool empty = iterator->second->ForgetTablet(tabletId);
            if (empty) {
                auto eraseIterator = iterator;
                ++iterator;
                map.erase(eraseIterator);
            } else {
                ++iterator;
            }
        }
    }
}

/*
** class TDbLabeledCounters
 */

TDbLabeledCounters::TDbLabeledCounters()
: TPQCounters(MakeIntrusive<::NMonitoring::TDynamicCounters>())
{}

TDbLabeledCounters::TDbLabeledCounters(::NMonitoring::TDynamicCounterPtr counters)
: TPQCounters(counters)
{}

void TDbLabeledCounters::ToProto(NKikimr::NSysView::TDbServiceCounters& counters) {
    counters.Clear();
    for (auto& bucket : LabeledCountersByGroup.Buckets) {
        TWriteGuard guard(bucket.GetLock());
        for (auto& [group, labeledCounters] : bucket.GetMap()) {
            auto* proto = counters.FindOrAddLabeledCounters(group);
            auto* labeled = proto->MutableAggregatedPerTablets();
            labeledCounters->ToProto(*labeled);
        }
    }
}

void TDbLabeledCounters::FromProto(NKikimr::NSysView::TDbServiceCounters& counters) {
    for (auto& proto : *counters.Proto().MutableLabeledCounters()) {
        TVector<TString> groups;
        TVector<TString> groupNames = {"topic", "important", "consumer"};
        Y_ABORT_UNLESS(proto.GetAggregatedPerTablets().GetDelimiter() == "|");
        StringSplitter(proto.GetAggregatedPerTablets().GetGroup()).Split('|').Collect(&groups);
        auto countersGroup = Group;
        // FIXME: a little hack here: we have consumer - important - topic group names in proto
        // that's why we iterate the group in reverse order
        // this comes from: ydb/core/persqueue/user_info.h:310 (TUserInfo::TUserInfo)
        std::reverse(groups.begin(), groups.end());

        for (size_t i = 0; i < groups.size(); ++i) {
            if (i != 1) {
                countersGroup = countersGroup->GetSubgroup(groupNames[i], groups[i]);
            }
        }
        const TString groupNamesStr = (groups.size() == 3) ? "client|important|topic" : "topic";

        LabeledCountersByGroupReference.at(groupNamesStr)->FromProto(countersGroup,
                                                                     proto.GetAggregatedPerTablets());
    }
}

} // namespace NKikimr::NPrivate
