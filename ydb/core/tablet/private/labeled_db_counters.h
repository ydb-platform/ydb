#pragma once

#include <ydb/core/sys_view/service/db_counters.h>
#include <ydb/core/tablet/labeled_db_counters.h>
#include <ydb/core/util/concurrent_rw_hash.h>

#include "aggregated_counters.h"


namespace NKikimr::NPrivate {


class TPQCounters : public ILabeledCounters {
protected:
    TConcurrentRWHashMap<TString, TAutoPtr<TAggregatedLabeledCounters>, 256> LabeledCountersByGroup;
    NMonitoring::TDynamicCounterPtr Group;

public:
    using TPtr = TIntrusivePtr<TPQCounters>;

    explicit TPQCounters(NMonitoring::TDynamicCounterPtr counters);

    void Apply(ui64 tabletID, const NKikimr::TTabletLabeledCountersBase* labeledCounters) override;
    void ForgetTablet(ui64 tabletID) override;

    static THashMap<TString, TAutoPtr<TAggregatedLabeledCounters>> LabeledCountersByGroupReference;
};

class TDbLabeledCounters : public TPQCounters, public NSysView::IDbCounters {
public:
    using TPtr = TIntrusivePtr<TDbLabeledCounters>;

    TDbLabeledCounters();
    explicit TDbLabeledCounters(::NMonitoring::TDynamicCounterPtr counters);

    void ToProto(NKikimr::NSysView::TDbServiceCounters& counters) override;
    void FromProto(NKikimr::NSysView::TDbServiceCounters& counters) override;
};

} // namespace NKikimr::NPrivate
