#pragma once

#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/tablet/labeled_db_counters.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/sys_view/service/db_counters.h>
#include <ydb/core/util/concurrent_rw_hash.h>

#include "aggregated_counters.h"


namespace NKikimr::NPrivate {

class TPQCounters : public ILabeledCounters {
protected:
    TConcurrentRWHashMap<TString, TIntrusivePtr<TAggregatedLabeledCounters>, 256> LabeledCountersByGroup;
    NMonitoring::TDynamicCounterPtr Group;

public:
    using TPtr = TIntrusivePtr<TPQCounters>;

    explicit TPQCounters(NMonitoring::TDynamicCounterPtr counters);

    void Apply(ui64 tabletID, const NKikimr::TTabletLabeledCountersBase* labeledCounters) override;
    void ForgetTablet(ui64 tabletID) override;

    const TProtobufTabletLabeledCounters<NKikimr::NPQ::EPartitionLabeledCounters_descriptor>
        PartitionCounters{"topic_name", 1, "/Root/Db"};
    const TProtobufTabletLabeledCounters<NKikimr::NPQ::EClientLabeledCounters_descriptor>
        UserCounters{"client_name||topic_name", 1, "/Root/Db"};
    const THashMap<TString, TAutoPtr<TAggregatedLabeledCounters>> LabeledCountersByGroupReference = {
        {
            "topic",
            new TAggregatedLabeledCounters(PartitionCounters.GetCounters().Size(),
                                           PartitionCounters.GetAggrFuncs(),
                                           PartitionCounters.GetNames(),
                                           PartitionCounters.GetTypes(),
                                           "topic")
        },
        {
            "client|important|topic",
            new TAggregatedLabeledCounters(UserCounters.GetCounters().Size(),
                                           UserCounters.GetAggrFuncs(),
                                           UserCounters.GetNames(),
                                           UserCounters.GetTypes(),
                                           "client|important|topic")
        },
        {}
    };
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
