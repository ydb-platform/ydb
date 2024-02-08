#pragma once

#include <ydb/core/protos/kesus.pb.h>
#include <ydb/library/time_series_vec/time_series_vec.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/noncopyable.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>

namespace NKikimr {

struct TBillRecord;

namespace NKesus {

// NOTE: IntervalUs is better to be the same on quoter proxy and kesus tablet
using TConsumptionHistory = TTimeSeriesVec<double>;

// Bill sink - encapsulates send method to metering actor
// Actually implements dependency injection for testing purpose
class IBillSink : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IBillSink>;
    virtual void Send(const NActors::TActorContext& ctx, const TBillRecord& billRecord) = 0;
};

IBillSink::TPtr MakeMeteringSink();

struct TRateAccountingCounters {
    TIntrusivePtr<::NMonitoring::TDynamicCounters> ResourceCounters;
    ::NMonitoring::TDynamicCounters::TCounterPtr Provisioned;
    ::NMonitoring::TDynamicCounters::TCounterPtr OnDemand;
    ::NMonitoring::TDynamicCounters::TCounterPtr Overshoot;

    void SetResourceCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& resourceCounters) {
        ResourceCounters = resourceCounters;
        if (ResourceCounters) {
            Provisioned = ResourceCounters->GetCounter("Provisioned", true);
            OnDemand = ResourceCounters->GetCounter("OnDemand", true);
            Overshoot = ResourceCounters->GetCounter("Overshoot", true);
        } else {
            Provisioned = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
            OnDemand = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
            Overshoot = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
        }
    }
};


// Consumption aggregation logic and bill making
class TRateAccounting : public TNonCopyable {
    NKikimrKesus::TStreamingQuoterResource Props;
    TString QuoterPath;
    using TDedupId = std::pair<NActors::TActorId, ui64>; // clientId, resourceId
    THashMap<TDedupId, TInstant> ClientToReported; // Most recent reported time by client
    TSet<std::pair<TInstant, TDedupId>> SortedClients; // for fast cleanup
    TInstant Accounted; // Next accounting period begining
    TConsumptionHistory History;
    NActors::TActorId Kesus;
    NActors::TActorId AccountingActor;
    TDuration MaxBillingPeriod;
    TRateAccountingCounters Counters;
public:
    // Init and create accounting actor
    TRateAccounting(NActors::TActorId kesus, const IBillSink::TPtr& billSink, const NKikimrKesus::TStreamingQuoterResource& props, const TString& quoterPath);

    // Destroy accounting actor
    void Stop();

    // Configuration
    static bool ValidateProps(const NKikimrKesus::TStreamingQuoterResource& props, TString& errorMessage);
    void Configure(const NKikimrKesus::TStreamingQuoterResource& props);

    // Deduplicate and merge client's data into consumption history
    TInstant Report(const NActors::TActorId& clientId, ui64 resourceId, TInstant start, TDuration interval, const double* values, size_t size);

    // Check timings and send message to accounting actor if required
    // Should be called periodically to account tail if there is no reports any longer
    // Returns true iff there are unsent reports (retry on next tick is required)
    bool RunAccounting();

    void SetResourceCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& resourceCounters);

private:
    void ConfigureImpl();
    void RemoveOldClients();
    bool RunAccountingNoClean();

    TDuration CollectPeriod() const {
        return TDuration::Seconds(Props.GetAccountingConfig().GetCollectPeriodSec());
    }

    TDuration AccountPeriod() const {
        return TDuration::MilliSeconds(Props.GetAccountingConfig().GetAccountPeriodMs());
    }

    size_t HistorySize() const {
        // One collect period into future and one into past plus window for accounting
        return (2 * CollectPeriod() + AccountPeriod()) / History.Interval();
    }
};

}   // namespace NKesus
}   // namespace NKikimr
