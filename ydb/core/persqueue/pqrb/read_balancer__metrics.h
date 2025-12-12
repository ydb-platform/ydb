#pragma once

#include <ydb/core/persqueue/public/config.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <unordered_map>
#include <vector>

namespace NKikimr::NPQ {

struct TDatabaseInfo;

struct TCounters {
    std::unique_ptr<TTabletLabeledCountersBase> Config;
    std::vector<::NMonitoring::TDynamicCounters::TCounterPtr> Counters;
};

class TTopicMetrics {
public:
    TTopicMetrics();
    ~TTopicMetrics();

    void Initialize(const NKikimrPQ::TPQTabletConfig& tabletConfig, const TDatabaseInfo& database, const TString& topicPath, const NActors::TActorContext& ctx);
    void UpdateConfig(const NKikimrPQ::TPQTabletConfig& tabletConfig, const TDatabaseInfo& database, const TString& topicPath, const NActors::TActorContext& ctx);

    void Handle(NKikimrPQ::TStatusResponse_TPartResult&& partitionStatus);
    void UpdateMetrics();

protected:
    void InitializeKeyCompactionCounters(const TString& databasePath, const NKikimrPQ::TPQTabletConfig& tabletConfig);
    void InitializeConsumerCounters(const TString& databasePath, const NKikimrPQ::TPQTabletConfig& tabletConfig, const NActors::TActorContext& ctx);

private:
    NMonitoring::TDynamicCounterPtr DynamicCounters;

    NMonitoring::TDynamicCounters::TCounterPtr ActivePartitionCountCounter;
    NMonitoring::TDynamicCounters::TCounterPtr InactivePartitionCountCounter;

    TCounters PartitionLabeledCounters;
    TCounters PartitionExtendedLabeledCounters;
    TCounters PartitionKeyCompactionLabeledCounters;

    struct TConsumerCounters {
        TCounters ClientLabeledCounters;
        TCounters MLPClientLabeledCounters;
        ::NMonitoring::TDynamicCounters::TCounterPtr MLPMessageLockAttemptsCounter;
    };
    std::unordered_map<TString, TConsumerCounters> ConsumerCounters;

    std::unordered_map<ui32, NKikimrPQ::TStatusResponse_TPartResult> PartitionStatuses;
};
}
