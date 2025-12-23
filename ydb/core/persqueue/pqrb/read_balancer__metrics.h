#pragma once

#include <ydb/core/persqueue/public/config.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <vector>

namespace NKikimr::NPQ {

struct TDatabaseInfo;

struct TTopicMetrics {
    ui64 TotalDataSize = 0;
    ui64 TotalUsedReserveSize = 0;

    ui64 TotalAvgWriteSpeedPerSec = 0;
    ui64 MaxAvgWriteSpeedPerSec = 0;
    ui64 TotalAvgWriteSpeedPerMin = 0;
    ui64 MaxAvgWriteSpeedPerMin = 0;
    ui64 TotalAvgWriteSpeedPerHour = 0;
    ui64 MaxAvgWriteSpeedPerHour = 0;
    ui64 TotalAvgWriteSpeedPerDay = 0;
    ui64 MaxAvgWriteSpeedPerDay = 0;
};

struct TPartitionMetrics {
    ui64 DataSize = 0;
    ui64 UsedReserveSize = 0;
};

struct TCounters {
    const ui8* Types;
    std::vector<::NMonitoring::TDynamicCounters::TCounterPtr> Counters;
};

class TTopicMetricsHandler {
public:
    TTopicMetricsHandler();
    ~TTopicMetricsHandler();

    const TTopicMetrics& GetTopicMetrics() const;
    const absl::flat_hash_map<ui32, TPartitionMetrics>& GetPartitionMetrics() const;

    void Initialize(const NKikimrPQ::TPQTabletConfig& tabletConfig, const TDatabaseInfo& database, const TString& topicPath, const NActors::TActorContext& ctx);
    void UpdateConfig(const NKikimrPQ::TPQTabletConfig& tabletConfig, const TDatabaseInfo& database, const TString& topicPath, const NActors::TActorContext& ctx);
    void InitializePartitions(ui32 partitionId, ui64 dataSize, ui64 usedReserveSize);

    void Handle(NKikimrPQ::TStatusResponse_TPartResult&& partitionStatus);
    void UpdateMetrics();

protected:
    void InitializeKeyCompactionCounters(const NKikimrPQ::TPQTabletConfig& tabletConfig);
    void InitializeConsumerCounters(const NKikimrPQ::TPQTabletConfig& tabletConfig, const NActors::TActorContext& ctx);

private:
    NMonitoring::TDynamicCounterPtr DynamicCounters;

    TTopicMetrics TopicMetrics;
    absl::flat_hash_map<ui32, TPartitionMetrics> PartitionMetrics;

    NMonitoring::TDynamicCounters::TCounterPtr ActivePartitionCountCounter;
    NMonitoring::TDynamicCounters::TCounterPtr InactivePartitionCountCounter;

    TCounters PartitionLabeledCounters;
    TCounters PartitionExtendedLabeledCounters;
    TCounters PartitionKeyCompactionLabeledCounters;

    struct TConsumerCounters {
        TCounters ClientLabeledCounters;
        TCounters MLPClientLabeledCounters;
        ::NMonitoring::THistogramPtr MLPMessageLockAttemptsCounter;
        ::NMonitoring::THistogramPtr MLPMessageLockingDurationCounter;

        ::NMonitoring::TDynamicCounters::TCounterPtr DeletedByRetentionPolicyCounter;
        ::NMonitoring::TDynamicCounters::TCounterPtr DeletedByDeadlinePolicyCounter;
        ::NMonitoring::TDynamicCounters::TCounterPtr DeletedByMovedToDLQCounter;
    };
    absl::flat_hash_map<TString, TConsumerCounters> ConsumerCounters;

    absl::flat_hash_map<ui32, NKikimrPQ::TStatusResponse_TPartResult> PartitionStatuses;
};
}
