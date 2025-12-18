#include "read_balancer.h"
#include "read_balancer__metrics.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/common/percentiles.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

namespace NKikimr::NPQ {


namespace {

struct TMetricCollector {
    TMetricCollector(TTabletLabeledCountersBase&& counters)
        : Counters(std::move(counters))
    {}

    void Collect(const auto& values) {
        Collect(values.begin(), values.end());
    }

    void Collect(auto begin, auto end) {
        ssize_t in_size = std::distance(begin, end);
        AFL_ENSURE(in_size >= 0)("in_size", in_size);

        ssize_t count = std::min<ssize_t>(Counters.GetCounters().Size(), in_size);
        for (ssize_t i = 0; i < count; ++i) {
            Counters.GetCounters()[i] = *begin;
            ++begin;
        }

        // The aggregation function configured in the protofile is used for each counter.
        Aggregator.AggregateWith(Counters);
    }

    TTabletLabeledCountersBase Counters;
    TTabletLabeledCountersBase Aggregator;
};

struct THistogramMetricCollector {
    THistogramMetricCollector(const NMonitoring::TBucketBounds& bounds)
    {
        Values.resize(bounds.size() + 1);
    }

    void Collect(const auto& values) {
        Collect(values.begin(), values.end());
    }

    void Collect(auto begin, auto end) {
        ssize_t in_size = std::distance(begin, end);
        AFL_ENSURE(in_size >= 0)("in_size", in_size);

        if (size_t(in_size) != Values.size()) {
            return;
        }

        for (size_t i = 0; i < Values.size(); ++i) {
            Values[i] += *begin;
            ++begin;
        }
    }
    std::vector<ui64> Values;
};

struct TConsumerMetricCollector {
    TMetricCollector ClientLabeledCounters = TMetricCollector(CreateProtobufTabletLabeledCounters<EClientLabeledCounters_descriptor>());
    TMetricCollector MLPConsumerLabeledCounters = TMetricCollector(CreateProtobufTabletLabeledCounters<EMLPConsumerLabeledCounters_descriptor>());
    THistogramMetricCollector MLPMessageLockAttemptsCounter{MLP_LOCKS_BOUNDS};
    THistogramMetricCollector MLPMessageLockingDurationCounter{SLOW_LATENCY_BOUNDS};

    size_t DeletedByRetentionPolicy = 0;
    size_t DeletedByDeadlinePolicy = 0;
    size_t DeletedByMovedToDLQ = 0;

    void Collect(const NKikimrPQ::TAggregatedCounters::TMLPConsumerCounters& metrics) {
        MLPConsumerLabeledCounters.Collect(metrics.GetCountersValues());
        MLPMessageLockAttemptsCounter.Collect(metrics.GetMessageLocksValues());
        MLPMessageLockingDurationCounter.Collect(metrics.GetMessageLockingDurationValues());

        DeletedByRetentionPolicy += metrics.GetDeletedByRetentionPolicy();
        DeletedByDeadlinePolicy += metrics.GetDeletedByDeadlinePolicy();
        DeletedByMovedToDLQ += metrics.GetDeletedByMovedToDLQ();
    }
};

struct TTopicMetricCollector {
    TTopicMetricCollector(absl::flat_hash_map<ui32, TPartitionMetrics>& partitionMetrics)
        : PartitionMetrics(partitionMetrics)
    {
    }

    absl::flat_hash_map<ui32, TPartitionMetrics>& PartitionMetrics;

    TTopicMetrics TopicMetrics;

    TMetricCollector PartitionLabeledCounters = TMetricCollector(CreateProtobufTabletLabeledCounters<EPartitionLabeledCounters_descriptor>());
    TMetricCollector PartitionExtendedLabeledCounters = TMetricCollector(CreateProtobufTabletLabeledCounters<EPartitionExtendedLabeledCounters_descriptor>());
    TMetricCollector PartitionKeyCompactionLabeledCounters = TMetricCollector(CreateProtobufTabletLabeledCounters<EPartitionKeyCompactionLabeledCounters_descriptor>());

    absl::flat_hash_map<TString, TConsumerMetricCollector> Consumers;

    void Collect(const NKikimrPQ::TStatusResponse::TPartResult& partitionStatus) {
        auto& partitionMetrics = PartitionMetrics[partitionStatus.GetPartition()];
        partitionMetrics.DataSize = partitionStatus.GetPartitionSize();
        partitionMetrics.UsedReserveSize = partitionStatus.GetUsedReserveSize();

        TopicMetrics.TotalAvgWriteSpeedPerSec += partitionStatus.GetAvgWriteSpeedPerSec();
        TopicMetrics.MaxAvgWriteSpeedPerSec = Max<ui64>(TopicMetrics.MaxAvgWriteSpeedPerSec, partitionStatus.GetAvgWriteSpeedPerSec());
        TopicMetrics.TotalAvgWriteSpeedPerMin += partitionStatus.GetAvgWriteSpeedPerMin();
        TopicMetrics.MaxAvgWriteSpeedPerMin = Max<ui64>(TopicMetrics.MaxAvgWriteSpeedPerMin, partitionStatus.GetAvgWriteSpeedPerMin());
        TopicMetrics.TotalAvgWriteSpeedPerHour += partitionStatus.GetAvgWriteSpeedPerHour();
        TopicMetrics.MaxAvgWriteSpeedPerHour = Max<ui64>(TopicMetrics.MaxAvgWriteSpeedPerHour, partitionStatus.GetAvgWriteSpeedPerHour());
        TopicMetrics.TotalAvgWriteSpeedPerDay += partitionStatus.GetAvgWriteSpeedPerDay();
        TopicMetrics.MaxAvgWriteSpeedPerDay = Max<ui64>(TopicMetrics.MaxAvgWriteSpeedPerDay, partitionStatus.GetAvgWriteSpeedPerDay());

        Collect(partitionStatus.GetAggregatedCounters());
    }

    void Collect(const NKikimrPQ::TAggregatedCounters& counters) {
        PartitionLabeledCounters.Collect(counters.GetValues());
        PartitionExtendedLabeledCounters.Collect(counters.GetExtendedCounters().GetValues());
        PartitionKeyCompactionLabeledCounters.Collect(counters.GetCompactionCounters().GetValues());

        for (const auto& consumer : counters.GetConsumerAggregatedCounters()) {
            Consumers[consumer.GetConsumer()].ClientLabeledCounters.Collect(consumer.GetValues());
        }

        for (const auto& consumer : counters.GetMLPConsumerCounters()) {
            auto& collector = Consumers[consumer.GetConsumer()];
            collector.Collect(consumer);
        }
    }

    void Finish() {
        for (auto& [_, partitionMetrics] : PartitionMetrics) {
            TopicMetrics.TotalDataSize += partitionMetrics.DataSize;
            TopicMetrics.TotalUsedReserveSize += partitionMetrics.UsedReserveSize;
        }
    }
};

template<const NProtoBuf::EnumDescriptor* SimpleDesc()>
TCounters InitializeCounters(
    NMonitoring::TDynamicCounterPtr root,
    const std::vector<std::pair<TString, TString>>& subgroups = {},
    bool skipPrefix = true
) {
    auto group = root;

    for (const auto& subgroup : subgroups) {
        group = group->GetSubgroup(subgroup.first, subgroup.second);
    }

    const auto* config = NAux::GetLabeledCounterOpts<SimpleDesc>();

    std::vector<::NMonitoring::TDynamicCounters::TCounterPtr> result;
    for (size_t i = 0; i < config->Size; ++i) {
        TString name = config->GetSVNames()[i];
        if (skipPrefix) {
            TStringBuf nameBuf = name;
            nameBuf.SkipPrefix("PQ/");
            name = nameBuf;
        }
        result.push_back(name.empty() ? nullptr : group->GetExpiringNamedCounter("name", name, false));
    }

    return {
        .Types = config->GetCounterTypes(),
        .Counters = std::move(result)
    };
}

NMonitoring::TDynamicCounters::TCounterPtr InitializeDeleteCounter(NMonitoring::TDynamicCounterPtr root, const auto& reason) {
    return root->GetSubgroup("name", "topic.deleted_messages")->GetExpiringNamedCounter("reason", reason, false);
}

void SetCounters(TCounters& counters, const auto& metrics) {
    ui64 now = TAppData::TimeProvider->Now().MilliSeconds();
    auto& aggregatedCounters = metrics.Aggregator.GetCounters();

    for (size_t i = 0; i < counters.Counters.size(); ++i) {
        if (!counters.Counters[i]) {
            continue;
        }
        if (aggregatedCounters.Size() == i) {
            break;
        }

        auto value = aggregatedCounters[i].Get();
        const auto& type = counters.Types[i];
        if (type == TLabeledCounterOptions::CT_TIMELAG) {
            value = value < now ? now - value : 0;
        }

        counters.Counters[i]->Set(value);
    }
}

void SetCounters(NMonitoring::THistogramPtr& counter, const THistogramMetricCollector& metrics, const auto& bounds) {
    counter->Reset();
    for (size_t i = 0; i < bounds.size(); ++i) {
        counter->Collect(bounds[i], metrics.Values[i]);
    }
    counter->Collect(Max<double>(), metrics.Values[metrics.Values.size() - 1]);
}

}

TTopicMetricsHandler::TTopicMetricsHandler() = default;
TTopicMetricsHandler::~TTopicMetricsHandler() = default;

const TTopicMetrics& TTopicMetricsHandler::GetTopicMetrics() const {
    return TopicMetrics;
}

const absl::flat_hash_map<ui32, TPartitionMetrics>& TTopicMetricsHandler::GetPartitionMetrics() const {
    return PartitionMetrics;
}

void TTopicMetricsHandler::Initialize(const NKikimrPQ::TPQTabletConfig& tabletConfig, const TDatabaseInfo& database, const TString& topicPath, const NActors::TActorContext& ctx) {
    if (DynamicCounters) {
        return;
    }

    TStringBuf name = TStringBuf(topicPath);
    name.SkipPrefix(database.DatabasePath);
    name.SkipPrefix("/");

    bool isServerless = AppData(ctx)->FeatureFlags.GetEnableDbCounters(); //TODO: find out it via describe
    DynamicCounters = AppData(ctx)->Counters->GetSubgroup("counters", isServerless ? "topics_serverless" : "topics")
                ->GetSubgroup("host", "")
                ->GetSubgroup("database", database.DatabasePath)
                ->GetSubgroup("cloud_id", database.CloudId)
                ->GetSubgroup("folder_id", database.FolderId)
                ->GetSubgroup("database_id", database.DatabaseId)
                ->GetSubgroup("topic", TString(name));

    ActivePartitionCountCounter = DynamicCounters->GetExpiringNamedCounter("name", "topic.partition.active_count", false);
    InactivePartitionCountCounter = DynamicCounters->GetExpiringNamedCounter("name", "topic.partition.inactive_count", false);

    PartitionLabeledCounters = InitializeCounters<EPartitionLabeledCounters_descriptor>(DynamicCounters);
    PartitionExtendedLabeledCounters = InitializeCounters<EPartitionExtendedLabeledCounters_descriptor>(DynamicCounters, {}, true);
    InitializeKeyCompactionCounters(tabletConfig);
    InitializeConsumerCounters(tabletConfig, ctx);
}

void TTopicMetricsHandler::InitializeConsumerCounters(const NKikimrPQ::TPQTabletConfig& tabletConfig, const NActors::TActorContext& ctx) {
    for (const auto& consumer : tabletConfig.GetConsumers()) {
        auto metricsConsumerName = NPersQueue::ConvertOldConsumerName(consumer.GetName(), ctx);

        auto& counters = ConsumerCounters[consumer.GetName()];
        counters.ClientLabeledCounters = InitializeCounters<EClientLabeledCounters_descriptor>(DynamicCounters, {{"consumer", metricsConsumerName}});

        if (consumer.GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
            counters.MLPClientLabeledCounters = InitializeCounters<EMLPConsumerLabeledCounters_descriptor>(DynamicCounters, {{"consumer", metricsConsumerName}});

            auto consumerGroup = DynamicCounters->GetSubgroup("consumer", metricsConsumerName);
            counters.MLPMessageLockAttemptsCounter = consumerGroup->GetExpiringNamedHistogram(
                "name", "topic.message_lock_attempts", NMonitoring::ExplicitHistogram(MLP_LOCKS_BOUNDS));
            counters.MLPMessageLockingDurationCounter = consumerGroup->GetExpiringNamedHistogram(
                "name", "topic.message_locking_duration_milliseconds", NMonitoring::ExplicitHistogram(SLOW_LATENCY_BOUNDS));

            counters.DeletedByRetentionPolicyCounter = InitializeDeleteCounter(consumerGroup, "retention");
            counters.DeletedByDeadlinePolicyCounter = InitializeDeleteCounter(consumerGroup, "delete_policy");
            counters.DeletedByMovedToDLQCounter = InitializeDeleteCounter(consumerGroup, "move_policy");
        }
    }

    absl::flat_hash_set<std::string_view> existedConsumers;
    for (const auto& consumer : tabletConfig.GetConsumers()) {
        existedConsumers.insert(consumer.GetName());
    }
    for (auto it = ConsumerCounters.begin(); it != ConsumerCounters.end();) {
        if (!existedConsumers.contains(it->first)) {
            ConsumerCounters.erase(it++);
        } else {
            ++it;
        }
    }
}

void TTopicMetricsHandler::InitializeKeyCompactionCounters(const NKikimrPQ::TPQTabletConfig& tabletConfig) {
    if (tabletConfig.GetEnableCompactification()) {
        PartitionKeyCompactionLabeledCounters = InitializeCounters<EPartitionKeyCompactionLabeledCounters_descriptor>(DynamicCounters, {}, true);
    } else {
        PartitionKeyCompactionLabeledCounters.Counters.clear();
    }
}

void TTopicMetricsHandler::UpdateConfig(const NKikimrPQ::TPQTabletConfig& tabletConfig, const TDatabaseInfo& database, const TString& topicPath, const NActors::TActorContext& ctx) {
    Y_UNUSED(database);
    Y_UNUSED(topicPath);

    if (!DynamicCounters) {
        return;
    }

    InitializeKeyCompactionCounters(tabletConfig);
    InitializeConsumerCounters(tabletConfig, ctx);

    size_t inactiveCount = std::count_if(tabletConfig.GetAllPartitions().begin(), tabletConfig.GetAllPartitions().end(), [](auto& p) {
        return p.GetStatus() == NKikimrPQ::ETopicPartitionStatus::Inactive;
    });

    ActivePartitionCountCounter->Set(tabletConfig.GetAllPartitions().size() - inactiveCount);
    InactivePartitionCountCounter->Set(inactiveCount);
}

void TTopicMetricsHandler::InitializePartitions(ui32 partitionId, ui64 dataSize, ui64 usedReserveSize) {
    PartitionMetrics[partitionId] = {
        .DataSize = dataSize,
        .UsedReserveSize = usedReserveSize
    };

    TopicMetrics.TotalDataSize += dataSize;
    TopicMetrics.TotalUsedReserveSize += usedReserveSize;
}

void TTopicMetricsHandler::Handle(NKikimrPQ::TStatusResponse_TPartResult&& partitionStatus) {
    PartitionStatuses[partitionStatus.GetPartition()] = std::move(partitionStatus);
}

void TTopicMetricsHandler::UpdateMetrics() {
    if (!DynamicCounters || PartitionStatuses.empty()) {
        return;
    }

    TTopicMetricCollector collector(PartitionMetrics);
    for (auto& [_, partitionStatus] : PartitionStatuses) {
        collector.Collect(partitionStatus);
    }
    collector.Finish();

    PartitionStatuses.erase(PartitionStatuses.begin(), PartitionStatuses.end());

    TopicMetrics = collector.TopicMetrics;

    SetCounters(PartitionLabeledCounters, collector.PartitionLabeledCounters);
    SetCounters(PartitionExtendedLabeledCounters, collector.PartitionExtendedLabeledCounters);
    SetCounters(PartitionKeyCompactionLabeledCounters, collector.PartitionKeyCompactionLabeledCounters);

    for (auto& [consumer, consumerCounters] : ConsumerCounters) {
        auto it = collector.Consumers.find(consumer);
        if (it == collector.Consumers.end()) {
            continue;
        }

        auto& consumerMetrics = it->second;

        SetCounters(consumerCounters.ClientLabeledCounters, consumerMetrics.ClientLabeledCounters);
        if (!consumerCounters.MLPClientLabeledCounters.Counters.empty()) {
            SetCounters(consumerCounters.MLPClientLabeledCounters, consumerMetrics.MLPConsumerLabeledCounters);
            SetCounters(consumerCounters.MLPMessageLockAttemptsCounter, consumerMetrics.MLPMessageLockAttemptsCounter, MLP_LOCKS_BOUNDS);
            SetCounters(consumerCounters.MLPMessageLockingDurationCounter, consumerMetrics.MLPMessageLockingDurationCounter, SLOW_LATENCY_BOUNDS);

            consumerCounters.DeletedByRetentionPolicyCounter->Set(consumerMetrics.DeletedByRetentionPolicy);
            consumerCounters.DeletedByDeadlinePolicyCounter->Set(consumerMetrics.DeletedByDeadlinePolicy);
            consumerCounters.DeletedByMovedToDLQCounter->Set(consumerMetrics.DeletedByMovedToDLQ);
        }
    }
}

}
