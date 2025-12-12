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
    TMetricCollector(size_t size)
        : Counters(size)
    {
    }

    void Collect(const auto& values) {
        Collect(values.begin(), values.end());
    }

    void Collect(auto begin, auto end) {
        ssize_t in_size = std::distance(begin, end);
        AFL_ENSURE(in_size >= 0)("in_size", in_size);
        size_t count = std::min(Counters.size(), static_cast<size_t>(in_size));

        for (size_t i = 0; i < count; ++i) {
            Counters[i] += *begin;
            ++begin;
        }
    }

    std::vector<ui64> Counters;
};

struct TConsumerMetricCollector {
    TConsumerMetricCollector()
        : ClientLabeledCounters(EClientLabeledCounters_descriptor()->value_count())
        //, MLPConsumerLabeledCounters(EMLPConsumerLabeledCounters_descriptor()->value_count())
        //, MLPMessageLockAttemptsCounter(std::size(MLP_LOCKS_INTERVALS))
    {
    }

    TMetricCollector ClientLabeledCounters;
    //TMetricCollector MLPConsumerLabeledCounters;
    //TMetricCollector MLPMessageLockAttemptsCounter;
};

struct TTopicMetricCollector {
    TTopicMetricCollector()
        : PartitionLabeledCounters(EPartitionLabeledCounters_descriptor()->value_count())
        , PartitionExtendedLabeledCounters(EPartitionExtendedLabeledCounters_descriptor()->value_count())
        , PartitionKeyCompactionLabeledCounters(EPartitionKeyCompactionLabeledCounters_descriptor()->value_count())
    {
    }

    TMetricCollector PartitionLabeledCounters;
    TMetricCollector PartitionExtendedLabeledCounters;
    TMetricCollector PartitionKeyCompactionLabeledCounters;

    std::unordered_map<TString, TConsumerMetricCollector> Consumers;

    void Collect(const NKikimrPQ::TStatusResponse::TPartResult& partitionStatus) {
        Collect(partitionStatus.GetAggregatedCounters());
    }

    void Collect(const NKikimrPQ::TAggregatedCounters& counters) {
        PartitionLabeledCounters.Collect(counters.GetValues());
        PartitionExtendedLabeledCounters.Collect(counters.GetExtendedCounters().GetValues());
        PartitionKeyCompactionLabeledCounters.Collect(counters.GetCompactionCounters().GetValues());

        for (const auto& consumer : counters.GetConsumerAggregatedCounters()) {
            Consumers[consumer.GetConsumer()].ClientLabeledCounters.Collect(consumer.GetValues());
        }

        // for (const auto& consumer : counters.GetMLPConsumerCounters()) {
        //     auto& collector = Consumers[consumer.GetConsumer()];
        //     collector.MLPConsumerLabeledCounters.Collect(consumer.GetCountersValues());
        //     collector.MLPMessageLockAttemptsCounter.Collect(consumer.GetMessageLocksValues());
        // }
    }
};

template<const NProtoBuf::EnumDescriptor* SimpleDesc()>
TCounters InitializeCounters(
    NMonitoring::TDynamicCounterPtr root,
    const TString& databasePath,
    const TString labels = "topic",
    const std::vector<std::pair<TString, TString>>& subgroups = {},
    bool skipPrefix = true
) {
    auto group = root;

    for (const auto& subgroup : subgroups) {
        group = group->GetSubgroup(subgroup.first, subgroup.second);
    }

    using TConfig = TProtobufTabletLabeledCounters<SimpleDesc>;
    auto config = std::make_unique<TConfig>(labels, 0, databasePath);

    std::vector<::NMonitoring::TDynamicCounters::TCounterPtr> result;
    for (size_t i = 0; i < config->GetCounters().Size(); ++i) {
        TString name = config->GetNames()[i];
        if (skipPrefix) {
            TStringBuf nameBuf = name;
            nameBuf.SkipPrefix("PQ/");
            name = nameBuf;
        }
        result.push_back(name.empty() ? nullptr : group->GetExpiringNamedCounter("name", name, false));
    }

    return {
        .Config = std::move(config),
        .Counters = std::move(result)
    };
}

void SetCounters(TCounters& counters, const TMetricCollector& metrics) {
    ui64 now = TAppData::TimeProvider->Now().MilliSeconds();

    for (size_t i = 0; i < counters.Counters.size(); ++i) {
        auto value = metrics.Counters[i];

        const auto& type = counters.Config->GetCounterType(i);
        if (type == TLabeledCounterOptions::CT_TIMELAG) {
            value = value < now ? now - value : 0;
        }
 
        counters.Counters[i]->Set(value);
    }
}

}



void TTopicMetrics::Initialize(const NKikimrPQ::TPQTabletConfig& tabletConfig, const TDatabaseInfo& database, const TString& topicPath, const NActors::TActorContext& ctx) {
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

    PartitionLabeledCounters = InitializeCounters<EPartitionLabeledCounters_descriptor>(DynamicCounters, database.DatabasePath);
    PartitionExtendedLabeledCounters = InitializeCounters<EPartitionExtendedLabeledCounters_descriptor>(DynamicCounters, database.DatabasePath, "topic", {}, true);
    InitializeKeyCompactionCounters(database.DatabasePath, tabletConfig);
    InitializeConsumerCounters(database.DatabasePath, tabletConfig, ctx);
}

void TTopicMetrics::InitializeConsumerCounters(const TString& databasePath, const NKikimrPQ::TPQTabletConfig& tabletConfig, const NActors::TActorContext& ctx) {
    for (const auto& consumer : tabletConfig.GetConsumers()) {
        auto metricsConsumerName = NPersQueue::ConvertOldConsumerName(consumer.GetName(), ctx);

        auto& counters = ConsumerCounters[consumer.GetName()];
        counters.ClientLabeledCounters = InitializeCounters<EClientLabeledCounters_descriptor>(DynamicCounters, databasePath, "topic|x|consumer",{{"consumer", metricsConsumerName}});

        if (consumer.GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
            //metrics.MLPClientLabeledCounters = InitializeCounters<EMLPConsumerLabeledCounters_descriptor>(DynamicCounters, databasePath, "topic|consumer", {{"consumer", metricsConsumerName}});
            //metrics.MLPMessageLockAttemptsCounter = InitializeCounters<EMLPMessageLockAttemptsLabeledCounters_descriptor>(DynamicCounters, databasePath, {{"consumer", metricsConsumerName}});
        }
    }

    std::unordered_set<std::string_view> existedConsumers;
    for (const auto& consumer : tabletConfig.GetConsumers()) {
        existedConsumers.insert(consumer.GetName());
    }
    for (auto it = ConsumerCounters.begin(); it != ConsumerCounters.end();) {
        if (!existedConsumers.contains(it->first)) {
            it = ConsumerCounters.erase(it);
        } else {
            ++it;
        }
    }
}

void TTopicMetrics::InitializeKeyCompactionCounters(const TString& databasePath, const NKikimrPQ::TPQTabletConfig& tabletConfig) {
    if (tabletConfig.GetEnableCompactification()) {
        PartitionKeyCompactionLabeledCounters = InitializeCounters<EPartitionKeyCompactionLabeledCounters_descriptor>(DynamicCounters, databasePath, "topic", {}, true);
    } else {
        PartitionKeyCompactionLabeledCounters.Counters.clear();
    }
}

void TTopicMetrics::UpdateConfig(const NKikimrPQ::TPQTabletConfig& tabletConfig, const TDatabaseInfo& database, const TString& topicPath, const NActors::TActorContext& ctx) {
    Y_UNUSED(topicPath);

    if (!DynamicCounters) {
        return;
    }

    InitializeConsumerCounters(database.DatabasePath, tabletConfig, ctx);

    size_t inactiveCount = std::count_if(tabletConfig.GetAllPartitions().begin(), tabletConfig.GetAllPartitions().end(), [](auto& p) {
        return p.GetStatus() == NKikimrPQ::ETopicPartitionStatus::Inactive;
    });

    ActivePartitionCountCounter->Set(tabletConfig.GetAllPartitions().size() - inactiveCount);
    InactivePartitionCountCounter->Set(inactiveCount);
}

void TTopicMetrics::Handle(NKikimrPQ::TStatusResponse::TPartResult&& partitionStatus) {
    PartitionStatuses[partitionStatus.GetPartition()] = std::move(partitionStatus);
}

void TTopicMetrics::UpdateMetrics() {
    if (PartitionStatuses.empty()) {
        return;
    }

    TTopicMetricCollector collector;
    for (auto& [_, partitionStatus] : PartitionStatuses) {
        collector.Collect(partitionStatus);
    }

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
            // SetCounters(consumerCounters.MLPClientLabeledCounters, consumerMetrics.MLPConsumerLabeledCounters);
            // TODO MLPMessageLockAttemptsCounter
        }
    }
}

}