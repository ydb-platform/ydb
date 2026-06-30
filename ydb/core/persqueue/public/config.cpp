#include "config.h"
#include "constants.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <util/generic/hash_set.h>
#include <util/string/printf.h>

namespace NKikimr {

bool CheckPersQueueConfig(const NKikimrPQ::TPQTabletConfig& config, const bool shouldHavePartitionsList, TString *error) {
    if (!config.HasPartitionConfig()) {
        if (error)
            *error = "no PartitionConfig";
        return false;
    }

    const auto& partitionIds = config.GetPartitionIds();
    const auto& partitions = config.GetPartitions();

    if (shouldHavePartitionsList) {
        if (partitionIds.empty() && partitions.empty()) {
            if (error)
                *error = "empty Partitions list";
            return false;
        }

        THashSet<ui32> parts;
        for (const auto partitionId : partitionIds) {
            if (!parts.insert(partitionId).second) {
                if (error)
                    *error = Sprintf("duplicate partitions with id %u", partitionId);
                return false;
            }
        }

        parts.clear();
        for (const auto& partition : partitions) {
            const auto partitionId = partition.GetPartitionId();
            if (!parts.insert(partitionId).second) {
                if (error)
                    *error = Sprintf("duplicate partitions with id %u", partitionId);
                return false;
            }
        }
    } else {
        if (!partitionIds.empty() || !partitions.empty()) {
            if (error)
                *error = "Partitions list must be empty";
            return false;
        }
    }

    return true;
}

namespace NPQ {

bool IsQuotingEnabled(const NKikimrPQ::TPQConfig& pqConfig, bool isLocalDC) {
    const auto& quotingConfig = pqConfig.GetQuotingConfig();
    return isLocalDC && quotingConfig.GetEnableQuoting() && !pqConfig.GetTopicsAreFirstClassCitizen();
}

bool DetailedMetricsAreEnabled(const NKikimrPQ::TPQTabletConfig& config) {
    return AppData()->FeatureFlags.GetEnableMetricsLevel() && config.HasMetricsLevel() && config.GetMetricsLevel() == METRICS_LEVEL_DETAILED;
}

const NKikimrPQ::TPQTabletConfig_TPartition* GetPartitionConfigFromAllPartitions(const NKikimrPQ::TPQTabletConfig& config Y_LIFETIME_BOUND, const ui32 partitionId) noexcept {
    for (const auto& partition : config.GetAllPartitions()) {
        if (partition.GetPartitionId() == partitionId) {
            return &partition;
        }
    }
    return nullptr;
}

TString GetDLQTopicPath(const NKikimrPQ::TPQTabletConfig_TConsumer& consumer) {
    if (consumer.GetType() != NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
        return {};
    }
    if (consumer.GetDeadLetterPolicy() != NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE
        || !consumer.GetDeadLetterPolicyEnabled()) {
        return {};
    }

    const auto& dlq = consumer.GetDeadLetterQueue();
    if (dlq.empty() || dlq.StartsWith("sqs://"sv)) {
        return {};
    }

    return dlq;
}

THashSet<TString> CollectDLQTopicPaths(
    const NKikimrPQ::TPQTabletConfig& config,
    const TString& database,
    std::optional<ui64> modificationVersion
) {
    THashSet<TString> result;

    for (const auto& consumer : config.GetConsumers()) {
        if (modificationVersion && consumer.GetModificationVersion() != *modificationVersion) {
            continue;
        }

        const auto dlq = GetDLQTopicPath(consumer);
        if (dlq.empty()) {
            continue;
        }

        result.insert(NKikimr::CanonizeAndNormalizePath(database, dlq));
    }

    return result;
}

bool IsTopicMessagesBatchingEnabled(const NActors::TActorContext& ctx) {
    return AppData(ctx)->FeatureFlags.GetEnableTopicMessagesBatching() &&
        AppData(ctx)->FeatureFlags.GetEnableTopicWriteOffsetDeltaInKeys();
}

} // namespace NPQ

} // namespace NKikimr
