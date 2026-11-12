#pragma once

#include "node.h"

namespace NSQLTranslationV1 {

struct TTopicRef {
    TString RefName;
    TDeferredAtom Cluster;
    TNodePtr Consumers;
    TNodePtr Settings;
    TNodePtr Keys;

    TTopicRef() = default;
    TTopicRef(TString refName, TDeferredAtom cluster, TNodePtr keys);
    TTopicRef(const TTopicRef&) = default;
    TTopicRef& operator=(const TTopicRef&) = default;
};

struct TTopicConsumerSettings {
    struct TLocalSinkSettings {
        // no special settings
    };

    TNodePtr Important;
    NYql::TResetableSetting<TNodePtr, void> AvailabilityPeriod;
    NYql::TResetableSetting<TNodePtr, void> ReadFromTs;
    NYql::TResetableSetting<TNodePtr, void> SupportedCodecs;
    TNodePtr Type;
    TNodePtr KeepMessagesOrder;
    TNodePtr DefaultProcessingTimeout;
    TNodePtr MaxProcessingAttempts;
    TNodePtr DeadLetterPolicy;
    TNodePtr DeadLetterQueue;
    TNodePtr ReceiveMessageWaitTime;
    TNodePtr ReceiveMessageDelay;
};

struct TTopicConsumerDescription {
    explicit TTopicConsumerDescription(TIdentifier name)
        : Name(std::move(name))
    {
    }

    TIdentifier Name;
    TTopicConsumerSettings Settings;
};
struct TTopicSettings {
    NYql::TResetableSetting<TNodePtr, void> MinPartitions;
    NYql::TResetableSetting<TNodePtr, void> MaxPartitions;
    NYql::TResetableSetting<TNodePtr, void> RetentionPeriod;
    NYql::TResetableSetting<TNodePtr, void> RetentionStorage;
    NYql::TResetableSetting<TNodePtr, void> SupportedCodecs;
    NYql::TResetableSetting<TNodePtr, void> PartitionWriteSpeed;
    NYql::TResetableSetting<TNodePtr, void> PartitionWriteBurstSpeed;
    NYql::TResetableSetting<TNodePtr, void> MeteringMode;
    NYql::TResetableSetting<TNodePtr, void> AutoPartitioningStabilizationWindow;
    NYql::TResetableSetting<TNodePtr, void> AutoPartitioningUpUtilizationPercent;
    NYql::TResetableSetting<TNodePtr, void> AutoPartitioningDownUtilizationPercent;
    NYql::TResetableSetting<TNodePtr, void> AutoPartitioningStrategy;
    NYql::TResetableSetting<TNodePtr, void> MetricsLevel;
    NYql::TResetableSetting<TNodePtr, void> ContentBasedDeduplication;

    bool IsSet() const {
        return MinPartitions ||
               MaxPartitions ||
               RetentionPeriod ||
               RetentionStorage ||
               SupportedCodecs ||
               PartitionWriteSpeed ||
               PartitionWriteBurstSpeed ||
               MeteringMode ||
               AutoPartitioningStabilizationWindow ||
               AutoPartitioningUpUtilizationPercent ||
               AutoPartitioningDownUtilizationPercent ||
               AutoPartitioningStrategy ||
               MetricsLevel ||
               ContentBasedDeduplication;
    }
};

struct TCreateTopicParameters {
    TVector<TTopicConsumerDescription> Consumers;
    TTopicSettings TopicSettings;
    bool ExistingOk;
};

struct TAlterTopicParameters {
    TVector<TTopicConsumerDescription> AddConsumers;
    THashMap<TString, TTopicConsumerDescription> AlterConsumers;
    TVector<TIdentifier> DropConsumers;
    TTopicSettings TopicSettings;
    bool MissingOk;
};

struct TDropTopicParameters {
    bool MissingOk;
};

TNodePtr BuildTopicKey(TPosition pos, const TDeferredAtom& cluster, const TDeferredAtom& name);

TNodePtr BuildCreateTopic(TPosition pos, const TTopicRef& tr, const TCreateTopicParameters& params,
                          TScopedStatePtr scoped);
TNodePtr BuildAlterTopic(TPosition pos, const TTopicRef& tr, const TAlterTopicParameters& params,
                         TScopedStatePtr scoped);
TNodePtr BuildDropTopic(TPosition pos, const TTopicRef& topic, const TDropTopicParameters& params,
                        TScopedStatePtr scoped);

} // namespace NSQLTranslationV1
