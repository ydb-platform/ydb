#pragma once

#include <ydb/core/protos/pqconfig.pb.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <google/protobuf/map.h>

#include <expected>
#include <string>

namespace NKikimr::NSqsTopic::V1 {

    struct TQueueAttributes {
        TMaybe<TDuration> DefaultProcessingTimeout;
        TMaybe<TDuration> MessageRetentionPeriod;
        TMaybe<TDuration> ReceiveMessageDelay;
        TMaybe<TDuration> ReceiveMessageWaitTime;
        TMaybe<ui32> MaximumMessageSize;
        TMaybe<TString> KmsMasterKeyId;
        TMaybe<TDuration> KmsDataKeyReusePeriodSeconds;
        TMaybe<bool> SqsManagedSseEnabled;
        TMaybe<bool> ContentBasedDeduplication;
        TMaybe<ui32> MaxReceiveCount;
        TMaybe<TString> DeadLetterQueue;
        bool FifoQueue = false;
    };

    enum class EConsumerAttributeUsageTarget {
        Create,
        Alter,
    };

    std::expected<TQueueAttributes, std::string> ParseQueueAttributes(
        const google::protobuf::Map<TString, TString>& attributes,
        const TString& queueName,
        const TString& consumerName,
        const TString& database,
        EConsumerAttributeUsageTarget usageTarget
    );

    std::expected<void, std::string> CompareWithExistingQueueAttributes(
        const NKikimrPQ::TPQTabletConfig& existingConfig,
        const NKikimrPQ::TPQTabletConfig::TConsumer& existingConsumer,
        const TQueueAttributes& newConfig
    );

    std::expected<void, std::string> ValidateLimits(const TQueueAttributes& config);
} // namespace NKikimr::NSqsTopic::V1
