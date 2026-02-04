#pragma once

#include <ydb/core/protos/pqconfig.pb.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <google/protobuf/map.h>

#include <expected>
#include <string>

namespace NKikimr::NSqsTopic::V1 {

    struct TConsumerAttributes {
        NKikimrPQ::TPQTabletConfig::TConsumer Consumer;
        TMaybe<TDuration> MessageRetentionPeriod;
        TMaybe<ui32> MaximumMessageSize;
        TMaybe<TString> KmsMasterKeyId;
        TMaybe<TDuration> KmsDataKeyReusePeriodSeconds;
        TMaybe<bool> SqsManagedSseEnabled;
    };

    enum class EConsumerAttributeUsageTarget {
        Create,
        Alter,
    };

    std::expected<TConsumerAttributes, std::string> ParseConsumerAttributes(
        const google::protobuf::Map<TString, TString>& attributes,
        const TString& queueName,
        const TString& consumerName,
        const TString& database,
        EConsumerAttributeUsageTarget usageTarget
    );

    std::expected<void, std::string> CompareWithExistingQueueAttributes(
        const NKikimrPQ::TPQTabletConfig& existingConfig,
        const NKikimrPQ::TPQTabletConfig::TConsumer& existingConsumer,
        const TConsumerAttributes& newConfig
    );

    std::expected<void, std::string> ValidateLimits(const TConsumerAttributes& config);
} // namespace NKikimr::NSqsTopic::V1
