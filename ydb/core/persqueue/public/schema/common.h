#pragma once

#include "schema.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/persqueue/public/cluster_tracker/cluster_tracker.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/services/lib/actors/consumers_advanced_monitoring_settings.h>

#include <expected>
#include <map>
#include <optional>

namespace NKikimr::NPQ::NSchema {

enum class EOperation {
    Create,
    Alter,
    Drop
};

template<typename T>
constexpr T IfEqualThenDefault(const T& value, const T& compareTo, const T& defaultValue) {
    return value == compareTo ? defaultValue : value;
}

struct TResult : public std::pair<Ydb::StatusIds::StatusCode, TString>{
    TResult()
        : std::pair<Ydb::StatusIds::StatusCode, TString>(Ydb::StatusIds::SUCCESS, TString())
    {
    }

    TResult(Ydb::StatusIds::StatusCode YdbCode, TString&& errorMessage)
        : std::pair<Ydb::StatusIds::StatusCode, TString>(YdbCode, std::move(errorMessage))
    {
    }

    Ydb::StatusIds::StatusCode GetStatus() const {
        return first;
    }

    TString& GetErrorMessage() {
        return second;
    }

    operator bool() const {
        return GetStatus() == Ydb::StatusIds::SUCCESS;
    }
};

std::pair<TString, TString> GetWorkingDirAndName(const TString& fullName);

void CopyConfig(
    NKikimrSchemeOp::TPersQueueGroupDescription& destination,
    const NKikimrSchemeOp::TPersQueueGroupDescription& source
);

TString GetLocalClusterName(NPQ::NClusterTracker::TClustersList::TConstPtr ClustersList);
TResult ValidateLocalCluster(NPQ::NClusterTracker::TClustersList::TConstPtr ClustersList, const NKikimrPQ::TPQTabletConfig& config);

struct TClientServiceType {
    TString Name;
    ui32 MaxCount;
    TVector<TString> PasswordHashes;
};
using TClientServiceTypes = std::map<TString, TClientServiceType>;
TClientServiceTypes GetSupportedClientServiceTypes();

TResult ValidatePartitionStrategy(const NKikimrPQ::TPQTabletConfig& config);
TResult ValidateConfig(
    const NKikimrPQ::TPQTabletConfig& config,
    const EOperation operation
);

TResult ValidateConsumersConfig(
    const NKikimrPQ::TPQTabletConfig& config,
    const EOperation operation
);
TResult ValidateDuration(const google::protobuf::Duration& duration, const TString& name);

ui32 ConvertDurationToMs32(const google::protobuf::Duration& duration);

std::expected<TDuration, TString> ConvertPositiveDuration(const google::protobuf::Duration& duration);
std::expected<i32, TString> CheckRetentionPeriod(i64 seconds);
std::expected<std::optional<TDuration>, TResult> ConvertConsumerAvailabilityPeriod(
    const google::protobuf::Duration& duration,
    std::string_view consumerName
);

TResult FillMeteringMode(
    NKikimrPQ::TPQTabletConfig& config,
    Ydb::Topic::MeteringMode mode,
    EOperation operation
);

TResult ProcessTopicAttributes(
    const ::google::protobuf::Map<TProtoStringType, TProtoStringType>& attributes,
    NKikimrSchemeOp::TPersQueueGroupDescription* config,
    const EOperation operation,
    const bool topicsAreFirstClassCitizen,
    NGRpcProxy::V1::TConsumersAdvancedMonitoringSettings& consumersAdvancedMonitoringSettings // out parameter
);

TResult AddConsumer(
    NKikimrPQ::TPQTabletConfig* config,
    const Ydb::Topic::Consumer& consumerConfig,
    const TClientServiceTypes& supportedClientServiceTypes,
    const bool checkServiceType,
    NGRpcProxy::V1::TConsumersAdvancedMonitoringSettings* consumersAdvancedMonitoringSettings
);
TResult ProcessAlterConsumer(
    Ydb::Topic::Consumer& consumer,
    const Ydb::Topic::AlterConsumer& alter
);

TResult ApplyChangesInt(
    const Ydb::Topic::AlterTopicRequest& request,
    NKikimrSchemeOp::TPersQueueGroupDescription& config,
    bool isCdcStream
);

TResult ProcessConsumerType(
    NKikimrPQ::TPQTabletConfig::TConsumer* consumer,
    const auto& consumerConfig
) {
    if (consumerConfig.has_shared_consumer_type()) {
        if (!AppData()->FeatureFlags.GetEnableTopicMessageLevelParallelism()) {
            return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "shared consumers are disabled"};
        }
        consumer->SetType(::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);

        const auto& type = consumerConfig.shared_consumer_type();
        const auto& deadLetterPolicy = type.dead_letter_policy();

        consumer->SetKeepMessageOrder(type.keep_messages_order());

        if (type.has_default_processing_timeout()) {
            auto defaultProcessingTimeout = type.default_processing_timeout();
            if (auto r = ValidateDuration(defaultProcessingTimeout, "default_processing_timeout"); !r) {
                return r;
            }
            consumer->SetDefaultProcessingTimeoutSeconds(defaultProcessingTimeout.seconds());
        }

        consumer->SetDeadLetterPolicyEnabled(deadLetterPolicy.enabled());
        if (deadLetterPolicy.has_condition()) {
            consumer->SetMaxProcessingAttempts(deadLetterPolicy.condition().max_processing_attempts());
        }

        if (type.has_receive_message_delay()) {
            auto delayMessageTime = type.receive_message_delay();
            if (auto r = ValidateDuration(delayMessageTime, "receive_message_delay"); !r) {
                return r;
            }
            consumer->SetDefaultDelayMessageTimeMs(ConvertDurationToMs32(delayMessageTime));
        }

        if (type.has_receive_message_wait_time()) {
            auto receiveMessageWaitTime = type.receive_message_wait_time();
            if (auto r = ValidateDuration(receiveMessageWaitTime, "receive_message_wait_time"); !r) {
                return r;
            }
            consumer->SetDefaultReceiveMessageWaitTimeMs(ConvertDurationToMs32(receiveMessageWaitTime));
        }

        if (deadLetterPolicy.has_move_action()) {
            consumer->SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
            consumer->SetDeadLetterQueue(deadLetterPolicy.move_action().dead_letter_queue());
        } else if (deadLetterPolicy.has_delete_action()) {
            consumer->SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE);
        } else {
            consumer->SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_UNSPECIFIED);
        }
    } else {
        consumer->SetType(::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING);
    }

    return {};
}

}
