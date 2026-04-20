#pragma once

#include "schema.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
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

    const TString& GetErrorMessage() const {
        return second;
    }

    operator bool() const {
        return GetStatus() == Ydb::StatusIds::SUCCESS;
    }
};

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
    const TClientServiceTypes& supportedClientServiceTypes,
    const EOperation operation
);
TResult ValidateConsumersConfig(
    const NKikimrPQ::TPQTabletConfig& config,
    const TClientServiceTypes& supportedClientServiceTypes,
    const EOperation operation
);

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

TResult ProcessAddConsumer(
    NKikimrPQ::TPQTabletConfig* config,
    const Ydb::Topic::Consumer& consumerConfig,
    const TClientServiceTypes& supportedClientServiceTypes,
    const bool checkServiceType,
    NGRpcProxy::V1::TConsumersAdvancedMonitoringSettings* consumersAdvancedMonitoringSettings
);
TResult ProcessAlterConsumer(Ydb::Topic::Consumer& consumer, const Ydb::Topic::AlterConsumer& alter);

TResult ApplyChangesInt(
    const Ydb::Topic::AlterTopicRequest& request,
    NKikimrSchemeOp::TPersQueueGroupDescription& config,
    bool isCdcStream
);

}
