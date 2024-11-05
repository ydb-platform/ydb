#include "config.h"
#include "util.h"

namespace NFq {

namespace {

FederatedQuery::ConnectionSetting::ConnectionCase GetConnectionType(const TString& typeStr) {
    FederatedQuery::ConnectionSetting::ConnectionType type = FederatedQuery::ConnectionSetting::CONNECTION_TYPE_UNSPECIFIED;
    FederatedQuery::ConnectionSetting::ConnectionType_Parse(typeStr, &type);
    return static_cast<FederatedQuery::ConnectionSetting::ConnectionCase>(type);
}

FederatedQuery::BindingSetting::BindingCase GetBindingType(const TString& typeStr) {
    FederatedQuery::BindingSetting::BindingType type = FederatedQuery::BindingSetting::BINDING_TYPE_UNSPECIFIED;
    FederatedQuery::BindingSetting::BindingType_Parse(typeStr, &type);
    return static_cast<FederatedQuery::BindingSetting::BindingCase>(type);
}

}

TControlPlaneStorageConfig::TControlPlaneStorageConfig(const NConfig::TControlPlaneStorageConfig& config, const NYql::TS3GatewayConfig& s3Config, const NConfig::TCommonConfig& common, const NConfig::TComputeConfig& computeConfigProto)
    : Proto(FillDefaultParameters(config))
    , ComputeConfigProto(computeConfigProto)
    , IdsPrefix(common.GetIdsPrefix())
    , IdempotencyKeyTtl(GetDuration(Proto.GetIdempotencyKeysTtl(), TDuration::Minutes(10)))
    , AutomaticQueriesTtl(GetDuration(Proto.GetAutomaticQueriesTtl(), TDuration::Days(1)))
    , ResultSetsTtl(GetDuration(Proto.GetResultSetsTtl(), TDuration::Days(1)))
    , AnalyticsRetryCounterUpdateTime(GetDuration(Proto.GetAnalyticsRetryCounterUpdateTime(), TDuration::Days(1)))
    , StreamingRetryCounterUpdateTime(GetDuration(Proto.GetAnalyticsRetryCounterUpdateTime(), TDuration::Days(1)))
    , TaskLeaseTtl(GetDuration(Proto.GetTaskLeaseTtl(), TDuration::Seconds(30)))
    , QuotaTtl(GetDuration(Proto.GetQuotaTtl(), TDuration::Zero()))
    , MetricsTtl(GetDuration(Proto.GetMetricsTtl(), TDuration::Days(1)))
{
    for (const auto& availableConnection : Proto.GetAvailableConnection()) {
        AvailableConnections.insert(GetConnectionType(availableConnection));
    }

    for (const auto& availableBinding : Proto.GetAvailableBinding()) {
        AvailableBindings.insert(GetBindingType(availableBinding));
    }

    for (const auto& availableConnection : Proto.GetAvailableStreamingConnection()) {
        AvailableStreamingConnections.insert(GetConnectionType(availableConnection));
    }

    GeneratorPathsLimit =
        s3Config.HasGeneratorPathsLimit() ? s3Config.GetGeneratorPathsLimit() : 50'000;

    for (const auto& mapping : Proto.GetRetryPolicyMapping()) {
        auto& retryPolicy = mapping.GetPolicy();
        auto retryCount = retryPolicy.GetRetryCount();
        auto retryLimit = retryPolicy.GetRetryLimit();
        auto retryPeriod = GetDuration(retryPolicy.GetRetryPeriod(), TDuration::Hours(1));
        auto backoffPeriod = GetDuration(retryPolicy.GetBackoffPeriod(), TDuration::Zero());
        for (const auto statusCode: mapping.GetStatusCode()) {
            RetryPolicies.emplace(statusCode, TRetryPolicyItem(retryCount, retryLimit, retryPeriod, backoffPeriod));
        }
    }

    if (Proto.HasTaskLeaseRetryPolicy()) {
        TaskLeaseRetryPolicy.RetryCount = Proto.GetTaskLeaseRetryPolicy().GetRetryCount();
        TaskLeaseRetryPolicy.RetryPeriod = GetDuration(Proto.GetTaskLeaseRetryPolicy().GetRetryPeriod(), TDuration::Days(1));
    }
}

} // NFq
