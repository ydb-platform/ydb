#include "config.h"
#include "util.h"

namespace NYq {

namespace {

YandexQuery::ConnectionSetting::ConnectionCase GetConnectionType(const TString& typeStr) {
    YandexQuery::ConnectionSetting::ConnectionType type = YandexQuery::ConnectionSetting::CONNECTION_TYPE_UNSPECIFIED;
    YandexQuery::ConnectionSetting::ConnectionType_Parse(typeStr, &type);
    return static_cast<YandexQuery::ConnectionSetting::ConnectionCase>(type);
}

YandexQuery::BindingSetting::BindingCase GetBindingType(const TString& typeStr) {
    YandexQuery::BindingSetting::BindingType type = YandexQuery::BindingSetting::BINDING_TYPE_UNSPECIFIED;
    YandexQuery::BindingSetting::BindingType_Parse(typeStr, &type);
    return static_cast<YandexQuery::BindingSetting::BindingCase>(type);
}

}

TControlPlaneStorageConfig::TControlPlaneStorageConfig(const NConfig::TControlPlaneStorageConfig& config, const NConfig::TCommonConfig& common)
    : Proto(FillDefaultParameters(config))
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

    for (const auto& mapping : Proto.GetRetryPolicyMapping()) {
        auto& retryPolicy = mapping.GetPolicy();
        auto retryCount = retryPolicy.GetRetryCount();
        auto retryPeriod = GetDuration(retryPolicy.GetRetryPeriod(), TDuration::Hours(1));
        auto backoffPeriod = GetDuration(retryPolicy.GetBackoffPeriod(), TDuration::Zero());
        for (const auto statusCode: mapping.GetStatusCode()) {
            RetryPolicies.emplace(statusCode, TRetryPolicyItem(retryCount, retryPeriod, backoffPeriod));
        }
    }

    if (Proto.HasTaskLeaseRetryPolicy()) {
        TaskLeaseRetryPolicy.RetryCount = Proto.GetTaskLeaseRetryPolicy().GetRetryCount();
        TaskLeaseRetryPolicy.RetryPeriod = GetDuration(Proto.GetTaskLeaseRetryPolicy().GetRetryPeriod(), TDuration::Days(1));
    }
}

} // NYq
