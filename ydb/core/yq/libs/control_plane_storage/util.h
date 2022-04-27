#pragma once

#include <ydb/core/yq/libs/control_plane_storage/events/events.h>

#include <util/datetime/base.h>

#include <contrib/libs/protobuf/src/google/protobuf/timestamp.pb.h>

#include <ydb/core/yq/libs/config/protos/control_plane_storage.pb.h>

namespace NYq {

class TRetryPolicyItem {
public:
    TRetryPolicyItem() = default;
    TRetryPolicyItem(ui64 retryCount, const TDuration& retryPeriod, const TDuration& backoffPeriod)
    : RetryCount(retryCount), RetryPeriod(retryPeriod), BackoffPeriod(backoffPeriod)
    { }
    ui64 RetryCount = 0;
    TDuration RetryPeriod = TDuration::Zero();
    TDuration BackoffPeriod = TDuration::Zero();
};

class TRetryLimiter {
public:
    TRetryLimiter() = default;
    TRetryLimiter(ui64 retryCount, const TInstant& retryCounterUpdatedAt, double retryRate);
    void Assign(ui64 retryCount, const TInstant& retryCounterUpdatedAt, double retryRate);
    bool UpdateOnRetry(const TInstant& lastSeenAt, const TRetryPolicyItem& policy, const TInstant now = Now());
    ui64 RetryCount = 0;
    TInstant RetryCounterUpdatedAt = TInstant::Zero();
    double RetryRate = 0.0;
};

bool IsTerminalStatus(YandexQuery::QueryMeta::ComputeStatus status);

TDuration GetDuration(const TString& value, const TDuration& defaultValue);

NConfig::TControlPlaneStorageConfig FillDefaultParameters(NConfig::TControlPlaneStorageConfig config);

template<std::size_t K, typename T, std::size_t N>
auto CreateArray(const T(&list)[N]) -> std::array<T, K> {
    static_assert(N == K, "not valid array size");
    std::array<T, K> result;
    std::copy(std::begin(list), std::end(list), std::begin(result));
    return result;
}

bool DoesPingTaskUpdateQueriesTable(const TEvControlPlaneStorage::TEvPingTaskRequest* request);

NYdb::TValue PackItemsToList(const TVector<NYdb::TValue>& items);

std::pair<TString, TString> SplitId(const TString& id, char delim = '-');

} // namespace NYq
