#pragma once

#include <util/datetime/base.h>

#include <ydb/core/protos/kqp.pb.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NKqp {

class TRetryPolicyItem {
public:
    TRetryPolicyItem() = default;

    TRetryPolicyItem(ui64 retryCount, ui64 retryLimit, TDuration retryPeriod, TDuration backoffPeriod);

    static TRetryPolicyItem FromProto(Ydb::StatusIds::StatusCode status, const NKikimrKqp::TScriptExecutionRetryState& mapping);

    ui64 RetryCount = 0;
    ui64 RetryLimit = 0;
    TDuration RetryPeriod = TDuration::Seconds(1);
    TDuration BackoffPeriod;
};

class TRetryLimiter {
public:
    TRetryLimiter() = default;

    TRetryLimiter(ui64 retryCount, TInstant retryCounterUpdatedAt, double retryRate);

    void Assign(ui64 retryCount, TInstant retryCounterUpdatedAt, double retryRate);

    bool UpdateOnRetry(TInstant lastSeenAt, const TRetryPolicyItem& policy, TInstant now = Now());

    TDuration GetBackoff() const;

    ui64 RetryCount = 0;
    TInstant RetryCounterUpdatedAt;
    double RetryRate = 0.0;
    TDuration Backoff;
    TString LastError;
};

}  // namespace NKikimr::NKqp
