#pragma once

#include <util/datetime/base.h>

#include <ydb/core/protos/kqp.pb.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NKqp {

class TRetryPolicyState {
public:
    TRetryPolicyState() = default;

    TRetryPolicyState(ui64 retryCount, TInstant retryCounterUpdatedAt, double retryRate);

    TRetryPolicyState(const NKikimrKqp::TScriptExecutionRetryState& retryState);

    void SaveToProto(NKikimrKqp::TScriptExecutionRetryState& retryState) const;

    ui64 RetryCount = 0;
    TInstant RetryCounterUpdatedAt;
    double RetryRate = 0.0;
};

class TRetryPolicyItem {
public:
    struct TRetryResult {
        bool Retry = false;
        TDuration Backoff;
        TString LastError;
    };

    class IPolicy {
    public:
        using TPtr = std::shared_ptr<IPolicy>;

        virtual ~IPolicy() = default;

        virtual bool IsInitialized() const = 0;

        virtual TRetryResult Update(TInstant startedAt, TInstant lastSeenAt, TInstant now, TRetryPolicyState& state) const = 0;
    };

    TRetryPolicyItem();

    TRetryPolicyItem(ui64 retryCount, ui64 retryLimit, TDuration retryPeriod, TDuration backoffPeriod);

    explicit TRetryPolicyItem(IPolicy::TPtr policy);

    static std::optional<TRetryPolicyItem> FromProto(Ydb::StatusIds::StatusCode status, const NKikimrKqp::TScriptExecutionRetryState& mapping);

    IPolicy::TPtr Policy;
    bool PolicyInitialized = false;
};

class TRetryLimiter final : public TRetryPolicyState {
    using TBase = TRetryPolicyState;

public:
    using TBase::TBase;

    void Assign(ui64 retryCount, TInstant retryCounterUpdatedAt, double retryRate);

    bool UpdateOnRetry(TInstant lastSeenAt, const TRetryPolicyItem& policy, TInstant now = Now());

    bool UpdateOnRetry(TInstant startedAt, TInstant lastSeenAt, const TRetryPolicyItem& policy, TInstant now = Now());

    TDuration Backoff;
    TString LastError;
};

}  // namespace NKikimr::NKqp
