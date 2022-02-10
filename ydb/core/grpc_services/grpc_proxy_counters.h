#pragma once
#include "defs.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <ydb/core/base/counters.h>

namespace NKikimr {
namespace NGRpcService {

class TGrpcProxyCounters : public TThrRefBase  {
public:
    using TPtr = TIntrusivePtr<TGrpcProxyCounters>;
    explicit TGrpcProxyCounters(const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters) {
        auto group = GetServiceCounters(counters, "grpc");

        DatabaseAccessDenyCounter_ = group->GetCounter("databaseAccessDeny", true);
        DatabaseSchemeErrorCounter_ = group->GetCounter("databaseSchemeError", true);
        DatabaseUnavailableCounter_ = group->GetCounter("databaseUnavailable", true);
        DatabaseRateLimitedCounter_ = group->GetCounter("databaseRateLimited", true);
    }

    void IncDatabaseAccessDenyCounter() {
        DatabaseAccessDenyCounter_->Inc();
    }

    void IncDatabaseSchemeErrorCounter() {
        DatabaseSchemeErrorCounter_->Inc();
    }

    void IncDatabaseUnavailableCounter() {
        DatabaseUnavailableCounter_->Inc();
    }

    void IncDatabaseRateLimitedCounter() {
        DatabaseRateLimitedCounter_->Inc();
    }

private:
    NMonitoring::TDynamicCounters::TCounterPtr DatabaseAccessDenyCounter_;
    NMonitoring::TDynamicCounters::TCounterPtr DatabaseSchemeErrorCounter_;
    NMonitoring::TDynamicCounters::TCounterPtr DatabaseUnavailableCounter_;
    NMonitoring::TDynamicCounters::TCounterPtr DatabaseRateLimitedCounter_;
};

}
}
