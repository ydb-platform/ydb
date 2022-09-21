#pragma once

#include <ydb/core/sys_view/common/events.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {
namespace NGRpcService {

class IGRpcProxyCounters : public virtual TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IGRpcProxyCounters>;

    virtual void IncDatabaseAccessDenyCounter() = 0;
    virtual void IncDatabaseSchemeErrorCounter() = 0;
    virtual void IncDatabaseUnavailableCounter() = 0;
    virtual void IncDatabaseRateLimitedCounter() = 0;

    virtual void AddConsumedRequestUnits(ui64 requestUnits) = 0;
    virtual void ReportThrottleDelay(const TDuration& duration) = 0;

    virtual void UseDatabase(const TString& database) { Y_UNUSED(database); }
};

IGRpcProxyCounters::TPtr WrapGRpcProxyDbCounters(IGRpcProxyCounters::TPtr common);

IGRpcProxyCounters::TPtr CreateGRpcProxyCounters(::NMonitoring::TDynamicCounterPtr appCounters);

TIntrusivePtr<NSysView::IDbCounters> CreateGRpcProxyDbCounters(
    ::NMonitoring::TDynamicCounterPtr externalGroup,
    ::NMonitoring::TDynamicCounterPtr internalGroup);

void InitializeGRpcProxyDbCountersRegistry(TActorSystem* actorSystem);

}
}
