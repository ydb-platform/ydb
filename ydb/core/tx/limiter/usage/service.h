#pragma once
#include "config.h"
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/core/tx/limiter/service/service.h>
#include <ydb/core/tx/limiter/usage/events.h>

namespace NKikimr::NLimiter {

template <class TLimiterPolicy>
class TServiceOperatorImpl {
private:
    using TSelf = TServiceOperatorImpl<TLimiterPolicy>;
    std::atomic<bool> IsEnabledFlag = false;
    static void Register(const TConfig& serviceConfig) {
        Singleton<TSelf>()->IsEnabledFlag = serviceConfig.IsEnabled() && serviceConfig.GetLimit();
    }
    static const TString& GetLimiterName() {
        Y_ABORT_UNLESS(TLimiterPolicy::Name.size() == 4);
        return TLimiterPolicy::Name;
    }
public:
    static bool AskResource(const std::shared_ptr<IResourceRequest>& request) {
        AFL_VERIFY(!!request);
        auto& context = NActors::TActorContext::AsActorContext();
        const NActors::TActorId& selfId = context.SelfID;
        if (TSelf::IsEnabled()) {
            context.Send(MakeServiceId(selfId.NodeId()), new TEvExternal::TEvAskResource(request));
            return true;
        } else {
            request->OnResourceAllocated();
            return false;
        }
    }
    static bool IsEnabled() {
        return Singleton<TSelf>()->IsEnabledFlag;
    }
    static NActors::TActorId MakeServiceId(const ui32 nodeId) {
        return NActors::TActorId(nodeId, "SrvcLimt" + GetLimiterName());
    }
    static NActors::IActor* CreateService(const TConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals) {
        Register(config);
        return new TLimiterActor(config, GetLimiterName(), baseSignals);
    }

};

class TCompDiskLimiterPolicy {
public:
    static const inline TString Name = "CMPD";
    static const inline TDuration DefaultPeriod = TDuration::Seconds(1);
    static const inline ui64 DefaultLimit = (ui64)256 * 1024 * 1024;
    static const inline bool DefaultEnabled = true;
};

using TCompDiskOperator = TServiceOperatorImpl<TCompDiskLimiterPolicy>;

}
