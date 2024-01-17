#pragma once
#include <ydb/core/fq/libs/quota_manager/events/events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NFq {

class TQuotaServiceFakeActor: public NActors::TActor<TQuotaServiceFakeActor> {
    TQuotaMap Quotas;

public:
    TQuotaServiceFakeActor(const TQuotaMap& map = {{ QUOTA_CPU_PERCENT_LIMIT, 3200 }})
        : TActor<TQuotaServiceFakeActor>(&TQuotaServiceFakeActor::StateFunc)
        , Quotas(map)
    {
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvQuotaService::TQuotaGetRequest, Handle);
    );

    void Handle(TEvQuotaService::TQuotaGetRequest::TPtr& ev);
};

} // namespace NFq
