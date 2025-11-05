#include "counters_info.h"

#include <ydb/core/base/defs.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/monlib/dynamic_counters/encode.h>

namespace NKikimr::NCountersInfo {

class TCountersInfoProviderService : public TActorBootstrapped<TCountersInfoProviderService> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::MONITORING_SERVICE; }

    TCountersInfoProviderService(NMonitoring::TDynamicCounterPtr counters)
    : Counters(counters)
    {
    }

    void Bootstrap() {
        Become(&TCountersInfoProviderService::StateWork);
    }

    void Handle(TEvCountersInfoRequest::TPtr &ev, const TActorContext &ctx) {
        TString nameLabel("sensor");
        TString out;
        TStringOutput oss(out);
        auto encoder = NMonitoring::CreateEncoder(&oss, NMonitoring::EFormat::JSON, nameLabel, {});

        Counters->Accept(TString(), TString(), *encoder);
        THolder<TEvCountersInfoResponse> response = MakeHolder<TEvCountersInfoResponse>();
        auto& record = response->Record;
        record.SetResponse(out);
        ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvCountersInfoRequest, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }

private:
    NMonitoring::TDynamicCounterPtr Counters;
};

IActor* CreateCountersInfoProviderService(::NMonitoring::TDynamicCounterPtr counters) {
    return new TCountersInfoProviderService(counters);
}
}
