#include "etcd_lease.h"
#include "etcd_shared.h"
#include "etcd_events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NEtcd {

using namespace NActors;

namespace {

class TLeasingOffice : public TActorBootstrapped<TLeasingOffice> {
public:
    TLeasingOffice(TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
        : Counters(std::move(counters))
    {}

    void Bootstrap(const TActorContext&) {
        Become(&TThis::StateFunc);
        TSharedStuff::Get()->Watchtower = SelfId();
    }

private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TEvWakeup::EventType, Wakeup);
        }
    }
/*
    void Handle(TEvWatchRequest::TPtr& ev, const TActorContext& ctx) {
        ctx.RegisterWithSameMailbox(new TWatchman(ev->Get()->ReleaseStreamCtx(), ctx.SelfID));
    }
*/
    void Wakeup(const TActorContext&) {
    }

    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

};

}

NActors::IActor* CreateEtcdLeasingOffice(TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
    return new TLeasingOffice(std::move(counters));

}

}


