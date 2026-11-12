#include "cq_actor.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/library/actors/interconnect/logging/logging.h>
#include <ydb/library/actors/interconnect/rdma/events.h>

using namespace NActors;

namespace NInterconnect::NRdma {

class TCqActorDummy: public TInterconnectLoggingBase, public TActorBootstrapped<TCqActorDummy> {

public:
    TCqActorDummy()
        : TInterconnectLoggingBase("CqActor")
    {}

    static constexpr IActor::EActivityType ActorActivityType() {
        return IActor::EActivityType::INTERCONNECT_RDMA_CQ;
    }

    void Bootstrap() {
        Become(&TCqActorDummy::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvGetCqHandle, Handle);
    )

    void Handle(TEvGetCqHandle::TPtr& ev) {
        ev->Get()->CqPtr = nullptr;
        TlsActivationContext->Send(ev->Sender, std::unique_ptr<TEvGetCqHandle>(ev->Release().Release()));
    }
};

NActors::IActor* CreateCqActor(int /*maxCqe*/, int /*maxWr*/, NMonitoring::TDynamicCounters* /*counters*/) {
    return new TCqActorDummy();
}

NActors::TActorId MakeCqActorId() {
    char x[12] = {'I', 'C', 'R', 'D', 'M', 'A', 'C', 'Q', '\xDE', '\xAD', '\xBE', '\xEF'};
    return TActorId(0, TStringBuf(std::begin(x), std::end(x)));
}

}
