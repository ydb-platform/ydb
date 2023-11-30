
#include "pg_proxy.h"
#include "pg_connection.h"
#include "pg_listener.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/interconnect/poller_actor.h>

namespace NPG {

using namespace NActors;
using namespace NKikimr::NRawSocket;

class TPGProxy : public TActorBootstrapped<TPGProxy> {
public:
    TPGProxy()
    {
    }

    void Bootstrap() {
        Poller = Register(CreatePollerActor());
        Listener = Register(CreatePGListener(Poller, {}));
        Become(&TPGProxy::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
        }
    }

    TActorId Poller;
    TActorId Listener;
};

NActors::IActor* CreatePGProxy() {
    return new TPGProxy();
}

} // namespace NPG
