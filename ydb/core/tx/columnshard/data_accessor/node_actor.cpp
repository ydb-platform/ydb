#include "node_actor.h"

namespace NKikimr::NOlap::NDataAccessorControl {

NActors::IActor* TNodeActor::CreateActor() {
    return new TNodeActor();
}

void TNodeActor::Handle(TEvAskServiceDataAccessors::TPtr& ev) {
    Manager->AskData(ev->Get()->GetRequest());
}

void TNodeActor::Bootstrap() {
    AccessorsCallback = std::make_shared<TActorAccessorsCallback>(SelfId());
    Manager = std::make_shared<TLocalManager>((NOlap::TTabletId)0, AccessorsCallback);
    Become(&TThis::StateWait);
}

}
