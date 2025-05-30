#include "node_actor.h"

namespace NKikimr::NOlap::NDataAccessorControl {

NActors::IActor* TNodeActor::CreateActor() {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("IURII", "CREATE");
    return new TNodeActor();
}

void TNodeActor::Handle(TEvAskServiceDataAccessors::TPtr& ev) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("IURII", "ASK")("Tid", ev->Get()->GetTabletId());
    Manager->AskData(ev->Get()->GetTabletId(), ev->Get()->GetRequest());
}

void TNodeActor::Bootstrap() {
    AccessorsCallback = std::make_shared<TActorAccessorsCallback>(SelfId());
    Manager = std::make_shared<TLocalManager>(AccessorsCallback);
    Become(&TThis::StateWait);
}

}
