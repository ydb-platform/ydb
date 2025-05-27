#include "actor.h"

namespace NKikimr::NOlap::NDataAccessorControl {

void TActor::Handle(TEvAskServiceDataAccessors::TPtr& ev) {
    Manager->AskData(ev->Get()->GetRequest());
}

void TActor::Bootstrap() {
    AccessorsCallback = std::make_shared<TActorAccessorsCallback>(SelfId());
    Manager = std::make_shared<TLocalManager>(AccessorsCallback);
    Become(&TThis::StateWait);
}

}
