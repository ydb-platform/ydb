#include "actor.h"

namespace NKikimr::NOlap::NDataAccessorControl {

void TActor::Handle(TEvAskDataAccessors::TPtr& ev) {
    Manager.AskData(ev->Get()->GetRequest());
}

}
