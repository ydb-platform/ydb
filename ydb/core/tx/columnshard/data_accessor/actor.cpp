#include "actor.h"

namespace NKikimr::NOlap::NDataAccessorControl {

void TActor::Handle(TEvAskDataAccessors::TPtr& ev) {
    const std::shared_ptr<TDataAccessorsRequest> request = ev->Get()->GetRequest();
    for (auto&& i : request->GetPathIds()) {
        auto it = Controllers.find(i);
        if (it == Controllers.end()) {
            request->AddData(i, TConclusionStatus::Fail("path id is not actual: " + ::ToString(i)));
        } else {
            it->second->AskData(request);
        }
    }
}

}
