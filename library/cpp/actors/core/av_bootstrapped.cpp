#include "av_bootstrapped.h"

namespace NActors {

class TEventForStart: public TEventLocalForActor<TEventForStart, TActorAutoStart> {
public:
};

TAutoPtr<NActors::IEventHandle> TActorAutoStart::AfterRegister(const TActorId& self, const TActorId& parentId) {
    return new IEventHandle(self, parentId, new TEventForStart, 0);
}

void TActorAutoStart::ProcessEvent(TEventContext<TEventForStart>& ev) {
    DoOnStart(ev.GetHandle().Sender);
}

}
