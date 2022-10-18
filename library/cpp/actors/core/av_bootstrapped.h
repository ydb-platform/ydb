#pragma once
#include "actor_virtual.h"

namespace NActors {

class TEventForStart;

class TActorAutoStart: public IActorVirtual {
protected:
    virtual void DoOnStart(const TActorId& senderActorId, const ::NActors::TActorContext& ctx) = 0;
    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override;
public:
    void ProcessEvent(TEventForStart* ev, TAutoPtr<IEventHandle>& handle, const NActors::TActorContext& ctx);

    TActorAutoStart() {
    }
};
}
