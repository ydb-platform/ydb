#pragma once
#include "actor_virtual.h"

namespace NActors {

class TEventForStart;

class TActorAutoStart: public IActor {
protected:
    virtual void DoOnStart(const TActorId& senderActorId) = 0;
    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override;
public:
    void ProcessEvent(TEventContext<TEventForStart>& ev);

    TActorAutoStart()
    {}
};
}
