#include "actor_helpers.h"

namespace NKikimr {

TActorSystemStub::TActorSystemStub()
    : AppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr)
{
    THolder<NActors::TActorSystemSetup> setup(new NActors::TActorSystemSetup);
    System.Reset(new NActors::TActorSystem(setup, &AppData));
    Mailbox.Reset(new NActors::TMailboxHeader(NActors::TMailboxType::Simple));
    ExecutorThread.Reset(new NActors::TExecutorThread(0, System.Get(), nullptr, nullptr, "thread"));
    Ctx.Reset(new NActors::TActorContext(*Mailbox, *ExecutorThread, GetCycleCountFast(), SelfID));
    PrevCtx = NActors::TlsActivationContext;
    NActors::TlsActivationContext = Ctx.Get();
}

TActorSystemStub::~TActorSystemStub() {
    NActors::TlsActivationContext = PrevCtx;
    PrevCtx = nullptr;
}

}
