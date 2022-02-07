#pragma once
#include "defs.h"

#include <library/cpp/actors/core/mailbox.h>
#include <library/cpp/actors/core/executor_thread.h>

namespace NKikimr {

struct TActorSystemStub {
    THolder<NActors::TActorSystem> System;
    THolder<NActors::TMailboxHeader> Mailbox;
    THolder<NActors::TExecutorThread> ExecutorThread;
    NActors::TActorId SelfID;
    THolder<NActors::TActorContext> Ctx;
    NActors::TActivationContext* PrevCtx;

    TActorSystemStub();
    ~TActorSystemStub();
};

}
