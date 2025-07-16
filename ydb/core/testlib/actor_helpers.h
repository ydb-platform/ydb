#pragma once
#include "defs.h"

#include <ydb/library/actors/core/mailbox.h>
#include <ydb/library/actors/core/executor_thread.h>

namespace NKikimr {

struct TActorSystemStub {
    THolder<NActors::TActorSystem> System;
    THolder<NActors::TMailbox> Mailbox;
    THolder<NActors::TExecutorThread> ExecutorThread;
    NActors::TActorId SelfID;
    THolder<NActors::TActorContext> Ctx;
    NActors::TActivationContext* PrevCtx;

    TActorSystemStub(TRcBufAllocator alloc = {}); 
    ~TActorSystemStub();
};

}
