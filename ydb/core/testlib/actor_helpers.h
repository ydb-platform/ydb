#pragma once
#include "defs.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/library/actors/core/mailbox.h>
#include <ydb/library/actors/core/executor_thread.h>

namespace NKikimr {

struct TActorSystemStub {
    std::unique_ptr<NActors::TActorSystem> System;
    std::unique_ptr<NActors::TMailbox> Mailbox;
    std::unique_ptr<NActors::TExecutorThread> ExecutorThread;
    NActors::TActorId SelfID;
    std::unique_ptr<NActors::TActorContext> Ctx;
    NActors::TActivationContext* PrevCtx;
    TAppData AppData;

    TActorSystemStub(std::shared_ptr<IRcBufAllocator> alloc = {});
    ~TActorSystemStub();
};

}
