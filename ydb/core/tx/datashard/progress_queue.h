#pragma once
#include "defs.h"
#include <ydb/core/util/queue_oneone_inplace.h>

namespace NKikimr {

template <typename T, typename TDestruct, typename TEvent>
class TTxProgressQueue {
    bool HasInFly;
    TOneOneQueueInplace<T, 32> Queue;
public:
    TTxProgressQueue()
        : HasInFly(false)
    {}

    ~TTxProgressQueue() {
        while (T head = Queue.Pop())
            TDestruct::Destroy(head);
    }

    void Progress(T x, const TActorContext &ctx) {
        if (!HasInFly) {
            Y_DEBUG_ABORT_UNLESS(!Queue.Head());
            ctx.Send(ctx.SelfID, new TEvent(x));
            HasInFly = true;
        } else {
            Queue.Push(x);
        }
    }

    void Reset(const TActorContext &ctx) {
        Y_DEBUG_ABORT_UNLESS(HasInFly);
        if (T x = Queue.Pop())
            ctx.Send(ctx.SelfID, new TEvent(x));
        else
            HasInFly = false;
    }
};

template <typename TEvent>
class TTxProgressCountedScalarQueue {
    ui32 InFly;
public:
    TTxProgressCountedScalarQueue()
        : InFly(0)
    {}

    void Progress(const TActorContext &ctx) {
        if (++InFly == 1) {
            ctx.Send(ctx.SelfID, new TEvent());
        }
    }

    void Reset(const TActorContext &ctx) {
        Y_DEBUG_ABORT_UNLESS(InFly);
        if (--InFly) {
            ctx.Send(ctx.SelfID, new TEvent());
        }
    }
};

template <typename TEvent>
class TTxProgressIdempotentScalarQueue {
    bool HasInFly;
public:
    TTxProgressIdempotentScalarQueue()
        : HasInFly(false)
    {}

    void Progress(const TActorContext &ctx) {
        if (!HasInFly) {
            ctx.Send(ctx.SelfID, new TEvent());
            HasInFly = true;
        }
    }

    void Reset(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        Y_DEBUG_ABORT_UNLESS(HasInFly);
        HasInFly = false;
    }
};

template <typename TDelayedEvent>
class TTxProgressIdempotentScalarScheduleQueue {
    bool HasSchedule;
public:
    TTxProgressIdempotentScalarScheduleQueue()
        : HasSchedule(false)
    {}

    void Schedule(const TActorContext& ctx, TDuration delta) {
        if (!HasSchedule) {
            ctx.Schedule(delta, new TDelayedEvent());
            HasSchedule = true;
        }
    }

    void Reset(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        Y_DEBUG_ABORT_UNLESS(HasSchedule);
        HasSchedule = false;
    }
};

}
