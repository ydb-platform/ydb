#pragma once

#include "defs.h"
#include <ydb/library/actors/util/ticket_lock.h>
#include <ydb/library/actors/util/queue_oneone_inplace.h>

namespace NActors {
    // dead-simple one-one queue, based on serializability guaranties of x64 and ticket lock to ensure writer unicity.
    template <typename T, ui32 TSize>
    class TSimpleMailboxQueue {
        TOneOneQueueInplace<T, TSize> Queue;
        TTicketLock Lock;

    public:
        ui32 Push(T x) noexcept {
            const ui32 spins = Lock.Acquire();
            Queue.Push(x);
            Lock.Release();
            return spins;
        }

        T Head() {
            return Queue.Head();
        }

        T Pop() {
            return Queue.Pop();
        }

        typename TOneOneQueueInplace<T, TSize>::TReadIterator ReadIterator() {
            return Queue.Iterator();
        }
    };
}
