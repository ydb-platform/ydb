#pragma once

#include "defs.h"
#include "incrhuge_keeper_common.h"
#include "incrhuge.h"

#include <util/generic/queue.h>

namespace NKikimr {
    namespace NIncrHuge {

        class TReader
            : public TKeeperComponentBase
        {
            struct TReadQueueItem {
                ui8 Owner;
                TIncrHugeBlobId Id;
                ui32 Offset;
                ui32 Size;
                TActorId Sender;
                ui64 Cookie;
            };
            using TReadQueue = TQueue<TReadQueueItem>;
            TReadQueue ReadQueue;

        public:
            TReader(TKeeper& keeper);
            ~TReader();

            // register read request; it is executed immediately or placed in queue depending on settings of keeper
            void HandleRead(TEvIncrHugeRead::TPtr& ev, const TActorContext& ctx);

        private:
            // process read queue item; return true if item is processed and can be deleted from queues; otherwise it
            // returns false
            bool ProcessReadItem(TReadQueueItem& item, const TActorContext& ctx);

            // process read queue items if possible
            void ProcessReadQueue(const TActorContext& ctx);
        };

    } // NIncrHuge
} // NKikimr
