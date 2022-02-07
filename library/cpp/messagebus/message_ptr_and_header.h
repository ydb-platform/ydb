#pragma once

#include "message.h"
#include "nondestroying_holder.h"

#include <util/generic/noncopyable.h>
#include <util/generic/utility.h>

namespace NBus {
    namespace NPrivate {
        struct TBusMessagePtrAndHeader : TNonCopyable {
            TNonDestroyingHolder<TBusMessage> MessagePtr;
            TBusHeader Header;
            ui32 LocalFlags;

            TBusMessagePtrAndHeader()
                : LocalFlags()
            {
            }

            explicit TBusMessagePtrAndHeader(TBusMessage* messagePtr)
                : MessagePtr(messagePtr)
                , Header(*MessagePtr->GetHeader())
                , LocalFlags(MessagePtr->LocalFlags)
            {
            }

            void Swap(TBusMessagePtrAndHeader& that) {
                DoSwap(MessagePtr, that.MessagePtr);
                DoSwap(Header, that.Header);
                DoSwap(LocalFlags, that.LocalFlags);
            }
        };

    }
}
