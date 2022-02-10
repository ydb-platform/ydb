#pragma once

#include "handler.h" 
#include "local_flags.h" 
#include "session.h"

namespace NBus {
    template <typename U /* <: TBusMessage */>
    EMessageStatus TOnMessageContext::SendReplyAutoPtr(TAutoPtr<U>& response) {
        return Session->SendReplyAutoPtr(Ident, response);
    }

    inline EMessageStatus TOnMessageContext::SendReplyMove(TBusMessageAutoPtr response) {
        return SendReplyAutoPtr(response);
    }

    inline void TOnMessageContext::AckMessage(TBusIdentity& ident) {
        Y_VERIFY(Ident.LocalFlags == NPrivate::MESSAGE_IN_WORK);
        Y_VERIFY(ident.LocalFlags == 0);
        Ident.Swap(ident);
    }

}
