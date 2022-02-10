#pragma once

#include "defs.h"
#include "message.h"

#include <util/generic/ptr.h>

namespace NBus {
    struct TBusClientConnection {
        /// if you want to open connection early
        virtual void OpenConnection() = 0;

        /// Send message to the destination
        /// If addr is set then use it as destination.
        /// Takes ownership of addr (see ClearState method).
        virtual EMessageStatus SendMessage(TBusMessage* pMes, bool wait = false) = 0;

        virtual EMessageStatus SendMessageOneWay(TBusMessage* pMes, bool wait = false) = 0;

        /// Like SendMessage but cares about message
        template <typename T /* <: TBusMessage */>
        EMessageStatus SendMessageAutoPtr(const TAutoPtr<T>& mes, bool wait = false) {
            EMessageStatus status = SendMessage(mes.Get(), wait);
            if (status == MESSAGE_OK)
                Y_UNUSED(mes.Release());
            return status;
        }

        /// Like SendMessageOneWay but cares about message
        template <typename T /* <: TBusMessage */>
        EMessageStatus SendMessageOneWayAutoPtr(const TAutoPtr<T>& mes, bool wait = false) {
            EMessageStatus status = SendMessageOneWay(mes.Get(), wait);
            if (status == MESSAGE_OK)
                Y_UNUSED(mes.Release());
            return status;
        }

        EMessageStatus SendMessageMove(TBusMessageAutoPtr message, bool wait = false) {
            return SendMessageAutoPtr(message, wait);
        }

        EMessageStatus SendMessageOneWayMove(TBusMessageAutoPtr message, bool wait = false) {
            return SendMessageOneWayAutoPtr(message, wait);
        }

        // TODO: implement similar one-way methods

        virtual ~TBusClientConnection() {
        }
    };

    namespace NPrivate {
        struct TBusClientConnectionPtrOps {
            static void Ref(TBusClientConnection*);
            static void UnRef(TBusClientConnection*);
        };
    }

    using TBusClientConnectionPtr = TIntrusivePtr<TBusClientConnection, NPrivate::TBusClientConnectionPtrOps>;

}
