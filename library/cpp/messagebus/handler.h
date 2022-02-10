#pragma once

#include "defs.h"
#include "message.h"
#include "message_status.h"
#include "use_after_free_checker.h"
#include "use_count_checker.h"

#include <util/generic/noncopyable.h>

namespace NBus {
    /////////////////////////////////////////////////////////////////
    /// \brief Interface to message bus handler

    struct IBusErrorHandler {
        friend struct ::NBus::NPrivate::TBusSessionImpl;

    private:
        TUseAfterFreeChecker UseAfterFreeChecker;
        TUseCountChecker UseCountChecker;

    public:
        /// called when message or reply can't be delivered
        virtual void OnError(TAutoPtr<TBusMessage> pMessage, EMessageStatus status);

        virtual ~IBusErrorHandler() {
        }
    };

    class TClientConnectionEvent {
    public:
        enum EType {
            CONNECTED,
            DISCONNECTED,
        };

    private:
        EType Type;
        ui64 Id;
        TNetAddr Addr;

    public:
        TClientConnectionEvent(EType type, ui64 id, TNetAddr addr)
            : Type(type)
            , Id(id)
            , Addr(addr)
        {
        }

        EType GetType() const {
            return Type;
        }
        ui64 GetId() const {
            return Id;
        }
        TNetAddr GetAddr() const {
            return Addr;
        }
    };

    class TOnMessageContext : TNonCopyable {
    private:
        THolder<TBusMessage> Message;
        TBusIdentity Ident;
        // TODO: we don't need to store session, we have connection in ident
        TBusServerSession* Session;

    public:
        TOnMessageContext()
            : Session()
        {
        }
        TOnMessageContext(TAutoPtr<TBusMessage> message, TBusIdentity& ident, TBusServerSession* session)
            : Message(message)
            , Session(session)
        {
            Ident.Swap(ident);
        }

        bool IsInWork() const {
            return Ident.IsInWork();
        }

        bool operator!() const {
            return !IsInWork();
        }

        TBusMessage* GetMessage() {
            return Message.Get();
        }

        TBusMessage* ReleaseMessage() {
            return Message.Release();
        }

        TBusServerSession* GetSession() {
            return Session;
        }

        template <typename U /* <: TBusMessage */>
        EMessageStatus SendReplyAutoPtr(TAutoPtr<U>& rep);

        EMessageStatus SendReplyMove(TBusMessageAutoPtr response);

        void AckMessage(TBusIdentity& ident);

        void ForgetRequest();

        void Swap(TOnMessageContext& that) {
            DoSwap(Message, that.Message);
            Ident.Swap(that.Ident);
            DoSwap(Session, that.Session);
        }

        TNetAddr GetPeerAddrNetAddr() const;

        bool IsConnectionAlive() const;
    };

    struct IBusServerHandler: public IBusErrorHandler {
        virtual void OnMessage(TOnMessageContext& onMessage) = 0;
        /// called when reply has been sent from destination
        virtual void OnSent(TAutoPtr<TBusMessage> pMessage);
    };

    struct IBusClientHandler: public IBusErrorHandler {
        /// called on source when reply arrives from destination
        virtual void OnReply(TAutoPtr<TBusMessage> pMessage, TAutoPtr<TBusMessage> pReply) = 0;
        /// called when client side message has gone into wire, place to call AckMessage()
        virtual void OnMessageSent(TBusMessage* pMessage);
        virtual void OnMessageSentOneWay(TAutoPtr<TBusMessage> pMessage);
        virtual void OnClientConnectionEvent(const TClientConnectionEvent&);
    };

}
