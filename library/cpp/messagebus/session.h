#pragma once

#include "connection.h"
#include "defs.h"
#include "handler.h"
#include "message.h"
#include "netaddr.h"
#include "network.h"
#include "session_config.h"
#include "misc/weak_ptr.h"

#include <library/cpp/messagebus/monitoring/mon_proto.pb.h>

#include <util/generic/array_ref.h>
#include <util/generic/ptr.h>

namespace NBus {
    template <typename TBusSessionSubclass>
    class TBusSessionPtr;
    using TBusClientSessionPtr = TBusSessionPtr<TBusClientSession>;
    using TBusServerSessionPtr = TBusSessionPtr<TBusServerSession>;

    ///////////////////////////////////////////////////////////////////
    /// \brief Interface of session object.

    /// Each client and server
    /// should instantiate session object to be able to communicate via bus
    /// client: sess = queue->CreateSource(protocol, handler);
    /// server: sess = queue->CreateDestination(protocol, handler);

    class TBusSession: public TWeakRefCounted<TBusSession> {
    public:
        size_t GetInFlight(const TNetAddr& addr) const;
        size_t GetConnectSyscallsNumForTest(const TNetAddr& addr) const;

        virtual void GetInFlightBulk(TArrayRef<const TNetAddr> addrs, TArrayRef<size_t> results) const = 0;
        virtual void GetConnectSyscallsNumBulkForTest(TArrayRef<const TNetAddr> addrs, TArrayRef<size_t> results) const = 0;

        virtual int GetInFlight() const noexcept = 0;
        /// monitoring status of current session and it's connections
        virtual TString GetStatus(ui16 flags = YBUS_STATUS_CONNS) = 0;
        virtual TConnectionStatusMonRecord GetStatusProtobuf() = 0;
        virtual NPrivate::TSessionDumpStatus GetStatusRecordInternal() = 0;
        virtual TString GetStatusSingleLine() = 0;
        /// return session config
        virtual const TBusSessionConfig* GetConfig() const noexcept = 0;
        /// return session protocol
        virtual const TBusProtocol* GetProto() const noexcept = 0;
        virtual TBusMessageQueue* GetQueue() const noexcept = 0;

        /// registers external session on host:port with locator service
        int RegisterService(const char* hostname, TBusKey start = YBUS_KEYMIN, TBusKey end = YBUS_KEYMAX, EIpVersion ipVersion = EIP_VERSION_4);

    protected:
        TBusSession();

    public:
        virtual TString GetNameInternal() = 0;

        virtual void Shutdown() = 0;

        virtual ~TBusSession();
    };

    struct TBusClientSession: public virtual TBusSession {
        typedef ::NBus::NPrivate::TRemoteClientSession TImpl;

        static TBusClientSessionPtr Create(
            TBusProtocol* proto,
            IBusClientHandler* handler,
            const TBusClientSessionConfig& config,
            TBusMessageQueuePtr queue);

        virtual TBusClientConnectionPtr GetConnection(const TNetAddr&) = 0;

        /// if you want to open connection early
        virtual void OpenConnection(const TNetAddr&) = 0;

        /// Send message to the destination
        /// If addr is set then use it as destination.
        /// Takes ownership of addr (see ClearState method).
        virtual EMessageStatus SendMessage(TBusMessage* pMes, const TNetAddr* addr = nullptr, bool wait = false) = 0;

        virtual EMessageStatus SendMessageOneWay(TBusMessage* pMes, const TNetAddr* addr = nullptr, bool wait = false) = 0;

        /// Like SendMessage but cares about message
        template <typename T /* <: TBusMessage */>
        EMessageStatus SendMessageAutoPtr(const TAutoPtr<T>& mes, const TNetAddr* addr = nullptr, bool wait = false) {
            EMessageStatus status = SendMessage(mes.Get(), addr, wait);
            if (status == MESSAGE_OK)
                Y_UNUSED(mes.Release());
            return status;
        }

        /// Like SendMessageOneWay but cares about message
        template <typename T /* <: TBusMessage */>
        EMessageStatus SendMessageOneWayAutoPtr(const TAutoPtr<T>& mes, const TNetAddr* addr = nullptr, bool wait = false) {
            EMessageStatus status = SendMessageOneWay(mes.Get(), addr, wait);
            if (status == MESSAGE_OK)
                Y_UNUSED(mes.Release());
            return status;
        }

        EMessageStatus SendMessageMove(TBusMessageAutoPtr message, const TNetAddr* addr = nullptr, bool wait = false) {
            return SendMessageAutoPtr(message, addr, wait);
        }

        EMessageStatus SendMessageOneWayMove(TBusMessageAutoPtr message, const TNetAddr* addr = nullptr, bool wait = false) {
            return SendMessageOneWayAutoPtr(message, addr, wait);
        }

        // TODO: implement similar one-way methods
    };

    struct TBusServerSession: public virtual TBusSession {
        typedef ::NBus::NPrivate::TRemoteServerSession TImpl;

        static TBusServerSessionPtr Create(
            TBusProtocol* proto,
            IBusServerHandler* handler,
            const TBusServerSessionConfig& config,
            TBusMessageQueuePtr queue);

        static TBusServerSessionPtr Create(
            TBusProtocol* proto,
            IBusServerHandler* handler,
            const TBusServerSessionConfig& config,
            TBusMessageQueuePtr queue,
            const TVector<TBindResult>& bindTo);

        // TODO: make parameter non-const
        virtual EMessageStatus SendReply(const TBusIdentity& ident, TBusMessage* pRep) = 0;

        // TODO: make parameter non-const
        virtual EMessageStatus ForgetRequest(const TBusIdentity& ident) = 0;

        template <typename U /* <: TBusMessage */>
        EMessageStatus SendReplyAutoPtr(TBusIdentity& ident, TAutoPtr<U>& resp) {
            EMessageStatus status = SendReply(const_cast<const TBusIdentity&>(ident), resp.Get());
            if (status == MESSAGE_OK) {
                Y_UNUSED(resp.Release());
            }
            return status;
        }

        EMessageStatus SendReplyMove(TBusIdentity& ident, TBusMessageAutoPtr resp) {
            return SendReplyAutoPtr(ident, resp);
        }

        /// Pause input from the network.
        /// It is valid to call this method in parallel.
        /// TODO: pull this method up to TBusSession.
        virtual void PauseInput(bool pause) = 0;
        virtual unsigned GetActualListenPort() = 0;
    };

    namespace NPrivate {
        template <typename TBusSessionSubclass>
        class TBusOwnerSessionPtr: public TAtomicRefCount<TBusOwnerSessionPtr<TBusSessionSubclass>> {
        private:
            TIntrusivePtr<TBusSessionSubclass> Ptr;

        public:
            TBusOwnerSessionPtr(TBusSessionSubclass* session)
                : Ptr(session)
            {
                Y_ASSERT(!!Ptr);
            }

            ~TBusOwnerSessionPtr() {
                Ptr->Shutdown();
            }

            TBusSessionSubclass* Get() const {
                return reinterpret_cast<TBusSessionSubclass*>(Ptr.Get());
            }
        };

    }

    template <typename TBusSessionSubclass>
    class TBusSessionPtr {
    private:
        TIntrusivePtr<NPrivate::TBusOwnerSessionPtr<TBusSessionSubclass>> SmartPtr;
        TBusSessionSubclass* Ptr;

    public:
        TBusSessionPtr()
            : Ptr()
        {
        }
        TBusSessionPtr(TBusSessionSubclass* session)
            : SmartPtr(!!session ? new NPrivate::TBusOwnerSessionPtr<TBusSessionSubclass>(session) : nullptr)
            , Ptr(session)
        {
        }

        TBusSessionSubclass* Get() const {
            return Ptr;
        }
        operator TBusSessionSubclass*() {
            return Get();
        }
        TBusSessionSubclass& operator*() const {
            return *Get();
        }
        TBusSessionSubclass* operator->() const {
            return Get();
        }

        bool operator!() const {
            return !Ptr;
        }

        void Swap(TBusSessionPtr& t) noexcept {
            DoSwap(SmartPtr, t.SmartPtr);
            DoSwap(Ptr, t.Ptr);
        }

        void Drop() {
            TBusSessionPtr().Swap(*this);
        }
    };

}
