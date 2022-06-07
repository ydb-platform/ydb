#pragma once

/// Asynchronous Messaging Library implements framework for sending and
/// receiving messages between loosely connected processes.

#include "coreconn.h"
#include "defs.h"
#include "handler.h"
#include "handler_impl.h"
#include "local_flags.h"
#include "locator.h"
#include "message.h"
#include "message_status.h"
#include "network.h"
#include "queue_config.h"
#include "remote_connection_status.h"
#include "session.h"
#include "session_config.h"
#include "socket_addr.h"

#include <library/cpp/messagebus/actor/executor.h>
#include <library/cpp/messagebus/scheduler/scheduler.h>

#include <library/cpp/codecs/codecs.h>

#include <util/generic/array_ref.h>
#include <util/generic/buffer.h>
#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>
#include <util/stream/input.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/condvar.h>
#include <util/system/type_name.h>
#include <util/system/event.h>
#include <util/system/mutex.h>

namespace NBus {
    ////////////////////////////////////////////////////////
    /// \brief Common structure to store address information

    int CompareByHost(const IRemoteAddr& l, const IRemoteAddr& r) noexcept;
    bool operator<(const TNetAddr& a1, const TNetAddr& a2); // compare by addresses

    /////////////////////////////////////////////////////////////////////////
    /// \brief Handles routing and data encoding to/from wire

    /// Protocol is stateless threadsafe singleton object that
    /// encapsulates relationship between a message (TBusMessage) object
    /// and destination server. Protocol object is reponsible for serializing in-memory
    /// message and reply into the wire, retuning name of the service and resource
    /// distribution key for given protocol.

    /// Protocol object should transparently handle messages and replies.
    /// This is interface only class, actuall instances of the protocols
    /// should be created using templates inhereted from this base class.
    class TBusProtocol {
    private:
        TString ServiceName;
        int ServicePort;

    public:
        TBusProtocol(TBusService name = "UNKNOWN", int port = 0)
            : ServiceName(name)
            , ServicePort(port)
        {
        }

        /// returns service type for this protocol and message
        TBusService GetService() const {
            return ServiceName.data();
        }

        /// returns port number for destination session to open socket
        int GetPort() const {
            return ServicePort;
        }

        virtual ~TBusProtocol() {
        }

        /// \brief serialized protocol specific data into TBusData
        /// \note buffer passed to the function (data) is not empty, use append functions
        virtual void Serialize(const TBusMessage* mess, TBuffer& data) = 0;

        /// deserialized TBusData into new instance of the message
        virtual TAutoPtr<TBusMessage> Deserialize(ui16 messageType, TArrayRef<const char> payload) = 0;

        /// returns key for messages of this protocol
        virtual TBusKey GetKey(const TBusMessage*) {
            return YBUS_KEYMIN;
        }

        /// default implementation of routing policy to allow overrides
        virtual EMessageStatus GetDestination(const TBusClientSession* session, TBusMessage* mess, TBusLocator* locator, TNetAddr* addr);

        /// codec for transport level compression
        virtual NCodecs::TCodecPtr GetTransportCodec(void) const {
            return NCodecs::ICodec::GetInstance("snappy");
        }
    };

    class TBusSyncSourceSession: public TAtomicRefCount<TBusSyncSourceSession> {
        friend class TBusMessageQueue;

    public:
        TBusSyncSourceSession(TIntrusivePtr< ::NBus::NPrivate::TBusSyncSourceSessionImpl> session);
        ~TBusSyncSourceSession();

        void Shutdown();

        TBusMessage* SendSyncMessage(TBusMessage* pMessage, EMessageStatus& status, const TNetAddr* addr = nullptr);

        int RegisterService(const char* hostname, TBusKey start = YBUS_KEYMIN, TBusKey end = YBUS_KEYMAX, EIpVersion ipVersion = EIP_VERSION_4);

        int GetInFlight();

        const TBusProtocol* GetProto() const;

        const TBusClientSession* GetBusClientSessionWorkaroundDoNotUse() const; // It's for TLoadBalancedProtocol::GetDestination() function that really needs TBusClientSession* unlike all other protocols. Look at review 32425 (http://rb.yandex-team.ru/arc/r/32425/) for more information.
    private:
        TIntrusivePtr< ::NBus::NPrivate::TBusSyncSourceSessionImpl> Session;
    };

    using TBusSyncClientSessionPtr = TIntrusivePtr<TBusSyncSourceSession>;

    ///////////////////////////////////////////////////////////////////
    /// \brief Main message queue object, need one per application
    class TBusMessageQueue: public TAtomicRefCount<TBusMessageQueue> {
        /// allow mesage queue to be created only via factory
        friend TBusMessageQueuePtr CreateMessageQueue(const TBusQueueConfig& config, NActor::TExecutorPtr executor, TBusLocator* locator, const char* name);
        friend class ::NBus::NPrivate::TRemoteConnection;
        friend struct ::NBus::NPrivate::TBusSessionImpl;
        friend class ::NBus::NPrivate::TAcceptor;
        friend struct ::NBus::TBusServerSession;

    private:
        const TBusQueueConfig Config;
        TMutex Lock;
        TList<TIntrusivePtr< ::NBus::NPrivate::TBusSessionImpl>> Sessions;
        TSimpleIntrusivePtr<TBusLocator> Locator;
        NPrivate::TScheduler Scheduler;

        ::NActor::TExecutorPtr WorkQueue;

        TAtomic Running;
        TSystemEvent ShutdownComplete;

    private:
        /// constructor is protected, used NBus::CreateMessageQueue() to create a instance
        TBusMessageQueue(const TBusQueueConfig& config, NActor::TExecutorPtr executor, TBusLocator* locator, const char* name);

    public:
        TString GetNameInternal() const;

        ~TBusMessageQueue();

        void Stop();
        bool IsRunning();

    public:
        void EnqueueWork(TArrayRef< ::NActor::IWorkItem* const> w) {
            WorkQueue->EnqueueWork(w);
        }

        ::NActor::TExecutor* GetExecutor() {
            return WorkQueue.Get();
        }

        TString GetStatus(ui16 flags = YBUS_STATUS_CONNS) const;
        // without sessions
        NPrivate::TBusMessageQueueStatus GetStatusRecordInternal() const;
        TString GetStatusSelf() const;
        TString GetStatusSingleLine() const;

        TBusLocator* GetLocator() const {
            return Locator.Get();
        }

        TBusClientSessionPtr CreateSource(TBusProtocol* proto, IBusClientHandler* handler, const TBusClientSessionConfig& config, const TString& name = "");
        TBusSyncClientSessionPtr CreateSyncSource(TBusProtocol* proto, const TBusClientSessionConfig& config, bool needReply = true, const TString& name = "");
        TBusServerSessionPtr CreateDestination(TBusProtocol* proto, IBusServerHandler* hander, const TBusServerSessionConfig& config, const TString& name = "");
        TBusServerSessionPtr CreateDestination(TBusProtocol* proto, IBusServerHandler* hander, const TBusServerSessionConfig& config, const TVector<TBindResult>& bindTo, const TString& name = "");

    private:
        void Destroy(TBusSession* session);
        void Destroy(TBusSyncClientSessionPtr session);

    public:
        void Schedule(NPrivate::IScheduleItemAutoPtr i);

    private:
        void DestroyAllSessions();
        void Add(TIntrusivePtr< ::NBus::NPrivate::TBusSessionImpl> session);
        void Remove(TBusSession* session);
    };

    /////////////////////////////////////////////////////////////////
    /// Factory methods to construct message queue
    TBusMessageQueuePtr CreateMessageQueue(const char* name = "");
    TBusMessageQueuePtr CreateMessageQueue(NActor::TExecutorPtr executor, const char* name = "");
    TBusMessageQueuePtr CreateMessageQueue(const TBusQueueConfig& config, const char* name = "");
    TBusMessageQueuePtr CreateMessageQueue(const TBusQueueConfig& config, TBusLocator* locator, const char* name = "");
    TBusMessageQueuePtr CreateMessageQueue(const TBusQueueConfig& config, NActor::TExecutorPtr executor, TBusLocator* locator, const char* name = "");

}
