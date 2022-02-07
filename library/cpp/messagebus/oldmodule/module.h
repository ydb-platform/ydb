#pragma once

///////////////////////////////////////////////////////////////////////////
/// \file
/// \brief Application interface for modules

/// NBus::TBusModule provides foundation for implementation of asynchnous
/// modules that communicate with multiple external or local sessions
/// NBus::TBusSession.

/// To implement the module some virtual functions needs to be overridden:

/// NBus::TBusModule::CreateExtSession() creates and registers an
/// external session that receives incoming messages as input for module
/// processing.

/// When new incoming message arrives the new NBus::TBusJob is created.
/// NBus::TBusJob is somewhat similar to a thread, it maintains all the state
/// during processing of one incoming message. Default implementation of
/// NBus::TBusJob will maintain all send and received messages during
/// lifetime of this job. Each message, status and reply can be found
/// within NBus::TJobState using NBus::TBusJob::GetState(). If your module
/// needs to maintain an additional information during lifetime of the job
/// you can derive your own class from NBus::TBusJob and override job
/// factory method NBus::IJobFactory::CreateJobInstance() to create your instances.

/// Processing of a given message starts with a call to NBus::TBusModule::Start()
/// handler that should be overridden in the module implementation. Within
/// the callback handler module can perform any computation and access any
/// datastore tables that it needs. The handler can also access any module
/// variables. However, same handler can be called from multiple threads so,
/// it is recommended that handler only access read-only module level variables.

/// Handler should use NBus::TBusJob::Send() to send messages to other client
/// sessions and it can use NBus::TBusJob::Reply() to send reply to the main
/// job message. When handler is done, it returns the pointer to the next handler to call
/// when all pending messages have cleared. If handler
/// returns pointer to itself the module will reschedule execution of this handler
/// for a later time. This should be done in case NBus::TBusJob::Send() returns
/// error (not MESSAGE_OK)

#include "startsession.h"

#include <library/cpp/messagebus/ybus.h>

#include <util/generic/noncopyable.h>
#include <util/generic/object_counter.h>

namespace NBus {
    class TBusJob;
    class TBusModule;

    namespace NPrivate {
        struct TCallJobHandlerWorkItem;
        struct TBusModuleImpl;
        struct TModuleServerHandler;
        struct TModuleClientHandler;
        struct TJobRunner;
    }

    class TJobHandler {
    protected:
        typedef TJobHandler (TBusModule::*TBusHandlerPtr)(TBusJob* job, TBusMessage* mess);
        TBusHandlerPtr MyPtr;

    public:
        template <class B>
        TJobHandler(TJobHandler (B::*fptr)(TBusJob* job, TBusMessage* mess)) {
            MyPtr = static_cast<TBusHandlerPtr>(fptr);
        }
        TJobHandler(TBusHandlerPtr fptr = nullptr) {
            MyPtr = fptr;
        }
        TJobHandler(const TJobHandler&) = default;
        TJobHandler& operator =(const TJobHandler&) = default;
        bool operator==(TJobHandler h) const {
            return MyPtr == h.MyPtr;
        }
        bool operator!=(TJobHandler h) const {
            return MyPtr != h.MyPtr;
        }
        bool operator!() const {
            return !MyPtr;
        }
        TJobHandler operator()(TBusModule* b, TBusJob* job, TBusMessage* mess) {
            return (b->*MyPtr)(job, mess);
        }
    };

    typedef void (TBusModule::*TReplyHandler)(TBusJob* job, EMessageStatus status, TBusMessage* mess, TBusMessage* reply);

    ////////////////////////////////////////////////////
    /// \brief Pending message state

    struct TJobState {
        friend class TBusJob;
        friend class ::TCrawlerModule;

        TReplyHandler Handler;
        EMessageStatus Status;
        TBusMessage* Message;
        TBusMessage* Reply;
        TBusClientSession* Session;
        size_t NumRetries;
        size_t MaxRetries;
        // If != NULL then use it as destination.
        TNetAddr Addr;
        bool UseAddr;
        bool OneWay;

    private:
        TJobState(TReplyHandler handler,
                  EMessageStatus status,
                  TBusMessage* mess, TBusClientSession* session, TBusMessage* reply, size_t maxRetries = 0,
                  const TNetAddr* addr = nullptr, bool oneWay = false)
            : Handler(handler)
            , Status(status)
            , Message(mess)
            , Reply(reply)
            , Session(session)
            , NumRetries(0)
            , MaxRetries(maxRetries)
            , OneWay(oneWay)
        {
            if (!!addr) {
                Addr = *addr;
            }
            UseAddr = !!addr;
        }

    public:
        TString GetStatus(unsigned flags);
    };

    using TJobStateVec = TVector<TJobState>;

    /////////////////////////////////////////////////////////
    /// \brief Execution item = thread

    /// Maintains internal state of document in computation
    class TBusJob {
        TObjectCounter<TBusJob> ObjectCounter;

    private:
        void CheckThreadCurrentJob();

    public:
        /// given a module and starter message
        TBusJob(TBusModule* module, TBusMessage* message);

        /// destructor will free all the message that were send and received
        virtual ~TBusJob();

        TBusMessage* GetMessage() const {
            return Message;
        }

        TNetAddr GetPeerAddrNetAddr() const;

        /// send message to any other session or application
        /// If addr is set then use it as destination.
        void Send(TBusMessageAutoPtr mess, TBusClientSession* session, TReplyHandler rhandler, size_t maxRetries, const TNetAddr& addr);
        void Send(TBusMessageAutoPtr mess, TBusClientSession* session, TReplyHandler rhandler = nullptr, size_t maxRetries = 0);

        void SendOneWayTo(TBusMessageAutoPtr req, TBusClientSession* session, const TNetAddr& addr);
        void SendOneWayWithLocator(TBusMessageAutoPtr req, TBusClientSession* session);

        /// send reply to the starter message
        virtual void SendReply(TBusMessageAutoPtr reply);

        /// set the flag to terminate job at the earliest convenience
        void Cancel(EMessageStatus status);

        /// helper to put item on finished list of states
        /// It should not be a part of public API,
        /// so prohibit it for all except current users.
    private:
        friend class ::TCrawlerModule;
        void PutState(const TJobState& state) {
            Finished.push_back(state);
        }

    public:
        /// retrieve all pending messages
        void GetPending(TJobStateVec* stateVec) {
            Y_ASSERT(stateVec);
            *stateVec = Pending;
        }

        /// helper function to find state of previously sent messages
        template <class MessageType>
        TJobState* GetState(int* startFrom = nullptr) {
            for (int i = startFrom ? *startFrom : 0; i < int(Finished.size()); i++) {
                TJobState* call = &Finished[i];
                if (call->Reply != nullptr && dynamic_cast<MessageType*>(call->Reply)) {
                    if (startFrom) {
                        *startFrom = i;
                    }
                    return call;
                }
                if (call->Message != nullptr && dynamic_cast<MessageType*>(call->Message)) {
                    if (startFrom) {
                        *startFrom = i;
                    }
                    return call;
                }
            }
            return nullptr;
        }

        /// helper function to find response for previously sent messages
        template <class MessageType>
        MessageType* Get(int* startFrom = nullptr) {
            for (int i = startFrom ? *startFrom : 0; i < int(Finished.size()); i++) {
                TJobState& call = Finished[i];
                if (call.Reply != nullptr && dynamic_cast<MessageType*>(call.Reply)) {
                    if (startFrom) {
                        *startFrom = i;
                    }
                    return static_cast<MessageType*>(call.Reply);
                }
                if (call.Message != nullptr && dynamic_cast<MessageType*>(call.Message)) {
                    if (startFrom) {
                        *startFrom = i;
                    }
                    return static_cast<MessageType*>(call.Message);
                }
            }
            return nullptr;
        }

        /// helper function to find status for previously sent message
        template <class MessageType>
        EMessageStatus GetStatus(int* startFrom = nullptr) {
            for (int i = startFrom ? *startFrom : 0; i < int(Finished.size()); i++) {
                TJobState& call = Finished[i];
                if (call.Message != nullptr && dynamic_cast<MessageType*>(call.Message)) {
                    if (startFrom) {
                        *startFrom = i;
                    }
                    return call.Status;
                }
            }
            return MESSAGE_UNKNOWN;
        }

        /// helper function to clear state of previosly sent messages
        template <class MessageType>
        void Clear() {
            for (size_t i = 0; i < Finished.size();) {
                // `Finished.size() - i` decreases with each iteration
                //  we either increment i, or remove element from Finished.
                TJobState& call = Finished[i];
                if (call.Message != nullptr && dynamic_cast<MessageType*>(call.Message)) {
                    ClearState(call);
                } else {
                    ++i;
                }
            }
        }

        /// helper function to clear state in order to try again
        void ClearState(TJobState& state);

        /// clears all message states
        void ClearAllMessageStates();

        /// returns true if job is done
        bool IsDone();

        /// return human reabable status of this job
        virtual TString GetStatus(unsigned flags);

        /// set sleep time for job
        void Sleep(int milliSeconds);

        void CallJobHandlerOnly();

    private:
        bool CallJobHandler();
        void DoCallReplyHandler(TJobState&);
        /// send out all Pending jobs, failed sends will be migrated to Finished
        bool SendPending();
        bool AnyPendingToSend();

    public:
        /// helper to call from OnReply() and OnError()
        int CallReplyHandler(EMessageStatus status, TBusMessage* mess, TBusMessage* reply);

    public:
        TJobHandler Handler;   ///< job handler to be executed within next CallJobHandler()
        EMessageStatus Status; ///< set != MESSAGE_OK if job should terminate asap
    private:
        NPrivate::TJobRunner* Runner;
        TBusMessage* Message;
        THolder<TBusMessage> MessageHolder;
        TOnMessageContext OnMessageContext; // starter
    public:
        bool ReplySent;

    private:
        friend class TBusModule;
        friend struct NPrivate::TBusModuleImpl;
        friend struct NPrivate::TCallJobHandlerWorkItem;
        friend struct NPrivate::TModuleServerHandler;
        friend struct NPrivate::TModuleClientHandler;
        friend struct NPrivate::TJobRunner;

        TJobStateVec Pending;  ///< messages currently outstanding via Send()
        TJobStateVec Finished; ///< messages that were replied to
        TBusModule* Module;
        NPrivate::TBusModuleImpl* ModuleImpl; ///< module which created the job
        TBusInstant SleepUntil;               ///< time to wakeup, 0 if no sleep
    };

    ////////////////////////////////////////////////////////////////////
    /// \brief Classes to implement basic module functionality

    class IJobFactory {
    protected:
        virtual ~IJobFactory() {
        }

    public:
        /// job factory method, override to create custom jobs
        virtual TBusJob* CreateJobInstance(TBusMessage* message) = 0;
    };

    struct TBusModuleConfig {
        unsigned StarterMaxInFlight;

        struct TSecret {
            TDuration SchedulePeriod;

            TSecret();
        };
        TSecret Secret;

        TBusModuleConfig();
    };

    namespace NPrivate {
        struct TBusModuleInternal: public TAtomicRefCount<TBusModuleInternal> {
            virtual TVector<TBusClientSessionPtr> GetClientSessionsInternal() = 0;
            virtual TVector<TBusServerSessionPtr> GetServerSessionsInternal() = 0;
            virtual TBusMessageQueue* GetQueue() = 0;

            virtual TString GetNameInternal() = 0;

            virtual TString GetStatusSingleLine() = 0;

            virtual ~TBusModuleInternal() {
            }
        };
    }

    class TBusModule: public IJobFactory, TNonCopyable {
        friend class TBusJob;

        TObjectCounter<TBusModule> ObjectCounter;

        TIntrusivePtr<NPrivate::TBusModuleImpl> Impl;

    public:
        /// Each module should have a name which is used as protocol service
        TBusModule(const char* name);
        ~TBusModule() override;

        const char* GetName() const;

        void SetConfig(const TBusModuleConfig& config);

        /// get status of all jobs in flight
        TString GetStatus(unsigned flags = 0);

        /// called when application is about to start
        virtual bool StartInput();
        /// called when application is about to exit
        virtual bool Shutdown();

        // this default implementation just creates TBusJob object
        TBusJob* CreateJobInstance(TBusMessage* message) override;

        EMessageStatus StartJob(TAutoPtr<TBusMessage> message);

        /// creates private sessions, calls CreateExtSession(), should be called before StartInput()
        bool CreatePrivateSessions(TBusMessageQueue* queue);

        virtual void OnClientConnectionEvent(const TClientConnectionEvent& event);

    public:
        /// entry point into module, first function to call
        virtual TJobHandler Start(TBusJob* job, TBusMessage* mess) = 0;

    protected:
        /// override this function to create destination session
        virtual TBusServerSessionPtr CreateExtSession(TBusMessageQueue& queue) = 0;

    public:
        int GetModuleSessionInFlight() const;

        TIntrusivePtr<NPrivate::TBusModuleInternal> GetInternal();

    protected:
        TBusServerSessionPtr CreateDefaultDestination(TBusMessageQueue& queue, TBusProtocol* proto, const TBusServerSessionConfig& config, const TString& name = TString());
        TBusClientSessionPtr CreateDefaultSource(TBusMessageQueue& queue, TBusProtocol* proto, const TBusClientSessionConfig& config, const TString& name = TString());
        TBusStarter* CreateDefaultStarter(TBusMessageQueue& unused, const TBusSessionConfig& config);
    };

}
