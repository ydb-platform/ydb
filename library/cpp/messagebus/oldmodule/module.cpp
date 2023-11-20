#include "module.h"

#include <library/cpp/messagebus/scheduler_actor.h>
#include <library/cpp/messagebus/thread_extra.h>
#include <library/cpp/messagebus/actor/actor.h>
#include <library/cpp/messagebus/actor/queue_in_actor.h>
#include <library/cpp/messagebus/actor/what_thread_does.h>
#include <library/cpp/messagebus/actor/what_thread_does_guard.h>

#include <util/generic/singleton.h>
#include <util/string/printf.h>
#include <util/system/event.h>

using namespace NActor;
using namespace NBus;
using namespace NBus::NPrivate;

namespace {
    Y_POD_STATIC_THREAD(TBusJob*)
    ThreadCurrentJob;

    struct TThreadCurrentJobGuard {
        TBusJob* Prev;

        TThreadCurrentJobGuard(TBusJob* job)
            : Prev(ThreadCurrentJob)
        {
            Y_ASSERT(!ThreadCurrentJob || ThreadCurrentJob == job);
            ThreadCurrentJob = job;
        }
        ~TThreadCurrentJobGuard() {
            ThreadCurrentJob = Prev;
        }
    };

    void ClearState(NBus::TJobState* state) {
        /// skip sendbacks handlers
        if (state->Message != state->Reply) {
            if (state->Message) {
                delete state->Message;
                state->Message = nullptr;
            }

            if (state->Reply) {
                delete state->Reply;
                state->Reply = nullptr;
            }
        }
    }

    void ClearJobStateVector(NBus::TJobStateVec* vec) {
        Y_ASSERT(vec);

        for (auto& call : *vec) {
            ClearState(&call);
        }

        vec->clear();
    }

}

namespace NBus {
    namespace NPrivate {
        class TJobStorage {
        };

        struct TModuleClientHandler
           : public IBusClientHandler {
            TModuleClientHandler(TBusModuleImpl* module)
                : Module(module)
            {
            }

            void OnReply(TAutoPtr<TBusMessage> req, TAutoPtr<TBusMessage> reply) override;
            void OnMessageSentOneWay(TAutoPtr<TBusMessage> pMessage) override;
            void OnError(TAutoPtr<TBusMessage> msg, EMessageStatus status) override;
            void OnClientConnectionEvent(const TClientConnectionEvent& event) override;

            TBusModuleImpl* const Module;
        };

        struct TModuleServerHandler
           : public IBusServerHandler {
            TModuleServerHandler(TBusModuleImpl* module)
                : Module(module)
            {
            }

            void OnMessage(TOnMessageContext& msg) override;

            TBusModuleImpl* const Module;
        };

        struct TBusModuleImpl: public TBusModuleInternal {
            TBusModule* const Module;

            TBusMessageQueue* Queue;

            TScheduler Scheduler;

            const char* const Name;

            typedef TList<TJobRunner*> TBusJobList;
            /// jobs currently in-flight on this module
            TBusJobList Jobs;
            /// module level mutex
            TMutex Lock;
            TCondVar ShutdownCondVar;
            TAtomic JobCount;

            enum EState {
                CREATED,
                RUNNING,
                STOPPED,
            };

            TAtomic State;
            TBusModuleConfig ModuleConfig;
            TBusServerSessionPtr ExternalSession;
            /// protocol for local proxy session
            THolder<IBusClientHandler> ModuleClientHandler;
            THolder<IBusServerHandler> ModuleServerHandler;
            TVector<TSimpleSharedPtr<TBusStarter>> Starters;

            // Sessions must be destroyed before
            // ModuleClientHandler / ModuleServerHandler
            TVector<TBusClientSessionPtr> ClientSessions;
            TVector<TBusServerSessionPtr> ServerSessions;

            TBusModuleImpl(TBusModule* module, const char* name)
                : Module(module)
                , Queue()
                , Name(name)
                , JobCount(0)
                , State(CREATED)
                , ExternalSession(nullptr)
                , ModuleClientHandler(new TModuleClientHandler(this))
                , ModuleServerHandler(new TModuleServerHandler(this))
            {
            }

            ~TBusModuleImpl() override {
                // Shutdown cannot be called from destructor,
                // because module has virtual methods.
                Y_ABORT_UNLESS(State != RUNNING, "if running, must explicitly call Shutdown() before destructor");

                Scheduler.Stop();

                while (!Jobs.empty()) {
                    DestroyJob(Jobs.front());
                }
                Y_ABORT_UNLESS(JobCount == 0, "state check");
            }

            void OnMessageReceived(TAutoPtr<TBusMessage> msg, TOnMessageContext&);

            void AddJob(TJobRunner* jobRunner);

            void DestroyJob(TJobRunner* job);

            /// terminate job on this message
            void CancelJob(TBusJob* job, EMessageStatus status);
            /// prints statuses of jobs
            TString GetStatus(unsigned flags);

            size_t Size() const {
                return AtomicGet(JobCount);
            }

            void Shutdown();

            TVector<TBusClientSessionPtr> GetClientSessionsInternal() override {
                return ClientSessions;
            }

            TVector<TBusServerSessionPtr> GetServerSessionsInternal() override {
                return ServerSessions;
            }

            TBusMessageQueue* GetQueue() override {
                return Queue;
            }

            TString GetNameInternal() override {
                return Name;
            }

            TString GetStatusSingleLine() override {
                TStringStream ss;
                ss << "jobs: " << Size();
                return ss.Str();
            }

            void OnClientConnectionEvent(const TClientConnectionEvent& event) {
                Module->OnClientConnectionEvent(event);
            }
        };

        struct TJobResponseMessage {
            TBusMessage* Request;
            TBusMessage* Response;
            EMessageStatus Status;

            TJobResponseMessage(TBusMessage* request, TBusMessage* response, EMessageStatus status)
                : Request(request)
                , Response(response)
                , Status(status)
            {
            }
        };

        struct TJobRunner: public TAtomicRefCount<TJobRunner>,
                            public NActor::TActor<TJobRunner>,
                            public NActor::TQueueInActor<TJobRunner, TJobResponseMessage>,
                            public TScheduleActor<TJobRunner> {
            THolder<TBusJob> Job;

            TList<TJobRunner*>::iterator JobStorageIterator;

            TJobRunner(TAutoPtr<TBusJob> job)
                : NActor::TActor<TJobRunner>(job->ModuleImpl->Queue->GetExecutor())
                , TScheduleActor<TJobRunner>(&job->ModuleImpl->Scheduler)
                , Job(job.Release())
                , JobStorageIterator()
            {
                Job->Runner = this;
            }

            ~TJobRunner() override {
                Y_ASSERT(JobStorageIterator == TList<TJobRunner*>::iterator());
            }

            void ProcessItem(NActor::TDefaultTag, NActor::TDefaultTag, const TJobResponseMessage& message) {
                Job->CallReplyHandler(message.Status, message.Request, message.Response);
            }

            void Destroy() {
                if (!!Job->OnMessageContext) {
                    if (!Job->ReplySent) {
                        Job->OnMessageContext.ForgetRequest();
                    }
                }
                Job->ModuleImpl->DestroyJob(this);
            }

            void Act(NActor::TDefaultTag) {
                if (JobStorageIterator == TList<TJobRunner*>::iterator()) {
                    return;
                }

                if (Job->SleepUntil != 0) {
                    if (AtomicGet(Job->ModuleImpl->State) == TBusModuleImpl::STOPPED) {
                        Destroy();
                        return;
                    }
                }

                TThreadCurrentJobGuard g(Job.Get());

                NActor::TQueueInActor<TJobRunner, TJobResponseMessage>::DequeueAll();

                if (Alarm.FetchTask()) {
                    if (Job->AnyPendingToSend()) {
                        Y_ASSERT(Job->SleepUntil == 0);
                        Job->SendPending();
                        if (Job->AnyPendingToSend()) {
                        }
                    } else {
                        // regular alarm
                        Y_ASSERT(Job->Pending.empty());
                        Y_ASSERT(Job->SleepUntil != 0);
                        Job->SleepUntil = 0;
                    }
                }

                for (;;) {
                    if (Job->Pending.empty() && !!Job->Handler && Job->Status == MESSAGE_OK) {
                        TWhatThreadDoesPushPop pp("do call job handler (do not confuse with reply handler)");

                        Job->Handler = Job->Handler(Job->Module, Job.Get(), Job->Message);
                    }

                    if (Job->SleepUntil != 0) {
                        ScheduleAt(TInstant::MilliSeconds(Job->SleepUntil));
                        return;
                    }

                    Job->SendPending();

                    if (Job->AnyPendingToSend()) {
                        ScheduleAt(TInstant::Now() + TDuration::Seconds(1));
                        return;
                    }

                    if (!Job->Pending.empty()) {
                        // waiting replies
                        return;
                    }

                    if (Job->IsDone()) {
                        Destroy();
                        return;
                    }
                }
            }
        };

    }

    static inline TJobRunner* GetJob(TBusMessage* message) {
        return (TJobRunner*)message->Data;
    }

    static inline void SetJob(TBusMessage* message, TJobRunner* job) {
        message->Data = job;
    }

    TBusJob::TBusJob(TBusModule* module, TBusMessage* message)
        : Status(MESSAGE_OK)
        , Runner()
        , Message(message)
        , ReplySent(false)
        , Module(module)
        , ModuleImpl(module->Impl.Get())
        , SleepUntil(0)
    {
        Handler = TJobHandler(&TBusModule::Start);
    }

    TBusJob::~TBusJob() {
        Y_ASSERT(Pending.size() == 0);
        //Y_ASSERT(SleepUntil == 0);

        ClearAllMessageStates();
    }

    TNetAddr TBusJob::GetPeerAddrNetAddr() const {
        Y_ABORT_UNLESS(!!OnMessageContext);
        return OnMessageContext.GetPeerAddrNetAddr();
    }

    void TBusJob::CheckThreadCurrentJob() {
        Y_ASSERT(ThreadCurrentJob == this);
    }

    /////////////////////////////////////////////////////////
    /// \brief Send messages in pending list

    /// If at least one message is gone return true
    /// If message has not been send, move it to Finished with appropriate error code
    bool TBusJob::SendPending() {
        // Iterator type must be size_t, not vector::iterator,
        // because `DoCallReplyHandler` may call `Send` that modifies `Pending` vector,
        // that in turn invalidates iterator.
        // Implementation assumes that `DoCallReplyHandler` only pushes back to `Pending`
        // (not erases, and not inserts) so iteration by index is valid.
        size_t it = 0;
        while (it != Pending.size()) {
            TJobState& call = Pending[it];

            if (call.Status == MESSAGE_DONT_ASK) {
                EMessageStatus getAddressStatus = MESSAGE_OK;
                TNetAddr addr;
                if (call.UseAddr) {
                    addr = call.Addr;
                } else {
                    getAddressStatus = const_cast<TBusProtocol*>(call.Session->GetProto())->GetDestination(call.Session, call.Message, call.Session->GetQueue()->GetLocator(), &addr);
                }

                if (getAddressStatus == MESSAGE_OK) {
                    // hold extra reference for each request in flight
                    Runner->Ref();

                    if (call.OneWay) {
                        call.Status = call.Session->SendMessageOneWay(call.Message, &addr);
                    } else {
                        call.Status = call.Session->SendMessage(call.Message, &addr);
                    }

                    if (call.Status != MESSAGE_OK) {
                        Runner->UnRef();
                    }

                } else {
                    call.Status = getAddressStatus;
                }
            }

            if (call.Status == MESSAGE_OK) {
                ++it; // keep pending list until we get reply
            } else if (call.Status == MESSAGE_BUSY) {
                Y_ABORT("MESSAGE_BUSY is prohibited in modules. Please increase MaxInFlight");
            } else if (call.Status == MESSAGE_CONNECT_FAILED && call.NumRetries < call.MaxRetries) {
                ++it; // try up to call.MaxRetries times to send message
                call.NumRetries++;
                DoCallReplyHandler(call);
                call.Status = MESSAGE_DONT_ASK;
                call.Message->Reset(); // generate new Id
            } else {
                Finished.push_back(call);
                DoCallReplyHandler(call);
                Pending.erase(Pending.begin() + it);
            }
        }
        return Pending.size() > 0;
    }

    bool TBusJob::AnyPendingToSend() {
        for (unsigned i = 0; i < Pending.size(); ++i) {
            if (Pending[i].Status == MESSAGE_DONT_ASK) {
                return true;
            }
        }

        return false;
    }

    bool TBusJob::IsDone() {
        bool r = (SleepUntil == 0 && Pending.size() == 0 && (Handler == nullptr || Status != MESSAGE_OK));
        return r;
    }

    void TBusJob::CallJobHandlerOnly() {
        TThreadCurrentJobGuard threadCurrentJobGuard(this);
        TWhatThreadDoesPushPop pp("do call job handler (do not confuse with reply handler)");

        Handler = Handler(ModuleImpl->Module, this, Message);
    }

    bool TBusJob::CallJobHandler() {
        /// go on as far as we can go without waiting
        while (!IsDone()) {
            /// call the handler
            CallJobHandlerOnly();

            /// quit if job is canceled
            if (Status != MESSAGE_OK) {
                break;
            }

            /// there are messages to send and wait for reply
            SendPending();

            if (!Pending.empty()) {
                break;
            }

            /// asked to sleep
            if (SleepUntil) {
                break;
            }
        }

        Y_ABORT_UNLESS(!(Pending.size() == 0 && Handler == nullptr && Status == MESSAGE_OK && !ReplySent),
                 "Handler returned NULL without Cancel() or SendReply() for message=%016" PRIx64 " type=%d",
                 Message->GetHeader()->Id, Message->GetHeader()->Type);

        return IsDone();
    }

    void TBusJob::DoCallReplyHandler(TJobState& call) {
        if (call.Handler) {
            TWhatThreadDoesPushPop pp("do call reply handler (do not confuse with job handler)");

            TThreadCurrentJobGuard threadCurrentJobGuard(this);
            (Module->*(call.Handler))(this, call.Status, call.Message, call.Reply);
        }
    }

    int TBusJob::CallReplyHandler(EMessageStatus status, TBusMessage* mess, TBusMessage* reply) {
        /// find handler for given message and update it's status
        size_t i = 0;
        for (; i < Pending.size(); ++i) {
            TJobState& call = Pending[i];
            if (call.Message == mess) {
                break;
            }
        }

        /// if not found, report error
        if (i == Pending.size()) {
            Y_ABORT("must not happen");
        }

        /// fill in response into job state
        TJobState& call = Pending[i];
        call.Status = status;
        Y_ASSERT(call.Message == mess);
        call.Reply = reply;

        if ((status == MESSAGE_TIMEOUT || status == MESSAGE_DELIVERY_FAILED) && call.NumRetries < call.MaxRetries) {
            call.NumRetries++;
            call.Status = MESSAGE_DONT_ASK;
            call.Message->Reset(); // generate new Id
            DoCallReplyHandler(call);
            return 0;
        }

        /// call the handler if provided
        DoCallReplyHandler(call);

        /// move job state into the finished stack
        Finished.push_back(Pending[i]);
        Pending.erase(Pending.begin() + i);

        return 0;
    }

    ///////////////////////////////////////////////////////////////
    /// send message to any other session or application
    void TBusJob::Send(TBusMessageAutoPtr mess, TBusClientSession* session, TReplyHandler rhandler, size_t maxRetries) {
        CheckThreadCurrentJob();

        SetJob(mess.Get(), Runner);
        Pending.push_back(TJobState(rhandler, MESSAGE_DONT_ASK, mess.Release(), session, nullptr, maxRetries, nullptr, false));
    }

    void TBusJob::Send(TBusMessageAutoPtr mess, TBusClientSession* session, TReplyHandler rhandler, size_t maxRetries, const TNetAddr& addr) {
        CheckThreadCurrentJob();

        SetJob(mess.Get(), Runner);
        Pending.push_back(TJobState(rhandler, MESSAGE_DONT_ASK, mess.Release(), session, nullptr, maxRetries, &addr, false));
    }

    void TBusJob::SendOneWayTo(TBusMessageAutoPtr req, TBusClientSession* session, const TNetAddr& addr) {
        CheckThreadCurrentJob();

        SetJob(req.Get(), Runner);
        Pending.push_back(TJobState(nullptr, MESSAGE_DONT_ASK, req.Release(), session, nullptr, 0, &addr, true));
    }

    void TBusJob::SendOneWayWithLocator(TBusMessageAutoPtr req, TBusClientSession* session) {
        CheckThreadCurrentJob();

        SetJob(req.Get(), Runner);
        Pending.push_back(TJobState(nullptr, MESSAGE_DONT_ASK, req.Release(), session, nullptr, 0, nullptr, true));
    }

    ///////////////////////////////////////////////////////////////
    /// send reply to the starter message
    void TBusJob::SendReply(TBusMessageAutoPtr reply) {
        CheckThreadCurrentJob();

        Y_ABORT_UNLESS(!ReplySent, "cannot call SendReply twice");
        ReplySent = true;
        if (!OnMessageContext)
            return;

        EMessageStatus ok = OnMessageContext.SendReplyMove(reply);
        if (ok != MESSAGE_OK) {
            // TODO: count errors
        }
    }

    /// set the flag to terminate job at the earliest convenience
    void TBusJob::Cancel(EMessageStatus status) {
        CheckThreadCurrentJob();

        Status = status;
    }

    void TBusJob::ClearState(TJobState& call) {
        TJobStateVec::iterator it;
        for (it = Finished.begin(); it != Finished.end(); ++it) {
            TJobState& state = *it;
            if (&call == &state) {
                ::ClearState(&call);
                Finished.erase(it);
                return;
            }
        }
        Y_ASSERT(0);
    }

    void TBusJob::ClearAllMessageStates() {
        ClearJobStateVector(&Finished);
        ClearJobStateVector(&Pending);
    }

    void TBusJob::Sleep(int milliSeconds) {
        CheckThreadCurrentJob();

        Y_ABORT_UNLESS(Pending.empty(), "sleep is not allowed when there are pending job");
        Y_ABORT_UNLESS(SleepUntil == 0, "must not override sleep");

        SleepUntil = Now() + milliSeconds;
    }

    TString TBusJob::GetStatus(unsigned flags) {
        TString strReturn;
        strReturn += Sprintf("  job=%016" PRIx64 " type=%d sent=%d pending=%d (%d) %s\n",
                             Message->GetHeader()->Id,
                             (int)Message->GetHeader()->Type,
                             (int)(Now() - Message->GetHeader()->SendTime) / 1000,
                             (int)Pending.size(),
                             (int)Finished.size(),
                             Status != MESSAGE_OK ? ToString(Status).data() : "");

        TJobStateVec::iterator it;
        for (it = Pending.begin(); it != Pending.end(); ++it) {
            TJobState& call = *it;
            strReturn += call.GetStatus(flags);
        }
        return strReturn;
    }

    TString TJobState::GetStatus(unsigned flags) {
        Y_UNUSED(flags);
        TString strReturn;
        strReturn += Sprintf("    pending=%016" PRIx64 " type=%d (%s) sent=%d %s\n",
                             Message->GetHeader()->Id,
                             (int)Message->GetHeader()->Type,
                             Session->GetProto()->GetService(),
                             (int)(Now() - Message->GetHeader()->SendTime) / 1000,
                             ToString(Status).data());
        return strReturn;
    }

    //////////////////////////////////////////////////////////////////////

    void TBusModuleImpl::CancelJob(TBusJob* job, EMessageStatus status) {
        TWhatThreadDoesAcquireGuard<TMutex> G(Lock, "modules: acquiring lock for CancelJob");
        if (job) {
            job->Cancel(status);
        }
    }

    TString TBusModuleImpl::GetStatus(unsigned flags) {
        Y_UNUSED(flags);
        TWhatThreadDoesAcquireGuard<TMutex> G(Lock, "modules: acquiring lock for GetStatus");
        TString strReturn = Sprintf("JobsInFlight=%d\n", (int)Jobs.size());
        for (auto job : Jobs) {
            //strReturn += job->Job->GetStatus(flags);
            Y_UNUSED(job);
            strReturn += "TODO\n";
        }
        return strReturn;
    }

    TBusModuleConfig::TBusModuleConfig()
        : StarterMaxInFlight(1000)
    {
    }

    TBusModuleConfig::TSecret::TSecret()
        : SchedulePeriod(TDuration::Seconds(1))
    {
    }

    TBusModule::TBusModule(const char* name)
        : Impl(new TBusModuleImpl(this, name))
    {
    }

    TBusModule::~TBusModule() {
    }

    const char* TBusModule::GetName() const {
        return Impl->Name;
    }

    void TBusModule::SetConfig(const TBusModuleConfig& config) {
        Impl->ModuleConfig = config;
    }

    bool TBusModule::StartInput() {
        Y_ABORT_UNLESS(Impl->State == TBusModuleImpl::CREATED, "state check");
        Y_ABORT_UNLESS(!!Impl->Queue, "state check");
        Impl->State = TBusModuleImpl::RUNNING;

        Y_ASSERT(!Impl->ExternalSession);
        TBusServerSessionPtr extSession = CreateExtSession(*Impl->Queue);
        if (extSession != nullptr) {
            Impl->ExternalSession = extSession;
        }

        return true;
    }

    bool TBusModule::Shutdown() {
        Impl->Shutdown();

        return true;
    }

    TBusJob* TBusModule::CreateJobInstance(TBusMessage* message) {
        TBusJob* job = new TBusJob(this, message);
        return job;
    }

    /**
Example for external session creation:

TBusSession* TMyModule::CreateExtSession(TBusMessageQueue& queue) {
    TBusSession* session = CreateDefaultDestination(queue, &ExternalProto, ExternalConfig);
    session->RegisterService(hostname, begin, end);
    return session;
*/

    bool TBusModule::CreatePrivateSessions(TBusMessageQueue* queue) {
        Impl->Queue = queue;
        return true;
    }

    int TBusModule::GetModuleSessionInFlight() const {
        return Impl->Size();
    }

    TIntrusivePtr<TBusModuleInternal> TBusModule::GetInternal() {
        return Impl.Get();
    }

    TBusServerSessionPtr TBusModule::CreateDefaultDestination(
        TBusMessageQueue& queue, TBusProtocol* proto, const TBusServerSessionConfig& config, const TString& name) {
        TBusServerSessionConfig patchedConfig = config;
        patchedConfig.ExecuteOnMessageInWorkerPool = false;
        if (!patchedConfig.Name) {
            patchedConfig.Name = name;
        }
        if (!patchedConfig.Name) {
            patchedConfig.Name = Impl->Name;
        }
        TBusServerSessionPtr session =
            TBusServerSession::Create(proto, Impl->ModuleServerHandler.Get(), patchedConfig, &queue);
        Impl->ServerSessions.push_back(session);
        return session;
    }

    TBusClientSessionPtr TBusModule::CreateDefaultSource(
        TBusMessageQueue& queue, TBusProtocol* proto, const TBusClientSessionConfig& config, const TString& name) {
        TBusClientSessionConfig patchedConfig = config;
        patchedConfig.ExecuteOnReplyInWorkerPool = false;
        if (!patchedConfig.Name) {
            patchedConfig.Name = name;
        }
        if (!patchedConfig.Name) {
            patchedConfig.Name = Impl->Name;
        }
        TBusClientSessionPtr session =
            TBusClientSession::Create(proto, Impl->ModuleClientHandler.Get(), patchedConfig, &queue);
        Impl->ClientSessions.push_back(session);
        return session;
    }

    TBusStarter* TBusModule::CreateDefaultStarter(TBusMessageQueue&, const TBusSessionConfig& config) {
        TBusStarter* session = new TBusStarter(this, config);
        Impl->Starters.push_back(session);
        return session;
    }

    void TBusModule::OnClientConnectionEvent(const TClientConnectionEvent& event) {
        Y_UNUSED(event);
    }

    TString TBusModule::GetStatus(unsigned flags) {
        TString strReturn = Sprintf("%s\n", Impl->Name);
        strReturn += Impl->GetStatus(flags);
        return strReturn;
    }

}

void TBusModuleImpl::AddJob(TJobRunner* jobRunner) {
    TWhatThreadDoesAcquireGuard<TMutex> G(Lock, "modules: acquiring lock for AddJob");
    Jobs.push_back(jobRunner);
    jobRunner->JobStorageIterator = Jobs.end();
    --jobRunner->JobStorageIterator;
}

void TBusModuleImpl::DestroyJob(TJobRunner* job) {
    Y_ASSERT(job->JobStorageIterator != TList<TJobRunner*>::iterator());

    {
        TWhatThreadDoesAcquireGuard<TMutex> G(Lock, "modules: acquiring lock for DestroyJob");
        int jobCount = AtomicDecrement(JobCount);
        Y_ABORT_UNLESS(jobCount >= 0, "decremented too much");
        Jobs.erase(job->JobStorageIterator);

        if (AtomicGet(State) == STOPPED) {
            if (jobCount == 0) {
                ShutdownCondVar.BroadCast();
            }
        }
    }

    job->JobStorageIterator = TList<TJobRunner*>::iterator();
}

void TBusModuleImpl::OnMessageReceived(TAutoPtr<TBusMessage> msg0, TOnMessageContext& context) {
    TBusMessage* msg = !!msg0 ? msg0.Get() : context.GetMessage();
    Y_ABORT_UNLESS(!!msg);

    THolder<TJobRunner> jobRunner(new TJobRunner(Module->CreateJobInstance(msg)));
    jobRunner->Job->MessageHolder.Reset(msg0.Release());
    jobRunner->Job->OnMessageContext.Swap(context);
    SetJob(jobRunner->Job->Message, jobRunner.Get());

    AtomicIncrement(JobCount);

    AddJob(jobRunner.Get());

    jobRunner.Release()->Schedule();
}

void TBusModuleImpl::Shutdown() {
    if (AtomicGet(State) != TBusModuleImpl::RUNNING) {
        AtomicSet(State, TBusModuleImpl::STOPPED);
        return;
    }
    AtomicSet(State, TBusModuleImpl::STOPPED);

    for (auto& clientSession : ClientSessions) {
        clientSession->Shutdown();
    }
    for (auto& serverSession : ServerSessions) {
        serverSession->Shutdown();
    }

    for (size_t starter = 0; starter < Starters.size(); ++starter) {
        Starters[starter]->Shutdown();
    }

    {
        TWhatThreadDoesAcquireGuard<TMutex> guard(Lock, "modules: acquiring lock for Shutdown");
        for (auto& Job : Jobs) {
            Job->Schedule();
        }

        while (!Jobs.empty()) {
            ShutdownCondVar.WaitI(Lock);
        }
    }
}

EMessageStatus TBusModule::StartJob(TAutoPtr<TBusMessage> message) {
    Y_ABORT_UNLESS(Impl->State == TBusModuleImpl::RUNNING);
    Y_ABORT_UNLESS(!!Impl->Queue);

    if ((unsigned)AtomicGet(Impl->JobCount) >= Impl->ModuleConfig.StarterMaxInFlight) {
        return MESSAGE_BUSY;
    }

    TOnMessageContext dummy;
    Impl->OnMessageReceived(message.Release(), dummy);

    return MESSAGE_OK;
}

void TModuleServerHandler::OnMessage(TOnMessageContext& msg) {
    Module->OnMessageReceived(nullptr, msg);
}

void TModuleClientHandler::OnReply(TAutoPtr<TBusMessage> req, TAutoPtr<TBusMessage> resp) {
    TJobRunner* job = GetJob(req.Get());
    Y_ASSERT(job);
    Y_ASSERT(job->Job->Message != req.Get());
    job->EnqueueAndSchedule(TJobResponseMessage(req.Release(), resp.Release(), MESSAGE_OK));
    job->UnRef();
}

void TModuleClientHandler::OnMessageSentOneWay(TAutoPtr<TBusMessage> req) {
    TJobRunner* job = GetJob(req.Get());
    Y_ASSERT(job);
    Y_ASSERT(job->Job->Message != req.Get());
    job->EnqueueAndSchedule(TJobResponseMessage(req.Release(), nullptr, MESSAGE_OK));
    job->UnRef();
}

void TModuleClientHandler::OnError(TAutoPtr<TBusMessage> msg, EMessageStatus status) {
    TJobRunner* job = GetJob(msg.Get());
    if (job) {
        Y_ASSERT(job->Job->Message != msg.Get());
        job->EnqueueAndSchedule(TJobResponseMessage(msg.Release(), nullptr, status));
        job->UnRef();
    }
}

void TModuleClientHandler::OnClientConnectionEvent(const TClientConnectionEvent& event) {
    Module->OnClientConnectionEvent(event);
}
