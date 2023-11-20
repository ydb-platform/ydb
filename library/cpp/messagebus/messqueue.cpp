#include "key_value_printer.h"
#include "mb_lwtrace.h"
#include "remote_client_session.h"
#include "remote_server_session.h"
#include "ybus.h"

#include <util/generic/singleton.h>

using namespace NBus;
using namespace NBus::NPrivate;
using namespace NActor;

TBusMessageQueuePtr NBus::CreateMessageQueue(const TBusQueueConfig& config, TExecutorPtr executor, TBusLocator* locator, const char* name) {
    return new TBusMessageQueue(config, executor, locator, name);
}

TBusMessageQueuePtr NBus::CreateMessageQueue(const TBusQueueConfig& config, TBusLocator* locator, const char* name) {
    TExecutor::TConfig executorConfig;
    executorConfig.WorkerCount = config.NumWorkers;
    executorConfig.Name = name;
    TExecutorPtr executor = new TExecutor(executorConfig);
    return CreateMessageQueue(config, executor, locator, name);
}

TBusMessageQueuePtr NBus::CreateMessageQueue(const TBusQueueConfig& config, const char* name) {
    return CreateMessageQueue(config, new TBusLocator, name);
}

TBusMessageQueuePtr NBus::CreateMessageQueue(TExecutorPtr executor, const char* name) {
    return CreateMessageQueue(TBusQueueConfig(), executor, new TBusLocator, name);
}

TBusMessageQueuePtr NBus::CreateMessageQueue(const char* name) {
    TBusQueueConfig config;
    return CreateMessageQueue(config, name);
}

namespace {
    TBusQueueConfig QueueConfigFillDefaults(const TBusQueueConfig& orig, const TString& name) {
        TBusQueueConfig patched = orig;
        if (!patched.Name) {
            patched.Name = name;
        }
        return patched;
    }
}

TBusMessageQueue::TBusMessageQueue(const TBusQueueConfig& config, TExecutorPtr executor, TBusLocator* locator, const char* name)
    : Config(QueueConfigFillDefaults(config, name))
    , Locator(locator)
    , WorkQueue(executor)
    , Running(1)
{
    InitBusLwtrace();
    InitNetworkSubSystem();
}

TBusMessageQueue::~TBusMessageQueue() {
    Stop();
}

void TBusMessageQueue::Stop() {
    if (!AtomicCas(&Running, 0, 1)) {
        ShutdownComplete.WaitI();
        return;
    }

    Scheduler.Stop();

    DestroyAllSessions();

    WorkQueue->Stop();

    ShutdownComplete.Signal();
}

bool TBusMessageQueue::IsRunning() {
    return AtomicGet(Running);
}

TBusMessageQueueStatus TBusMessageQueue::GetStatusRecordInternal() const {
    TBusMessageQueueStatus r;
    r.ExecutorStatus = WorkQueue->GetStatusRecordInternal();
    r.Config = Config;
    return r;
}

TString TBusMessageQueue::GetStatusSelf() const {
    return GetStatusRecordInternal().PrintToString();
}

TString TBusMessageQueue::GetStatusSingleLine() const {
    return WorkQueue->GetStatusSingleLine();
}

TString TBusMessageQueue::GetStatus(ui16 flags) const {
    TStringStream ss;

    ss << GetStatusSelf();

    TList<TIntrusivePtr<TBusSessionImpl>> sessions;
    {
        TGuard<TMutex> scope(Lock);
        sessions = Sessions;
    }

    for (TList<TIntrusivePtr<TBusSessionImpl>>::const_iterator session = sessions.begin();
         session != sessions.end(); ++session) {
        ss << Endl;
        ss << (*session)->GetStatus(flags);
    }

    ss << Endl;
    ss << "object counts (not necessarily owned by this message queue):" << Endl;
    TKeyValuePrinter p;
    p.AddRow("TRemoteClientConnection", TObjectCounter<TRemoteClientConnection>::ObjectCount(), false);
    p.AddRow("TRemoteServerConnection", TObjectCounter<TRemoteServerConnection>::ObjectCount(), false);
    p.AddRow("TRemoteClientSession", TObjectCounter<TRemoteClientSession>::ObjectCount(), false);
    p.AddRow("TRemoteServerSession", TObjectCounter<TRemoteServerSession>::ObjectCount(), false);
    p.AddRow("NEventLoop::TEventLoop", TObjectCounter<NEventLoop::TEventLoop>::ObjectCount(), false);
    p.AddRow("NEventLoop::TChannel", TObjectCounter<NEventLoop::TChannel>::ObjectCount(), false);
    ss << p.PrintToString();

    return ss.Str();
}

TBusClientSessionPtr TBusMessageQueue::CreateSource(TBusProtocol* proto, IBusClientHandler* handler, const TBusClientSessionConfig& config, const TString& name) {
    TRemoteClientSessionPtr session(new TRemoteClientSession(this, proto, handler, config, name));
    Add(session.Get());
    return session.Get();
}

TBusServerSessionPtr TBusMessageQueue::CreateDestination(TBusProtocol* proto, IBusServerHandler* handler, const TBusClientSessionConfig& config, const TString& name) {
    TRemoteServerSessionPtr session(new TRemoteServerSession(this, proto, handler, config, name));
    try {
        int port = config.ListenPort;
        if (port == 0) {
            port = Locator->GetLocalPort(proto->GetService());
        }
        if (port == 0) {
            port = proto->GetPort();
        }

        session->Listen(port, this);

        Add(session.Get());
        return session.Release();
    } catch (...) {
        Y_ABORT("create destination failure: %s", CurrentExceptionMessage().c_str());
    }
}

TBusServerSessionPtr TBusMessageQueue::CreateDestination(TBusProtocol* proto, IBusServerHandler* handler, const TBusServerSessionConfig& config, const TVector<TBindResult>& bindTo, const TString& name) {
    TRemoteServerSessionPtr session(new TRemoteServerSession(this, proto, handler, config, name));
    try {
        session->Listen(bindTo, this);
        Add(session.Get());
        return session.Release();
    } catch (...) {
        Y_ABORT("create destination failure: %s", CurrentExceptionMessage().c_str());
    }
}

void TBusMessageQueue::Add(TIntrusivePtr<TBusSessionImpl> session) {
    TGuard<TMutex> scope(Lock);
    Sessions.push_back(session);
}

void TBusMessageQueue::Remove(TBusSession* session) {
    TGuard<TMutex> scope(Lock);
    TList<TIntrusivePtr<TBusSessionImpl>>::iterator it = std::find(Sessions.begin(), Sessions.end(), session);
    Y_ABORT_UNLESS(it != Sessions.end(), "do not destroy session twice");
    Sessions.erase(it);
}

void TBusMessageQueue::Destroy(TBusSession* session) {
    session->Shutdown();
}

void TBusMessageQueue::DestroyAllSessions() {
    TList<TIntrusivePtr<TBusSessionImpl>> sessions;
    {
        TGuard<TMutex> scope(Lock);
        sessions = Sessions;
    }

    for (auto& session : sessions) {
        Y_ABORT_UNLESS(session->IsDown(), "Session must be shut down prior to queue shutdown");
    }
}

void TBusMessageQueue::Schedule(IScheduleItemAutoPtr i) {
    Scheduler.Schedule(i);
}

TString TBusMessageQueue::GetNameInternal() const {
    return Config.Name;
}
