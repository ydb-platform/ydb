#include "session_impl.h"

#include "acceptor.h"
#include "network.h"
#include "remote_client_connection.h"
#include "remote_client_session.h"
#include "remote_server_connection.h"
#include "remote_server_session.h"
#include "misc/weak_ptr.h"

#include <util/generic/cast.h>

using namespace NActor;
using namespace NBus;
using namespace NBus::NPrivate;
using namespace NEventLoop;

namespace {
    class TScheduleSession: public IScheduleItem {
    public:
        TScheduleSession(TBusSessionImpl* session, TInstant deadline)
            : IScheduleItem(deadline)
            , Session(session)
            , SessionImpl(session)
        {
        }

        void Do() override {
            TIntrusivePtr<TBusSession> session = Session.Get();
            if (!!session) {
                SessionImpl->Cron();
            }
        }

    private:
        TWeakPtr<TBusSession> Session;
        // Work around TWeakPtr limitation
        TBusSessionImpl* SessionImpl;
    };
}

TConnectionsAcceptorsSnapshot::TConnectionsAcceptorsSnapshot()
    : LastConnectionId(0)
    , LastAcceptorId(0)
{
}

struct TBusSessionImpl::TImpl {
    TRemoteConnectionWriterIncrementalStatus DeadConnectionWriterStatusSummary;
    TRemoteConnectionReaderIncrementalStatus DeadConnectionReaderStatusSummary;
    TAcceptorStatus DeadAcceptorStatusSummary;
};

namespace {
    TBusSessionConfig SessionConfigFillDefaults(const TBusSessionConfig& config, const TString& name) {
        TBusSessionConfig copy = config;
        if (copy.TotalTimeout == 0 && copy.SendTimeout == 0) {
            copy.TotalTimeout = TDuration::Seconds(60).MilliSeconds();
            copy.SendTimeout = TDuration::Seconds(15).MilliSeconds();
        } else if (copy.TotalTimeout == 0) {
            Y_ASSERT(copy.SendTimeout != 0);
            copy.TotalTimeout = config.SendTimeout + TDuration::MilliSeconds(10).MilliSeconds();
        } else if (copy.SendTimeout == 0) {
            Y_ASSERT(copy.TotalTimeout != 0);
            if ((ui64)copy.TotalTimeout > (ui64)TDuration::MilliSeconds(10).MilliSeconds()) {
                copy.SendTimeout = copy.TotalTimeout - TDuration::MilliSeconds(10).MilliSeconds();
            } else {
                copy.SendTimeout = copy.TotalTimeout;
            }
        } else {
            Y_ASSERT(copy.TotalTimeout != 0);
            Y_ASSERT(copy.SendTimeout != 0);
        }

        if (copy.ConnectTimeout == 0) {
            copy.ConnectTimeout = copy.SendTimeout;
        }

        Y_ABORT_UNLESS(copy.SendTimeout > 0, "SendTimeout must be > 0");
        Y_ABORT_UNLESS(copy.TotalTimeout > 0, "TotalTimeout must be > 0");
        Y_ABORT_UNLESS(copy.ConnectTimeout > 0, "ConnectTimeout must be > 0");
        Y_ABORT_UNLESS(copy.TotalTimeout >= copy.SendTimeout, "TotalTimeout must be >= SendTimeout");

        if (!copy.Name) {
            copy.Name = name;
        }

        return copy;
    }
}

TBusSessionImpl::TBusSessionImpl(bool isSource, TBusMessageQueue* queue, TBusProtocol* proto,
                                 IBusErrorHandler* handler,
                                 const TBusSessionConfig& config, const TString& name)
    : TActor<TBusSessionImpl, TStatusTag>(queue->WorkQueue.Get())
    , TActor<TBusSessionImpl, TConnectionTag>(queue->WorkQueue.Get())
    , Impl(new TImpl)
    , IsSource_(isSource)
    , Queue(queue)
    , Proto(proto)
    , ProtoName(Proto->GetService())
    , ErrorHandler(handler)
    , HandlerUseCountHolder(&handler->UseCountChecker)
    , Config(SessionConfigFillDefaults(config, name))
    , WriteEventLoop("wr-el")
    , ReadEventLoop("rd-el")
    , LastAcceptorId(0)
    , LastConnectionId(0)
    , Down(0)
{
    Impl->DeadAcceptorStatusSummary.Summary = true;

    ReadEventLoopThread.Reset(new NThreading::TLegacyFuture<void, false>(std::bind(&TEventLoop::Run, std::ref(ReadEventLoop))));
    WriteEventLoopThread.Reset(new NThreading::TLegacyFuture<void, false>(std::bind(&TEventLoop::Run, std::ref(WriteEventLoop))));

    Queue->Schedule(IScheduleItemAutoPtr(new TScheduleSession(this, TInstant::Now() + Config.Secret.TimeoutPeriod)));
}

TBusSessionImpl::~TBusSessionImpl() {
    Y_ABORT_UNLESS(Down);
    Y_ABORT_UNLESS(ShutdownCompleteEvent.WaitT(TDuration::Zero()));
    Y_ABORT_UNLESS(!WriteEventLoop.IsRunning());
    Y_ABORT_UNLESS(!ReadEventLoop.IsRunning());
}

TBusSessionStatus::TBusSessionStatus()
    : InFlightCount(0)
    , InFlightSize(0)
    , InputPaused(false)
{
}

void TBusSessionImpl::Shutdown() {
    if (!AtomicCas(&Down, 1, 0)) {
        ShutdownCompleteEvent.WaitI();
        return;
    }

    Y_ABORT_UNLESS(Queue->IsRunning(), "Session must be shut down prior to queue shutdown");

    TUseAfterFreeCheckerGuard handlerAliveCheckedGuard(ErrorHandler->UseAfterFreeChecker);

    // For legacy clients that don't use smart pointers
    TIntrusivePtr<TBusSessionImpl> thiz(this);

    Queue->Remove(this);

    // shutdown event loops first, so they won't send more events
    // to acceptors and connections
    ReadEventLoop.Stop();
    WriteEventLoop.Stop();
    ReadEventLoopThread->Get();
    WriteEventLoopThread->Get();

    // shutdown acceptors before connections
    // so they won't create more connections
    TVector<TAcceptorPtr> acceptors;
    GetAcceptors(&acceptors);
    {
        TGuard<TMutex> guard(ConnectionsLock);
        Acceptors.clear();
    }

    for (auto& acceptor : acceptors) {
        acceptor->Shutdown();
    }

    // shutdown connections
    TVector<TRemoteConnectionPtr> cs;
    GetConnections(&cs);

    for (auto& c : cs) {
        c->Shutdown(MESSAGE_SHUTDOWN);
    }

    // shutdown connections actor
    // must shutdown after connections destroyed
    ConnectionsData.ShutdownState.ShutdownCommand();
    GetConnectionsActor()->Schedule();
    ConnectionsData.ShutdownState.ShutdownComplete.WaitI();

    // finally shutdown status actor
    StatusData.ShutdownState.ShutdownCommand();
    GetStatusActor()->Schedule();
    StatusData.ShutdownState.ShutdownComplete.WaitI();

    // Make sure no one references IMessageHandler after Shutdown()
    JobCount.WaitForZero();
    HandlerUseCountHolder.Reset();

    ShutdownCompleteEvent.Signal();
}

bool TBusSessionImpl::IsDown() {
    return static_cast<bool>(AtomicGet(Down));
}

size_t TBusSessionImpl::GetInFlightImpl(const TNetAddr& addr) const {
    TRemoteConnectionPtr conn = const_cast<TBusSessionImpl*>(this)->GetConnection(addr, false);
    if (!!conn) {
        return conn->GetInFlight();
    } else {
        return 0;
    }
}

void TBusSessionImpl::GetInFlightBulk(TArrayRef<const TNetAddr> addrs, TArrayRef<size_t> results) const {
    Y_ABORT_UNLESS(addrs.size() == results.size(), "input.size != output.size");
    for (size_t i = 0; i < addrs.size(); ++i) {
        results[i] = GetInFlightImpl(addrs[i]);
    }
}

size_t TBusSessionImpl::GetConnectSyscallsNumForTestImpl(const TNetAddr& addr) const {
    TRemoteConnectionPtr conn = const_cast<TBusSessionImpl*>(this)->GetConnection(addr, false);
    if (!!conn) {
        return conn->GetConnectSyscallsNumForTest();
    } else {
        return 0;
    }
}

void TBusSessionImpl::GetConnectSyscallsNumBulkForTest(TArrayRef<const TNetAddr> addrs, TArrayRef<size_t> results) const {
    Y_ABORT_UNLESS(addrs.size() == results.size(), "input.size != output.size");
    for (size_t i = 0; i < addrs.size(); ++i) {
        results[i] = GetConnectSyscallsNumForTestImpl(addrs[i]);
    }
}

void TBusSessionImpl::FillStatus() {
}

TSessionDumpStatus TBusSessionImpl::GetStatusRecordInternal() {
    // Probably useless, because it returns cached info now
    Y_ABORT_UNLESS(!Queue->GetExecutor()->IsInExecutorThread(),
             "GetStatus must not be called from executor thread");

    TGuard<TMutex> guard(StatusData.StatusDumpCachedMutex);
    // TODO: returns zeros for a second after start
    // (until first cron)
    return StatusData.StatusDumpCached;
}

TString TBusSessionImpl::GetStatus(ui16 flags) {
    Y_UNUSED(flags);

    return GetStatusRecordInternal().PrintToString();
}

TConnectionStatusMonRecord TBusSessionImpl::GetStatusProtobuf() {
    Y_ABORT_UNLESS(!Queue->GetExecutor()->IsInExecutorThread(),
             "GetStatus must not be called from executor thread");

    TGuard<TMutex> guard(StatusData.StatusDumpCachedMutex);

    return StatusData.StatusDumpCached.ConnectionStatusSummary.GetStatusProtobuf();
}

TString TBusSessionImpl::GetStatusSingleLine() {
    TSessionDumpStatus status = GetStatusRecordInternal();

    TStringStream ss;
    ss << "in-flight: " << status.Status.InFlightCount;
    if (IsSource_) {
        ss << " ack: " << status.ConnectionStatusSummary.WriterStatus.AckMessagesSize;
    }
    ss << " send-q: " << status.ConnectionStatusSummary.WriterStatus.SendQueueSize;
    return ss.Str();
}

void TBusSessionImpl::ProcessItem(TStatusTag, TDeadConnectionTag, const TRemoteConnectionWriterIncrementalStatus& connectionStatus) {
    Impl->DeadConnectionWriterStatusSummary += connectionStatus;
}

void TBusSessionImpl::ProcessItem(TStatusTag, TDeadConnectionTag, const TRemoteConnectionReaderIncrementalStatus& connectionStatus) {
    Impl->DeadConnectionReaderStatusSummary += connectionStatus;
}

void TBusSessionImpl::ProcessItem(TStatusTag, TDeadConnectionTag, const TAcceptorStatus& acceptorStatus) {
    Impl->DeadAcceptorStatusSummary += acceptorStatus;
}

void TBusSessionImpl::ProcessItem(TConnectionTag, ::NActor::TDefaultTag, const TOnAccept& onAccept) {
    TSocketHolder socket(onAccept.s);

    if (AtomicGet(Down)) {
        // do not create connections after shutdown initiated
        return;
    }

    //if (Connections.find(addr) != Connections.end()) {
    // TODO: it is possible
    // won't be a problem after socket address replaced with id
    //}

    TRemoteConnectionPtr c(new TRemoteServerConnection(VerifyDynamicCast<TRemoteServerSession*>(this), ++LastConnectionId, onAccept.addr));

    VerifyDynamicCast<TRemoteServerConnection*>(c.Get())->Init(socket.Release(), onAccept.now);

    InsertConnectionLockAcquired(c.Get());
}

void TBusSessionImpl::ProcessItem(TConnectionTag, TRemoveTag, TRemoteConnectionPtr c) {
    TAddrRemoteConnections::iterator it1 = Connections.find(c->PeerAddrSocketAddr);
    if (it1 != Connections.end()) {
        if (it1->second.Get() == c.Get()) {
            Connections.erase(it1);
        }
    }

    THashMap<ui64, TRemoteConnectionPtr>::iterator it2 = ConnectionsById.find(c->ConnectionId);
    if (it2 != ConnectionsById.end()) {
        ConnectionsById.erase(it2);
    }

    SendSnapshotToStatusActor();
}

void TBusSessionImpl::ProcessConnectionsAcceptorsShapshotQueueItem(TAtomicSharedPtr<TConnectionsAcceptorsSnapshot> snapshot) {
    for (TVector<TRemoteConnectionPtr>::const_iterator connection = snapshot->Connections.begin();
         connection != snapshot->Connections.end(); ++connection) {
        Y_ASSERT((*connection)->ConnectionId <= snapshot->LastConnectionId);
    }

    for (TVector<TAcceptorPtr>::const_iterator acceptor = snapshot->Acceptors.begin();
         acceptor != snapshot->Acceptors.end(); ++acceptor) {
        Y_ASSERT((*acceptor)->AcceptorId <= snapshot->LastAcceptorId);
    }

    StatusData.ConnectionsAcceptorsSnapshot = snapshot;
}

void TBusSessionImpl::StatusUpdateCachedDumpIfNecessary(TInstant now) {
    if (now - StatusData.StatusDumpCachedLastUpdate > Config.Secret.StatusFlushPeriod) {
        StatusUpdateCachedDump();
        StatusData.StatusDumpCachedLastUpdate = now;
    }
}

void TBusSessionImpl::StatusUpdateCachedDump() {
    TSessionDumpStatus r;

    if (AtomicGet(Down)) {
        r.Shutdown = true;
        TGuard<TMutex> guard(StatusData.StatusDumpCachedMutex);
        StatusData.StatusDumpCached = r;
        return;
    }

    // TODO: make thread-safe
    FillStatus();

    r.Status = StatusData.Status;

    {
        TStringStream ss;

        TString name = Config.Name;
        if (!name) {
            name = "unnamed";
        }

        ss << (IsSource_ ? "client" : "server") << " session " << name << ", proto " << Proto->GetService() << Endl;
        ss << "in flight: " << r.Status.InFlightCount;
        if (!IsSource_) {
            ss << ", " << r.Status.InFlightSize << "b";
        }
        if (r.Status.InputPaused) {
            ss << " (input paused)";
        }
        ss << "\n";

        r.Head = ss.Str();
    }

    TVector<TRemoteConnectionPtr>& connections = StatusData.ConnectionsAcceptorsSnapshot->Connections;
    TVector<TAcceptorPtr>& acceptors = StatusData.ConnectionsAcceptorsSnapshot->Acceptors;

    r.ConnectionStatusSummary = TRemoteConnectionStatus();
    r.ConnectionStatusSummary.Summary = true;
    r.ConnectionStatusSummary.Server = !IsSource_;
    r.ConnectionStatusSummary.WriterStatus.Incremental = Impl->DeadConnectionWriterStatusSummary;
    r.ConnectionStatusSummary.ReaderStatus.Incremental = Impl->DeadConnectionReaderStatusSummary;

    TAcceptorStatus acceptorStatusSummary = Impl->DeadAcceptorStatusSummary;

    {
        TStringStream ss;

        for (TVector<TAcceptorPtr>::const_iterator acceptor = acceptors.begin();
             acceptor != acceptors.end(); ++acceptor) {
            const TAcceptorStatus status = (*acceptor)->GranStatus.Listen.Get();

            acceptorStatusSummary += status;

            if (acceptor != acceptors.begin()) {
                ss << "\n";
            }
            ss << status.PrintToString();
        }

        r.Acceptors = ss.Str();
    }

    {
        TStringStream ss;

        for (TVector<TRemoteConnectionPtr>::const_iterator connection = connections.begin();
             connection != connections.end(); ++connection) {
            if (connection != connections.begin()) {
                ss << "\n";
            }

            TRemoteConnectionStatus status;
            status.Server = !IsSource_;
            status.ReaderStatus = (*connection)->GranStatus.Reader.Get();
            status.WriterStatus = (*connection)->GranStatus.Writer.Get();

            ss << status.PrintToString();

            r.ConnectionStatusSummary.ReaderStatus += status.ReaderStatus;
            r.ConnectionStatusSummary.WriterStatus += status.WriterStatus;
        }

        r.ConnectionsSummary = r.ConnectionStatusSummary.PrintToString();
        r.Connections = ss.Str();
    }

    r.Config = Config;

    TGuard<TMutex> guard(StatusData.StatusDumpCachedMutex);
    StatusData.StatusDumpCached = r;
}

TBusSessionImpl::TStatusData::TStatusData()
    : ConnectionsAcceptorsSnapshot(new TConnectionsAcceptorsSnapshot)
{
}

void TBusSessionImpl::Act(TStatusTag) {
    TInstant now = TInstant::Now();

    EShutdownState shutdownState = StatusData.ShutdownState.State.Get();

    StatusData.ConnectionsAcceptorsSnapshotsQueue.DequeueAllLikelyEmpty(std::bind(&TBusSessionImpl::ProcessConnectionsAcceptorsShapshotQueueItem, this, std::placeholders::_1));

    GetDeadConnectionWriterStatusQueue()->DequeueAllLikelyEmpty();
    GetDeadConnectionReaderStatusQueue()->DequeueAllLikelyEmpty();
    GetDeadAcceptorStatusQueue()->DequeueAllLikelyEmpty();

    // TODO: check queues are empty if already stopped

    if (shutdownState != SS_RUNNING) {
        // important to beak cyclic link session -> connection -> session
        StatusData.ConnectionsAcceptorsSnapshot->Connections.clear();
        StatusData.ConnectionsAcceptorsSnapshot->Acceptors.clear();
    }

    if (shutdownState == SS_SHUTDOWN_COMMAND) {
        StatusData.ShutdownState.CompleteShutdown();
    }

    StatusUpdateCachedDumpIfNecessary(now);
}

TBusSessionImpl::TConnectionsData::TConnectionsData() {
}

void TBusSessionImpl::Act(TConnectionTag) {
    TConnectionsGuard guard(ConnectionsLock);

    EShutdownState shutdownState = ConnectionsData.ShutdownState.State.Get();
    if (shutdownState == SS_SHUTDOWN_COMPLETE) {
        Y_ABORT_UNLESS(GetRemoveConnectionQueue()->IsEmpty());
        Y_ABORT_UNLESS(GetOnAcceptQueue()->IsEmpty());
    }

    GetRemoveConnectionQueue()->DequeueAllLikelyEmpty();
    GetOnAcceptQueue()->DequeueAllLikelyEmpty();

    if (shutdownState == SS_SHUTDOWN_COMMAND) {
        ConnectionsData.ShutdownState.CompleteShutdown();
    }
}

void TBusSessionImpl::Listen(int port, TBusMessageQueue* q) {
    Listen(BindOnPort(port, Config.ReusePort).second, q);
}

void TBusSessionImpl::Listen(const TVector<TBindResult>& bindTo, TBusMessageQueue* q) {
    Y_ASSERT(q == Queue);
    int actualPort = -1;

    for (const TBindResult& br : bindTo) {
        if (actualPort == -1) {
            actualPort = br.Addr.GetPort();
        } else {
            Y_ABORT_UNLESS(actualPort == br.Addr.GetPort(), "state check");
        }
        if (Config.SocketToS >= 0) {
            SetSocketToS(*br.Socket, &(br.Addr), Config.SocketToS);
        }

        TAcceptorPtr acceptor(new TAcceptor(this, ++LastAcceptorId, br.Socket->Release(), br.Addr));

        TConnectionsGuard guard(ConnectionsLock);
        InsertAcceptorLockAcquired(acceptor.Get());
    }

    Config.ListenPort = actualPort;
}

void TBusSessionImpl::SendSnapshotToStatusActor() {
    //Y_ASSERT(ConnectionsLock.IsLocked());

    TAtomicSharedPtr<TConnectionsAcceptorsSnapshot> snapshot(new TConnectionsAcceptorsSnapshot);
    GetAcceptorsLockAquired(&snapshot->Acceptors);
    GetConnectionsLockAquired(&snapshot->Connections);
    snapshot->LastAcceptorId = LastAcceptorId;
    snapshot->LastConnectionId = LastConnectionId;
    StatusData.ConnectionsAcceptorsSnapshotsQueue.Enqueue(snapshot);
    GetStatusActor()->Schedule();
}

void TBusSessionImpl::InsertConnectionLockAcquired(TRemoteConnection* connection) {
    //Y_ASSERT(ConnectionsLock.IsLocked());

    Connections.insert(std::make_pair(connection->PeerAddrSocketAddr, connection));
    // connection for given adds may already exist at this point
    // (so we overwrite old connection)
    // after reconnect, if previous connections wasn't shutdown yet

    bool inserted2 = ConnectionsById.insert(std::make_pair(connection->ConnectionId, connection)).second;
    Y_ABORT_UNLESS(inserted2, "state check: must be inserted (2)");

    SendSnapshotToStatusActor();
}

void TBusSessionImpl::InsertAcceptorLockAcquired(TAcceptor* acceptor) {
    //Y_ASSERT(ConnectionsLock.IsLocked());

    Acceptors.push_back(acceptor);

    SendSnapshotToStatusActor();
}

void TBusSessionImpl::GetConnections(TVector<TRemoteConnectionPtr>* r) {
    TConnectionsGuard guard(ConnectionsLock);
    GetConnectionsLockAquired(r);
}

void TBusSessionImpl::GetAcceptors(TVector<TAcceptorPtr>* r) {
    TConnectionsGuard guard(ConnectionsLock);
    GetAcceptorsLockAquired(r);
}

void TBusSessionImpl::GetConnectionsLockAquired(TVector<TRemoteConnectionPtr>* r) {
    //Y_ASSERT(ConnectionsLock.IsLocked());

    r->reserve(Connections.size());

    for (auto& connection : Connections) {
        r->push_back(connection.second);
    }
}

void TBusSessionImpl::GetAcceptorsLockAquired(TVector<TAcceptorPtr>* r) {
    //Y_ASSERT(ConnectionsLock.IsLocked());

    r->reserve(Acceptors.size());

    for (auto& acceptor : Acceptors) {
        r->push_back(acceptor);
    }
}

TRemoteConnectionPtr TBusSessionImpl::GetConnectionById(ui64 id) {
    TConnectionsGuard guard(ConnectionsLock);

    THashMap<ui64, TRemoteConnectionPtr>::const_iterator it = ConnectionsById.find(id);
    if (it == ConnectionsById.end()) {
        return nullptr;
    } else {
        return it->second;
    }
}

TAcceptorPtr TBusSessionImpl::GetAcceptorById(ui64 id) {
    TGuard<TMutex> guard(ConnectionsLock);

    for (const auto& Acceptor : Acceptors) {
        if (Acceptor->AcceptorId == id) {
            return Acceptor;
        }
    }

    return nullptr;
}

void TBusSessionImpl::InvokeOnError(TNonDestroyingAutoPtr<TBusMessage> message, EMessageStatus status) {
    message->CheckClean();
    ErrorHandler->OnError(message, status);
}

TRemoteConnectionPtr TBusSessionImpl::GetConnection(const TBusSocketAddr& addr, bool create) {
    TConnectionsGuard guard(ConnectionsLock);

    TAddrRemoteConnections::const_iterator it = Connections.find(addr);
    if (it != Connections.end()) {
        return it->second;
    }

    if (!create) {
        return TRemoteConnectionPtr();
    }

    Y_ABORT_UNLESS(IsSource_, "must be source");

    TRemoteConnectionPtr c(new TRemoteClientConnection(VerifyDynamicCast<TRemoteClientSession*>(this), ++LastConnectionId, addr.ToNetAddr()));
    InsertConnectionLockAcquired(c.Get());

    return c;
}

void TBusSessionImpl::Cron() {
    TVector<TRemoteConnectionPtr> connections;
    GetConnections(&connections);

    for (const auto& it : connections) {
        TRemoteConnection* connection = it.Get();
        if (IsSource_) {
            VerifyDynamicCast<TRemoteClientConnection*>(connection)->ScheduleTimeoutMessages();
        } else {
            VerifyDynamicCast<TRemoteServerConnection*>(connection)->WriterData.TimeToRotateCounters.AddTask();
            // no schedule: do not rotate if there's no traffic
        }
    }

    // status updates are sent without scheduling
    GetStatusActor()->Schedule();

    Queue->Schedule(IScheduleItemAutoPtr(new TScheduleSession(this, TInstant::Now() + Config.Secret.TimeoutPeriod)));
}

TString TBusSessionImpl::GetNameInternal() {
    if (!!Config.Name) {
        return Config.Name;
    }
    return ProtoName;
}
