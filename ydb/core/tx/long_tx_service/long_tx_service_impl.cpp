#include "long_tx_service_impl.h"
#include "lwtrace_probes.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor.h>
#include <util/string/builder.h>

#define TXLOG_LOG(priority, stream) \
    LOG_LOG_S(*TlsActivationContext, priority, NKikimrServices::LONG_TX_SERVICE, LogPrefix << stream)
#define TXLOG_DEBUG(stream) TXLOG_LOG(NActors::NLog::PRI_DEBUG, stream)
#define TXLOG_NOTICE(stream) TXLOG_LOG(NActors::NLog::PRI_NOTICE, stream)
#define TXLOG_ERROR(stream) TXLOG_LOG(NActors::NLog::PRI_ERROR, stream)

LWTRACE_USING(LONG_TX_SERVICE_PROVIDER)

namespace NKikimr {
namespace NLongTxService {

static constexpr size_t MaxAcquireSnapshotInFlight = 4;
static constexpr TDuration AcquireSnapshotBatchDelay = TDuration::MicroSeconds(100);
static constexpr TDuration RemoteLockTimeout = TDuration::Seconds(15);
static constexpr bool InterconnectUndeliveryBroken = true;

void TLongTxServiceActor::Bootstrap() {
    LogPrefix = TStringBuilder() << "TLongTxService [Node " << SelfId().NodeId() << "] ";
    RegisterLongTxServiceProbes();
    Become(&TThis::StateWork);
}

void TLongTxServiceActor::TSessionSubscribeActor::Subscribe(const TActorId& sessionId) {
    Send(sessionId, new TEvents::TEvSubscribe(), IEventHandle::FlagTrackDelivery);
}

void TLongTxServiceActor::TSessionSubscribeActor::Handle(TEvInterconnect::TEvNodeConnected::TPtr&) {
    // nothing
}

void TLongTxServiceActor::TSessionSubscribeActor::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    if (Self) {
        Self->OnSessionDisconnected(ev->Sender);
    }
}

void TLongTxServiceActor::TSessionSubscribeActor::Handle(TEvents::TEvUndelivered::TPtr& ev) {
    if (Self) {
        Self->OnSessionDisconnected(ev->Sender);
    }
}

TLongTxServiceActor::TSessionState& TLongTxServiceActor::SubscribeToSession(const TActorId& sessionId) {
    auto it = Sessions.find(sessionId);
    if (it != Sessions.end()) {
        return it->second;
    }
    if (!SessionSubscribeActor) {
        SessionSubscribeActor = new TSessionSubscribeActor(this);
        RegisterWithSameMailbox(SessionSubscribeActor);
    }
    SessionSubscribeActor->Subscribe(sessionId);
    return Sessions[sessionId];
}

void TLongTxServiceActor::OnSessionDisconnected(const TActorId& sessionId) {
    auto itSession = Sessions.find(sessionId);
    if (itSession == Sessions.end()) {
        return;
    }
    auto& session = itSession->second;
    for (ui64 lockId : session.SubscribedLocks) {
        auto itLock = Locks.find(lockId);
        if (itLock != Locks.end()) {
            itLock->second.RemoteSubscribers.erase(sessionId);
        }
    }
    session.SubscribedLocks.clear();
    Sessions.erase(itSession);
}

void TLongTxServiceActor::HandlePoison() {
    if (SessionSubscribeActor) {
        SessionSubscribeActor->PassAway();
        SessionSubscribeActor->Self = nullptr;
        SessionSubscribeActor = nullptr;
    }
    PassAway();
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvBeginTx::TPtr& ev) {
    const auto* msg = ev->Get();
    const TString& databaseName = msg->Record.GetDatabaseName();

    TXLOG_DEBUG("Received TEvBeginTx from " << ev->Sender);

    switch (msg->Record.GetMode()) {
        case NKikimrLongTxService::TEvBeginTx::MODE_READ_ONLY:
        case NKikimrLongTxService::TEvBeginTx::MODE_READ_WRITE: {
            TLongTxId txId;
            if (msg->Record.GetMode() == NKikimrLongTxService::TEvBeginTx::MODE_READ_WRITE) {
                auto now = TActivationContext::Now();
                txId.UniqueId = IdGenerator.Next(now);
                txId.NodeId = SelfId().NodeId();
            }
            auto& dbState = DatabaseSnapshots[databaseName];
            auto& req = dbState.PendingBeginTxRequests.emplace_back();
            req.TxId = txId;
            req.Sender = ev->Sender;
            req.Cookie = ev->Cookie;
            ScheduleAcquireSnapshot(databaseName, dbState);
            return;
        }
        case NKikimrLongTxService::TEvBeginTx::MODE_WRITE_ONLY: {
            TLongTxId txId;
            auto now = TActivationContext::Now();
            txId.UniqueId = IdGenerator.Next(now);
            txId.NodeId = SelfId().NodeId();
            Y_ABORT_UNLESS(!Transactions.contains(txId.UniqueId));
            auto& tx = Transactions[txId.UniqueId];
            tx.TxId = txId;
            tx.DatabaseName = databaseName;
            tx.State = ETxState::Active;
            TXLOG_DEBUG("Created new LongTxId# " << txId);
            Send(ev->Sender, new TEvLongTxService::TEvBeginTxResult(txId), 0, ev->Cookie);
            return;
        }
        default: {
            NYql::TIssues issues;
            issues.AddIssue("Unsupported transaction mode");
            Send(ev->Sender, new TEvLongTxService::TEvBeginTxResult(Ydb::StatusIds::BAD_REQUEST, std::move(issues)), 0, ev->Cookie);
            return;
        }
    }
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvCommitTx::TPtr& ev) {
    const auto* msg = ev->Get();

    TXLOG_DEBUG("Received TEvCommitTx from " << ev->Sender << " LongTxId# " << msg->GetLongTxId());

    TLongTxId txId = msg->GetLongTxId();
    if (!txId.IsWritable()) {
        Send(ev->Sender, new TEvLongTxService::TEvCommitTxResult(Ydb::StatusIds::SUCCESS), 0, ev->Cookie);
        return;
    }

    if (txId.NodeId != SelfId().NodeId()) {
        return SendProxyRequest(txId.NodeId, ERequestType::Commit, std::move(ev));
    }

    auto it = Transactions.find(txId.UniqueId);
    if (it == Transactions.end() || it->second.State == ETxState::Uninitialized) {
        return SendReply(ERequestType::Commit, ev->Sender, ev->Cookie,
            Ydb::StatusIds::BAD_SESSION, TStringBuilder()
                << "Unknown transaction id: " << txId);
    }

    auto& tx = it->second;
    if (tx.ColumnShardWrites.empty()) {
        // There's nothing to commit, so just reply with success
        Send(ev->Sender, new TEvLongTxService::TEvCommitTxResult(Ydb::StatusIds::SUCCESS), 0, ev->Cookie);
        TXLOG_DEBUG("Committed LongTxId# " << txId << " without side-effects");
        Transactions.erase(it);
        return;
    }

    tx.Committers.push_back(TSenderId{ ev->Sender, ev->Cookie });
    if (tx.State == ETxState::Committing) {
        return;
    }

    Y_ABORT_UNLESS(tx.State == ETxState::Active);
    Y_ABORT_UNLESS(!tx.CommitActor);
    StartCommitActor(tx);
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvCommitTxResult::TPtr& ev) {
    ui32 nodeId = ev->Sender.NodeId();
    auto* node = ProxyNodes.FindPtr(nodeId);
    if (!node) {
        TXLOG_DEBUG("Ignored unexpected TEvCommitTxResult from node " << nodeId);
        return;
    }

    auto it = node->ActiveRequests.find(ev->Cookie);
    if (it == node->ActiveRequests.end() || it->second.Type != ERequestType::Commit) {
        TXLOG_DEBUG("Ignored unexpected TEvCommitTxResult from node " << nodeId << " with cookie " << ev->Cookie);
        return;
    }

    Send(it->second.Sender, ev->Release().Release(), 0, it->second.Cookie);
    node->ActiveRequests.erase(it);
}

void TLongTxServiceActor::Handle(TEvPrivate::TEvCommitFinished::TPtr& ev) {
    const auto* msg = ev->Get();

    auto it = Transactions.find(msg->TxId.UniqueId);
    Y_ABORT_UNLESS(it != Transactions.end());
    auto& tx = it->second;
    Y_DEBUG_ABORT_UNLESS(tx.TxId == msg->TxId);

    for (auto& c : tx.Committers) {
        SendReplyIssues(ERequestType::Commit, c.Sender, c.Cookie, msg->Status, msg->Issues);
    }
    Transactions.erase(it);
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvRollbackTx::TPtr& ev) {
    const auto* msg = ev->Get();

    TXLOG_DEBUG("Received TEvRollbackTx from " << ev->Sender << " LongTxId# " << msg->GetLongTxId());

    TLongTxId txId = msg->GetLongTxId();
    if (!txId.IsWritable()) {
        Send(ev->Sender, new TEvLongTxService::TEvRollbackTxResult(Ydb::StatusIds::SUCCESS), 0, ev->Cookie);
        return;
    }

    if (txId.NodeId != SelfId().NodeId()) {
        return SendProxyRequest(txId.NodeId, ERequestType::Rollback, std::move(ev));
    }

    auto it = Transactions.find(txId.UniqueId);
    if (it == Transactions.end() || it->second.State == ETxState::Uninitialized) {
        return SendReply(ERequestType::Rollback, ev->Sender, ev->Cookie,
            Ydb::StatusIds::BAD_SESSION, TStringBuilder()
                << "Unknown transaction id: " << txId);
    }

    auto& tx = it->second;
    if (tx.State == ETxState::Committing) {
        // TODO: could we try to abort?
        return SendReply(ERequestType::Rollback, ev->Sender, ev->Cookie,
            Ydb::StatusIds::SESSION_BUSY, TStringBuilder()
                << "Cannot rollback committing transaction id: " << txId);
    }

    Send(ev->Sender, new TEvLongTxService::TEvRollbackTxResult(Ydb::StatusIds::SUCCESS), 0, ev->Cookie);
    TXLOG_DEBUG("Erased LongTxId# " << txId);
    Transactions.erase(it);
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvRollbackTxResult::TPtr& ev) {
    ui32 nodeId = ev->Sender.NodeId();
    auto* node = ProxyNodes.FindPtr(nodeId);
    if (!node) {
        TXLOG_DEBUG("Ignored unexpected TEvRollbackTxResult from node " << nodeId);
        return;
    }

    auto it = node->ActiveRequests.find(ev->Cookie);
    if (it == node->ActiveRequests.end() || it->second.Type != ERequestType::Rollback) {
        TXLOG_DEBUG("Ignored unexpected TEvRollbackTxResult from node " << nodeId << " with cookie " << ev->Cookie);
        return;
    }

    Send(it->second.Sender, ev->Release().Release(), 0, it->second.Cookie);
    node->ActiveRequests.erase(it);
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvAttachColumnShardWrites::TPtr& ev) {
    const auto* msg = ev->Get();

    TXLOG_DEBUG("Received TEvAttachColumnShardWrites from " << ev->Sender << " LongTxId# " << msg->GetLongTxId());

    TLongTxId txId = msg->GetLongTxId();
    if (!txId.IsWritable()) {
        return SendReply(ERequestType::AttachColumnShardWrites, ev->Sender, ev->Cookie,
            Ydb::StatusIds::BAD_REQUEST, "Cannot attach writes to a read-only tx");
    }

    if (txId.NodeId != SelfId().NodeId()) {
        return SendProxyRequest(txId.NodeId, ERequestType::AttachColumnShardWrites, std::move(ev));
    }

    auto it = Transactions.find(txId.UniqueId);
    if (it == Transactions.end() || it->second.State == ETxState::Uninitialized) {
        return SendReply(ERequestType::AttachColumnShardWrites, ev->Sender, ev->Cookie,
            Ydb::StatusIds::BAD_SESSION, TStringBuilder()
                << "Unknown transaction id: " << txId);
    }

    auto& tx = it->second;
    if (tx.State != ETxState::Active) {
        return SendReply(ERequestType::AttachColumnShardWrites, ev->Sender, ev->Cookie,
            Ydb::StatusIds::UNDETERMINED, "Cannot attach new writes to the transaction");
    }

    for (const auto& write : msg->Record.GetWrites()) {
        const ui64 shardId = write.GetColumnShard();
        const ui64 writeId = write.GetWriteId();
        auto it = tx.ColumnShardWrites.find(shardId);
        if (it == tx.ColumnShardWrites.end()) {
            it = tx.ColumnShardWrites.emplace(shardId, TTransaction::TShardWriteIds()).first;
        }
        it->second.emplace_back(writeId);
    }

    Send(ev->Sender, new TEvLongTxService::TEvAttachColumnShardWritesResult(Ydb::StatusIds::SUCCESS), 0, ev->Cookie);
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvAttachColumnShardWritesResult::TPtr& ev) {
    ui32 nodeId = ev->Sender.NodeId();
    auto* node = ProxyNodes.FindPtr(nodeId);
    if (!node) {
        TXLOG_DEBUG("Ignored unexpected TEvAttachColumnShardWritesResult from node " << nodeId);
        return;
    }

    auto it = node->ActiveRequests.find(ev->Cookie);
    if (it == node->ActiveRequests.end() || it->second.Type != ERequestType::AttachColumnShardWrites) {
        TXLOG_DEBUG("Ignored unexpected TEvAttachColumnShardWritesResult from node " << nodeId << " with cookie " << ev->Cookie);
        return;
    }

    Send(it->second.Sender, ev->Release().Release(), 0, it->second.Cookie);
    node->ActiveRequests.erase(it);
}

const TString& TLongTxServiceActor::GetDatabaseNameOrLegacyDefault(const TString& databaseName) {
    if (!databaseName.empty()) {
        return databaseName;
    }

    if (DefaultDatabaseName.empty()) {
        auto* appData = AppData();
        Y_ABORT_UNLESS(appData);
        Y_ABORT_UNLESS(appData->DomainsInfo);
        // Usually there's exactly one domain
        if (appData->EnableMvccSnapshotWithLegacyDomainRoot && Y_LIKELY(appData->DomainsInfo->Domain)) {
            DefaultDatabaseName = appData->DomainsInfo->GetDomain()->Name;
        }
    }

    return DefaultDatabaseName;
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvAcquireReadSnapshot::TPtr& ev) {
    if (Settings.Counters) {
        Settings.Counters->AcquireReadSnapshotInRequests->Inc();
    }

    auto* msg = ev->Get();
    const TString& databaseName = GetDatabaseNameOrLegacyDefault(msg->Record.GetDatabaseName());
    TXLOG_DEBUG("Received TEvAcquireReadSnapshot from " << ev->Sender << " for database " << databaseName);

    LWTRACK(AcquireReadSnapshotRequest, msg->Orbit, databaseName);

    if (databaseName.empty()) {
        NYql::TIssues issues;
        issues.AddIssue("Cannot acquire snapshot for an unspecified database");
        Send(ev->Sender, new TEvLongTxService::TEvAcquireReadSnapshotResult(Ydb::StatusIds::SCHEME_ERROR, std::move(issues), std::move(msg->Orbit)), 0, ev->Cookie);
        return;
    }

    // TODO: we need to filter allowed databases

    auto& state = DatabaseSnapshots[databaseName];
    {
        auto& req = state.PendingUserRequests.emplace_back();
        req.Sender = ev->Sender;
        req.Cookie = ev->Cookie;
        req.Orbit = std::move(msg->Orbit);
    }

    if (Settings.Counters) {
        Settings.Counters->AcquireReadSnapshotInInFlight->Inc();
    }

    ScheduleAcquireSnapshot(databaseName, state);
}

void TLongTxServiceActor::ScheduleAcquireSnapshot(const TString& databaseName, TDatabaseSnapshotState& state) {
    Y_ABORT_UNLESS(state.PendingUserRequests || state.PendingBeginTxRequests);

    if (state.FlushPending || state.ActiveRequests.size() >= MaxAcquireSnapshotInFlight) {
        return;
    }

    TXLOG_DEBUG("Scheduling TEvAcquireSnapshotFlush for database " << databaseName);
    Schedule(AcquireSnapshotBatchDelay, new TEvPrivate::TEvAcquireSnapshotFlush(databaseName));
    state.FlushPending = true;
}

void TLongTxServiceActor::Handle(TEvPrivate::TEvAcquireSnapshotFlush::TPtr& ev) {
    const auto* msg = ev->Get();
    TXLOG_DEBUG("Received TEvAcquireSnapshotFlush for database " << msg->DatabaseName);

    auto& state = DatabaseSnapshots[msg->DatabaseName];
    Y_ABORT_UNLESS(state.FlushPending);
    Y_ABORT_UNLESS(state.PendingUserRequests || state.PendingBeginTxRequests);
    state.FlushPending = false;

    StartAcquireSnapshotActor(msg->DatabaseName, state);
}

void TLongTxServiceActor::Handle(TEvPrivate::TEvAcquireSnapshotFinished::TPtr& ev) {
    const auto* msg = ev->Get();
    TXLOG_DEBUG("Received TEvAcquireSnapshotFinished, cookie = " << ev->Cookie);

    auto* req = AcquireSnapshotInFlight.FindPtr(ev->Cookie);
    Y_ABORT_UNLESS(req, "Unexpected reply for request that is not inflight");
    TString databaseName = req->DatabaseName;

    auto* state = DatabaseSnapshots.FindPtr(databaseName);
    Y_ABORT_UNLESS(state && state->ActiveRequests.contains(ev->Cookie), "Unexpected database snapshot state");

    if (msg->Status == Ydb::StatusIds::SUCCESS) {
        for (auto& userReq : req->UserRequests) {
            LWTRACK(AcquireReadSnapshotSuccess, userReq.Orbit, msg->Snapshot.Step, msg->Snapshot.TxId);
            Send(userReq.Sender, new TEvLongTxService::TEvAcquireReadSnapshotResult(databaseName, msg->Snapshot, std::move(userReq.Orbit)), 0, userReq.Cookie);
        }
        for (auto& beginReq : req->BeginTxRequests) {
            auto txId = beginReq.TxId;
            txId.Snapshot = msg->Snapshot;
            if (txId.IsWritable()) {
                Y_ABORT_UNLESS(!Transactions.contains(txId.UniqueId));
                auto& tx = Transactions[txId.UniqueId];
                tx.TxId = txId;
                tx.DatabaseName = databaseName;
                tx.State = ETxState::Active;
                TXLOG_DEBUG("Created new read-write LongTxId# " << txId);
            } else {
                TXLOG_DEBUG("Created new read-only LongTxId# " << txId);
            }
            Send(beginReq.Sender, new TEvLongTxService::TEvBeginTxResult(txId), 0, beginReq.Cookie);
        }
    } else {
        for (auto& userReq : req->UserRequests) {
            LWTRACK(AcquireReadSnapshotFailure, userReq.Orbit, int(msg->Status));
            Send(userReq.Sender, new TEvLongTxService::TEvAcquireReadSnapshotResult(msg->Status, msg->Issues, std::move(userReq.Orbit)), 0, userReq.Cookie);
        }
        for (auto& beginReq : req->BeginTxRequests) {
            Send(beginReq.Sender, new TEvLongTxService::TEvBeginTxResult(msg->Status, msg->Issues), 0, beginReq.Cookie);
        }
    }

    if (Settings.Counters) {
        Settings.Counters->AcquireReadSnapshotInInFlight->Sub(req->UserRequests.size());
        Settings.Counters->AcquireReadSnapshotOutInFlight->Dec();
    }

    state->ActiveRequests.erase(ev->Cookie);
    AcquireSnapshotInFlight.erase(ev->Cookie);

    if (state->FlushPending) {
        // A next flush message is pending for this database
        return;
    }

    if (state->PendingUserRequests || state->PendingBeginTxRequests) {
        // We have just finished one request and may start another one
        StartAcquireSnapshotActor(databaseName, *state);
        return;
    }

    // There is nothing to track for this database, free resources
    if (state->ActiveRequests.empty()) {
        DatabaseSnapshots.erase(databaseName);
        return;
    }
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvRegisterLock::TPtr& ev) {
    auto* msg = ev->Get();
    ui64 lockId = msg->LockId;
    TXLOG_DEBUG("Received TEvRegisterLock for LockId# " << lockId);

    Y_ABORT_UNLESS(lockId, "Unexpected registration of a zero LockId");

    auto& lock = Locks[lockId];
    ++lock.RefCount;
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvUnregisterLock::TPtr& ev) {
    auto* msg = ev->Get();
    ui64 lockId = msg->LockId;
    TXLOG_DEBUG("Received TEvUnregisterLock for LockId# " << lockId);

    auto it = Locks.find(lockId);
    if (it == Locks.end()) {
        return;
    }

    auto& lock = it->second;
    Y_ABORT_UNLESS(lock.RefCount > 0);
    if (0 == --lock.RefCount) {
        for (auto& pr : lock.LocalSubscribers) {
            Send(pr.first,
                new TEvLongTxService::TEvLockStatus(
                    lockId, SelfId().NodeId(),
                    NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND),
                0, pr.second);
        }
        for (auto& prSession : lock.RemoteSubscribers) {
            TActorId sessionId = prSession.first;
            for (const auto& pr : prSession.second) {
                SendViaSession(
                    sessionId, pr.first,
                    new TEvLongTxService::TEvLockStatus(
                        lockId, SelfId().NodeId(),
                        NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND),
                    0, pr.second);
            }
            auto itSession = Sessions.find(sessionId);
            if (itSession != Sessions.end()) {
                itSession->second.SubscribedLocks.erase(lockId);
            }
        }
        Locks.erase(it);
    }
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvSubscribeLock::TPtr& ev) {
    auto& record = ev->Get()->Record;
    ui64 lockId = record.GetLockId();
    ui32 lockNode = record.GetLockNode();
    TXLOG_DEBUG("Received TEvSubscribeLock from " << ev->Sender << " for LockId# " << lockId << " LockNode# " << lockNode);

    if (!lockId) {
        SendViaSession(
            ev->InterconnectSession, ev->Sender,
            new TEvLongTxService::TEvLockStatus(
                lockId, lockNode,
                NKikimrLongTxService::TEvLockStatus::STATUS_UNAVAILABLE),
            0, ev->Cookie);
        return;
    }

    // For remote locks we start a proxy subscription
    if (lockNode != SelfId().NodeId()) {
        auto& node = ConnectProxyNode(lockNode);
        if (node.State == EProxyState::Disconnected) {
            // Looks like there's no proxy for this node
            Send(ev->Sender,
                new TEvLongTxService::TEvLockStatus(
                    lockId, lockNode,
                    NKikimrLongTxService::TEvLockStatus::STATUS_UNAVAILABLE),
                0, ev->Cookie);
            return;
        }

        auto& lock = node.Locks[lockId];
        if (lock.LockId == 0) {
            lock.LockId = lockId;
        }

        if (lock.State == EProxyLockState::Subscribed) {
            Send(ev->Sender,
                new TEvLongTxService::TEvLockStatus(
                    lockId, lockNode,
                    NKikimrLongTxService::TEvLockStatus::STATUS_SUBSCRIBED),
                0, ev->Cookie);
            lock.RepliedSubscribers[ev->Sender] = ev->Cookie;
            return;
        }

        lock.NewSubscribers[ev->Sender] = ev->Cookie;
        lock.RepliedSubscribers.erase(ev->Sender);

        // Send subscription request immediately if node is already connected
        if (node.State == EProxyState::Connected && lock.Cookie == 0) {
            lock.Cookie = ++LastCookie;
            node.CookieToLock[lock.Cookie] = lockId;
            SendViaSession(
                node.Session, MakeLongTxServiceID(lockNode),
                new TEvLongTxService::TEvSubscribeLock(lockId, lockNode),
                IEventHandle::FlagTrackDelivery, lock.Cookie);
        }

        // Otherwise we wait until the lock is subscribed
        return;
    }

    auto it = Locks.find(lockId);
    if (it == Locks.end()) {
        SendViaSession(
            ev->InterconnectSession, ev->Sender,
            new TEvLongTxService::TEvLockStatus(
                lockId, lockNode,
                NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND),
            0, ev->Cookie);
        return;
    }

    auto& lock = it->second;
    if (ev->InterconnectSession) {
        auto& session = SubscribeToSession(ev->InterconnectSession);
        session.SubscribedLocks.insert(lockId);
        lock.RemoteSubscribers[ev->InterconnectSession][ev->Sender] = ev->Cookie;
    } else {
        lock.LocalSubscribers[ev->Sender] = ev->Cookie;
    }

    SendViaSession(
        ev->InterconnectSession, ev->Sender,
        new TEvLongTxService::TEvLockStatus(
            lockId, lockNode,
            NKikimrLongTxService::TEvLockStatus::STATUS_SUBSCRIBED),
        0, ev->Cookie);
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvLockStatus::TPtr& ev) {
    auto& record = ev->Get()->Record;
    ui64 lockId = record.GetLockId();
    ui32 lockNode = record.GetLockNode();
    auto lockStatus = record.GetStatus();
    TXLOG_DEBUG("Received TEvLockStatus from " << ev->Sender
            << " for LockId# " << lockId << " LockNode# " << lockNode
            << " LockStatus# " << lockStatus);

    auto* node = ProxyNodes.FindPtr(lockNode);
    if (!node || node->State != EProxyState::Connected) {
        // Ignore replies from unexpected nodes
        return;
    }

    if (ev->InterconnectSession != node->Session) {
        // Ignore replies that arrived via unexpected sessions
        return;
    }

    auto itLock = node->Locks.find(lockId);
    if (itLock == node->Locks.end()) {
        // Ignore replies for locks without subscriptions
        return;
    }

    auto& lock = itLock->second;

    if (lock.Cookie != ev->Cookie) {
        // Ignore replies that don't have a matching cookie
        return;
    }

    // Make sure lock is removed from expire queue
    if (node->LockExpireQueue.Has(&lock)) {
        node->LockExpireQueue.Remove(&lock);
    }

    // Special handling for successful lock subscriptions
    if (lockStatus == NKikimrLongTxService::TEvLockStatus::STATUS_SUBSCRIBED) {
        lock.State = EProxyLockState::Subscribed;
        for (auto& pr : lock.NewSubscribers) {
            Send(pr.first,
                new TEvLongTxService::TEvLockStatus(lockId, lockNode, lockStatus),
                0, pr.second);
            lock.RepliedSubscribers[pr.first] = pr.second;
        }
        lock.NewSubscribers.clear();
        return;
    }

    // Treat any other status as a confirmed error, reply to all and remove the lock

    for (auto& pr : lock.RepliedSubscribers) {
        Send(pr.first,
            new TEvLongTxService::TEvLockStatus(lockId, lockNode, lockStatus),
            0, pr.second);
    }

    for (auto& pr : lock.NewSubscribers) {
        Send(pr.first,
            new TEvLongTxService::TEvLockStatus(lockId, lockNode, lockStatus),
            0, pr.second);
    }

    node->CookieToLock.erase(lock.Cookie);
    node->Locks.erase(itLock);
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvUnsubscribeLock::TPtr& ev) {
    auto& record = ev->Get()->Record;
    ui64 lockId = record.GetLockId();
    ui32 lockNode = record.GetLockNode();
    TXLOG_DEBUG("Received TEvUnsubscribeLock from " << ev->Sender << " for LockId# " << lockId << " LockNode# " << lockNode);

    if (!lockId) {
        return;
    }

    if (lockNode != SelfId().NodeId()) {
        auto* node = ProxyNodes.FindPtr(lockNode);
        if (!node) {
            return;
        }

        auto itLock = node->Locks.find(lockId);
        if (itLock == node->Locks.end()) {
            return;
        }

        auto& lock = itLock->second;
        lock.NewSubscribers.erase(ev->Sender);
        lock.RepliedSubscribers.erase(ev->Sender);

        if (lock.Empty()) {
            // We don't need this lock anymore, unsubscribe if the node is already connected
            if (node->State == EProxyState::Connected) {
                SendViaSession(
                    node->Session, MakeLongTxServiceID(lockNode),
                    new TEvLongTxService::TEvUnsubscribeLock(lockId, lockNode));
            }
            if (node->LockExpireQueue.Has(&lock)) {
                node->LockExpireQueue.Remove(&lock);
            }
            node->CookieToLock.erase(lock.Cookie);
            node->Locks.erase(itLock);
        }

        return;
    }

    auto it = Locks.find(lockId);
    if (it == Locks.end()) {
        return;
    }

    auto& lock = it->second;
    if (ev->InterconnectSession) {
        auto itSubscribers = lock.RemoteSubscribers.find(ev->InterconnectSession);
        if (itSubscribers != lock.RemoteSubscribers.end()) {
            itSubscribers->second.erase(ev->Sender);
            if (itSubscribers->second.empty()) {
                lock.RemoteSubscribers.erase(itSubscribers);
                auto itSession = Sessions.find(ev->InterconnectSession);
                if (itSession != Sessions.end()) {
                    itSession->second.SubscribedLocks.erase(lockId);
                }
            }
        }
    } else {
        lock.LocalSubscribers.erase(ev->Sender);
    }
}

void TLongTxServiceActor::SendViaSession(const TActorId& sessionId, const TActorId& recipient,
        IEventBase* event, ui32 flags, ui64 cookie)
{
    auto ev = MakeHolder<IEventHandle>(recipient, SelfId(), event, flags, cookie);
    if (sessionId) {
        ev->Rewrite(TEvInterconnect::EvForward, sessionId);
    }
    TActivationContext::Send(ev.Release());
}

void TLongTxServiceActor::SendReply(ERequestType type, TActorId sender, ui64 cookie,
        Ydb::StatusIds::StatusCode status, TStringBuf details)
{
    NYql::TIssues issues;
    issues.AddIssue(details);
    SendReplyIssues(type, sender, cookie, status, issues);
}

void TLongTxServiceActor::SendReplyIssues(ERequestType type, TActorId sender, ui64 cookie,
        Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues)
{
    switch (type) {
        case ERequestType::Commit:
            Send(sender, new TEvLongTxService::TEvCommitTxResult(status, issues), 0, cookie);
            break;
        case ERequestType::Rollback:
            Send(sender, new TEvLongTxService::TEvRollbackTxResult(status, issues), 0, cookie);
            break;
        case ERequestType::AttachColumnShardWrites:
            Send(sender, new TEvLongTxService::TEvAttachColumnShardWritesResult(status, issues), 0, cookie);
            break;
    }
}

void TLongTxServiceActor::SendReplyUnavailable(ERequestType type, TActorId sender, ui64 cookie, TStringBuf details) {
    SendReply(type, sender, cookie, Ydb::StatusIds::UNAVAILABLE, details);
}

TLongTxServiceActor::TProxyNodeState& TLongTxServiceActor::ConnectProxyNode(ui32 nodeId) {
    auto& node = ProxyNodes[nodeId];
    if (node.NodeId == 0) {
        node.NodeId = nodeId;
    }
    if (node.State == EProxyState::Disconnected) {
        // Node will be left in Disconnected state if there's no proxy
        if (auto proxy = TActivationContext::InterconnectProxy(nodeId)) {
            Send(proxy, new TEvInterconnect::TEvConnectNode(), IEventHandle::FlagTrackDelivery, nodeId);
            node.State = EProxyState::Connecting;
        }
    }
    return node;
}

void TLongTxServiceActor::SendProxyRequest(ui32 nodeId, ERequestType type, THolder<IEventHandle> ev) {
    auto& node = ConnectProxyNode(nodeId);
    if (node.State == EProxyState::Disconnected) {
        return SendReplyUnavailable(type, ev->Sender, ev->Cookie, "Cannot forward request: node unknown");
    }

    ui64 cookie = ++LastCookie;
    auto& req = node.ActiveRequests[cookie];
    req.Type = type;
    req.State = ERequestState::Pending;
    req.Sender = ev->Sender;
    req.Cookie = ev->Cookie;

    // Construct a new event
    THolder<IEventHandle> pendingEv;
    auto target = MakeLongTxServiceID(nodeId);
    auto flags = IEventHandle::FlagTrackDelivery;
    if (ev->HasBuffer()) {
        auto type = ev->GetTypeRewrite();
        auto buffer = ev->ReleaseChainBuffer();
        pendingEv.Reset(new IEventHandle(type, flags, target, SelfId(), std::move(buffer), cookie));
    } else {
        auto event = ev->ReleaseBase();
        pendingEv.Reset(new IEventHandle(target, SelfId(), event.Release(), flags, cookie));
    }
    Y_ABORT_UNLESS(pendingEv->Recipient.NodeId() == nodeId);

    if (node.State == EProxyState::Connecting) {
        auto& pending = node.Pending.emplace_back();
        pending.Ev = std::move(pendingEv);
        pending.Request = &req;
        return;
    }

    Y_DEBUG_ABORT_UNLESS(node.State == EProxyState::Connected);
    pendingEv->Rewrite(TEvInterconnect::EvForward, node.Session);
    TActivationContext::Send(pendingEv.Release());
    req.State = ERequestState::Sent;
}

void TLongTxServiceActor::Handle(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    const ui32 nodeId = ev->Get()->NodeId;
    TXLOG_DEBUG("Received TEvNodeConnected for NodeId# " << nodeId << " from session " << ev->Sender);

    auto itNode = ProxyNodes.find(nodeId);
    if (itNode == ProxyNodes.end()) {
        return;
    }

    auto& node = itNode->second;
    if (node.State != EProxyState::Connecting) {
        return;
    }

    node.State = EProxyState::Connected;
    node.Session = ev->Sender;

    auto pending = std::move(node.Pending);
    node.Pending.clear();
    for (auto& req : pending) {
        req.Ev->Rewrite(TEvInterconnect::EvForward, node.Session);
        TActivationContext::Send(req.Ev.Release());
        req.Request->State = ERequestState::Sent;
    }

    // Send subscription requests for all remote locks
    for (auto& pr : node.Locks) {
        const ui64 lockId = pr.first;
        auto& lock = pr.second;
        lock.Cookie = ++LastCookie;
        node.CookieToLock[lock.Cookie] = lock.Cookie;
        SendViaSession(
            node.Session, MakeLongTxServiceID(nodeId),
            new TEvLongTxService::TEvSubscribeLock(lockId, nodeId),
            IEventHandle::FlagTrackDelivery, lock.Cookie);
    }
}

void TLongTxServiceActor::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    const ui32 nodeId = ev->Get()->NodeId;
    TXLOG_DEBUG("Received TEvNodeDisconnected for NodeId# " << nodeId << " from session " << ev->Sender);
    OnNodeDisconnected(nodeId, ev->Sender);
}

void TLongTxServiceActor::OnNodeDisconnected(ui32 nodeId, const TActorId& sender) {
    auto itNode = ProxyNodes.find(nodeId);
    if (itNode == ProxyNodes.end()) {
        return;
    }

    auto& node = itNode->second;
    if (node.Session && node.Session != sender) {
        return;
    }

    if (node.ActiveRequests) {
        NYql::TIssues issuesPending;
        issuesPending.AddIssue("Cannot forward request: node disconnected");
        NYql::TIssues issuesSent;
        issuesSent.AddIssue("Unknown state of forwarded request: node disconnected");

        auto isRetriable = [](const TProxyRequestState& req) -> bool {
            if (req.State == ERequestState::Pending) {
                return true; // request not sent yet, always retriable
            }
            switch (req.Type) {
                case ERequestType::Commit:
                case ERequestType::Rollback:
                    return false;
                case ERequestType::AttachColumnShardWrites:
                    return true;
            }
        };

        auto active = std::move(node.ActiveRequests);
        node.ActiveRequests.clear();
        for (auto& pr : active) {
            auto& req = pr.second;
            const auto status = isRetriable(req) ? Ydb::StatusIds::UNAVAILABLE : Ydb::StatusIds::UNDETERMINED;
            const auto& issues = req.State == ERequestState::Pending ? issuesPending : issuesSent;
            SendReplyIssues(req.Type, req.Sender, req.Cookie, status, issues);
        }
    }

    if (node.Pending) {
        auto pending = std::move(node.Pending);
        node.Pending.clear();
    }

    TMonotonic now = TActivationContext::Monotonic();

    for (auto& pr : node.Locks) {
        auto& lock = pr.second;
        // For each lock remember the first time it became unavailable
        if (lock.State != EProxyLockState::Unavailable) {
            lock.State = EProxyLockState::Unavailable;
            lock.ExpiresAt = now + RemoteLockTimeout;
            node.LockExpireQueue.Add(&lock);
        }
        // Forget subscribe requests that have been in-flight
        if (lock.Cookie != 0) {
            node.CookieToLock.erase(lock.Cookie);
            lock.Cookie = 0;
        }
    }

    // See which locks are unavailable for more than timeout
    while (auto* lock = node.LockExpireQueue.Top()) {
        if (now < lock->ExpiresAt) {
            break;
        }
        RemoveUnavailableLock(node, *lock);
    }

    if (node.Locks.empty()) {
        // Remove an unnecessary node structure
        ProxyNodes.erase(itNode);
        return;
    }

    node.State = EProxyState::Disconnected;
    node.Session = {};

    // TODO: faster retries with exponential backoff?
    Schedule(TDuration::MilliSeconds(100), new TEvPrivate::TEvReconnect(nodeId));
}

void TLongTxServiceActor::Handle(TEvPrivate::TEvReconnect::TPtr& ev) {
    ui32 nodeId = ev->Get()->NodeId;

    auto& node = ConnectProxyNode(nodeId);
    if (node.State == EProxyState::Disconnected) {
        // TODO: handle proxy disappearing from interconnect?
    }
}

void TLongTxServiceActor::Handle(TEvents::TEvUndelivered::TPtr& ev) {
    auto* msg = ev->Get();
    TXLOG_DEBUG("Received TEvUndelivered from " << ev->Sender << " cookie " << ev->Cookie
        << " type " << msg->SourceType
        << " reason " << msg->Reason
        << " session " << ev->InterconnectSession);

    if (msg->SourceType == TEvInterconnect::EvConnectNode) {
        return OnNodeDisconnected(ev->Cookie, ev->Sender);
    }

    // InterconnectSession will be set if we received this notification from
    // a remote node, as opposed to a local notification when message is not
    // delivered to interconnect session itself. Session problems are handled
    // by separate disconnect notifications.
    // FIXME: currently interconnect mock is broken, so we assume all other
    // undelivery notifications are coming over the network. It should be
    // safe to do, since each request uses a unique cookie that is only valid
    // while session is connected.
    if (!ev->InterconnectSession && !InterconnectUndeliveryBroken) {
        return;
    }

    auto nodeId = ev->Sender.NodeId();
    auto itNode = ProxyNodes.find(nodeId);
    if (itNode == ProxyNodes.end()) {
        return;
    }

    auto& node = itNode->second;

    if (msg->SourceType == TEvLongTxService::EvSubscribeLock) {
        auto itCookie = node.CookieToLock.find(ev->Cookie);
        if (itCookie == node.CookieToLock.end()) {
            return;
        }

        ui64 lockId = itCookie->second;
        auto itLock = node.Locks.find(lockId);
        if (itLock == node.Locks.end()) {
            return;
        }

        auto& lock = itLock->second;
        RemoveUnavailableLock(node, lock);
        return;
    }

    auto itReq = node.ActiveRequests.find(ev->Cookie);
    if (itReq == node.ActiveRequests.end()) {
        return;
    }
    auto& req = itReq->second;
    SendReplyUnavailable(req.Type, req.Sender, req.Cookie, "Cannot forward request: service unavailable");
    node.ActiveRequests.erase(itReq);
}

void TLongTxServiceActor::RemoveUnavailableLock(TProxyNodeState& node, TProxyLockState& lock) {
    const ui32 nodeId = node.NodeId;
    const ui64 lockId = lock.LockId;

    for (auto& pr : lock.RepliedSubscribers) {
        Send(pr.first,
            new TEvLongTxService::TEvLockStatus(
                lockId, nodeId,
                NKikimrLongTxService::TEvLockStatus::STATUS_UNAVAILABLE),
            0, pr.second);
    }

    for (auto& pr : lock.NewSubscribers) {
        Send(pr.first,
            new TEvLongTxService::TEvLockStatus(
                lockId, nodeId,
                NKikimrLongTxService::TEvLockStatus::STATUS_UNAVAILABLE),
            0, pr.second);
    }

    if (node.LockExpireQueue.Has(&lock)) {
        node.LockExpireQueue.Remove(&lock);
    }

    if (lock.Cookie != 0) {
        node.CookieToLock.erase(lock.Cookie);
        lock.Cookie = 0;
    }

    node.Locks.erase(lockId);
}

} // namespace NLongTxService
} // namespace NKikimr
