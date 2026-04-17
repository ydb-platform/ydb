#include "long_tx_service_impl.h"
#include "lwtrace_probes.h"
#include "snapshots_exchange.h"

#include <util/string/builder.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_handle.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>
#include <atomic>

#define TXLOG_LOG(priority, stream) \
    LOG_LOG_S(*TlsActivationContext, priority, NKikimrServices::LONG_TX_SERVICE, LogPrefix << stream)
#define TXLOG_DEBUG(stream) TXLOG_LOG(NActors::NLog::PRI_DEBUG, stream)
#define TXLOG_NOTICE(stream) TXLOG_LOG(NActors::NLog::PRI_NOTICE, stream)
#define TXLOG_WARN(stream) TXLOG_LOG(NActors::NLog::PRI_WARN, stream)
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

    TSnapshotExchangeCounters snapshotExchangeCounters;
    if (Settings.Counters) {
        snapshotExchangeCounters.SnapshotsCollectionTimeMs = Settings.Counters->SnapshotsCollectionTimeMs;
        snapshotExchangeCounters.SnapshotsPropagationTimeMs = Settings.Counters->SnapshotsPropagationTimeMs;
        snapshotExchangeCounters.TimeSinceLastRemoteSnapshotsUpdateMs = Settings.Counters->TimeSinceLastRemoteSnapshotsUpdateMs;
    }

    auto* snapshotExchangeActor = CreateSnapshotExchangeActor(
        LocalSnapshotsStorage,
        RemoteSnapshotsStorage,
        snapshotExchangeCounters);
    SnapshotsExchangeActorId = RegisterWithSameMailbox(snapshotExchangeActor);
    Send(SelfId(), new TEvPrivate::TEvSnapshotMaintenance());

    TXLOG_NOTICE("Started, SelfId: " << SelfId());
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
    if (SnapshotsExchangeActorId) {
        Send(SnapshotsExchangeActorId, new TKikimrEvents::TEvPoison());
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
    const TString& databaseName = GetDatabaseNameOrLegacyDefault(msg->DatabaseName);
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
        req.TableIds = std::move(msg->TableIds);
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
        const auto now = AppData()->TimeProvider->Now();
        for (auto& userReq : req->UserRequests) {
            auto snapshotHandle = [&]() {
                if (AppData()->FeatureFlags.GetEnableSnapshotsLocking()) {
                    TLocalSnapshotInfo snapshotInfo(msg->Snapshot, userReq.Sender, std::move(userReq.TableIds), now);
                    NKqp::TSnapshotHandle snapshotHandle(snapshotInfo.AliveFlag);
                    LocalSnapshotsStorage->Insert(std::move(snapshotInfo));

                    return std::move(snapshotHandle);
                } else {
                    return NKqp::TSnapshotHandle{};
                }
            }();
            
            LWTRACK(AcquireReadSnapshotSuccess, userReq.Orbit, msg->Snapshot.Step, msg->Snapshot.TxId);
            Send(userReq.Sender, new TEvLongTxService::TEvAcquireReadSnapshotResult(databaseName, msg->Snapshot, std::move(snapshotHandle), std::move(userReq.Orbit)), 0, userReq.Cookie);
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
    TInstant lockTimestamp = msg->LockTimestamp;
    TXLOG_DEBUG("Received TEvRegisterLock for LockId# " << lockId << " LockTimestamp# " << lockTimestamp);

    Y_ABORT_UNLESS(lockId, "Unexpected registration of a zero LockId");

    auto& lock = Locks.try_emplace(lockId, lockId).first->second;
    ++lock.RefCount;
    if (lockTimestamp && !lock.Timestamp) {
        lock.Timestamp = lockTimestamp;
    }
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

        UnlinkWaitNode(lock.WaitNode);

        Locks.erase(it);
    }
}

TLongTxServiceActor::TProxyLockState&
TLongTxServiceActor::SubscribeToProxyLock(TProxyNodeState& node, ui64 lockId) {
    auto emplaceRes = node.Locks.try_emplace(lockId, lockId, node);
    if (Settings.Counters && emplaceRes.second) {
        Settings.Counters->RemoteLockSubscriptions->Inc();
    }
    auto& lock = emplaceRes.first->second;
    if (lock.State == EProxyLockState::Subscribed) {
        return lock;
    }

    // Send subscription request immediately if node is already connected
    if (node.State == EProxyState::Connected && lock.Cookie == 0) {
        lock.Cookie = ++LastCookie;
        node.CookieToLock[lock.Cookie] = lockId;

        auto subscribeEv = MakeHolder<TEvLongTxService::TEvSubscribeLock>(lockId, node.NodeId);
        for (const auto& edge : lock.WaitNode.Blockers) {
            if (edge.Id.OwnerId.NodeId() == SelfId().NodeId()) {
                subscribeEv->AddLocalWaitEdge(edge.Id, edge.Blocker.LockInfo(SelfId()));
            }
        }

        SendViaSession(
            node.Session, MakeLongTxServiceID(node.NodeId),
            subscribeEv.Release(), IEventHandle::FlagTrackDelivery, lock.Cookie);
    }

    // Otherwise the TEvSubscribeLock will be sent in the TEvNodeConnected handler.
    return lock;
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

        auto& lock = SubscribeToProxyLock(node, lockId);

        if (lock.State == EProxyLockState::Subscribed) {
            Send(ev->Sender,
                new TEvLongTxService::TEvLockStatus(
                    lockId, lockNode,
                    NKikimrLongTxService::TEvLockStatus::STATUS_SUBSCRIBED,
                    lock.Timestamp),
                0, ev->Cookie);
            lock.RepliedSubscribers[ev->Sender] = ev->Cookie;
        } else {
            lock.NewSubscribers[ev->Sender] = ev->Cookie;
            lock.RepliedSubscribers.erase(ev->Sender);
        }

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

    auto statusEv = MakeHolder<TEvLongTxService::TEvLockStatus>(
        lockId, lockNode,
        NKikimrLongTxService::TEvLockStatus::STATUS_SUBSCRIBED,
        lock.Timestamp);

    if (ev->InterconnectSession) {
        // Sync wait edges of the local lock that are local to the sender node, and notify subscribers.
        // Do it before we add the new subscriber because it does not need this update.

        SyncLockWaitEdgesSubset(
            TLockStateHandle{lock}, record.GetLocalWaitEdges(),
            [&](const TWaitEdgeId& id) { return id.OwnerId.NodeId() == ev->Sender.NodeId(); });

        // Now add the new subscriber.

        auto& session = SubscribeToSession(ev->InterconnectSession);
        session.SubscribedLocks.insert(lockId);
        lock.RemoteSubscribers[ev->InterconnectSession][ev->Sender] = ev->Cookie;

        for (const auto& edge : lock.WaitNode.Blockers) {
            if (edge.Id.OwnerId.NodeId() != ev->Sender.NodeId()) {
                // Send edges that are not local to the requester.
                statusEv->AddWaitEdge(
                    edge.Id, edge.Blocker.LockInfo(SelfId()));
            }
        }
    } else {
        lock.LocalSubscribers[ev->Sender] = ev->Cookie;
    }

    SendViaSession(
        ev->InterconnectSession, ev->Sender,
        statusEv.Release(), 0, ev->Cookie);
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvLockStatus::TPtr& ev) {
    auto* msg = ev->Get();
    auto& record = msg->Record;
    ui64 lockId = record.GetLockId();
    ui32 lockNode = record.GetLockNode();
    auto lockStatus = record.GetStatus();
    auto lockTimestamp = msg->GetLockTimestamp();
    TXLOG_DEBUG("Received TEvLockStatus from " << ev->Sender
            << " for LockId# " << lockId << " LockNode# " << lockNode
            << " LockStatus# " << lockStatus << " LockTimestamp# " << lockTimestamp);

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
        if (lockTimestamp && !lock.Timestamp) {
            lock.Timestamp = lockTimestamp;
        }
        for (auto& pr : lock.NewSubscribers) {
            Send(pr.first,
                new TEvLongTxService::TEvLockStatus(lockId, lockNode, lockStatus, lock.Timestamp),
                0, pr.second);
            lock.RepliedSubscribers[pr.first] = pr.second;
        }
        lock.NewSubscribers.clear();

        // Sync wait edges of the proxy lock except those that are local to us, and subscribe to locks
        // down the wait tree.

        SyncLockWaitEdgesSubset(
            TLockStateHandle{lock}, record.GetWaitEdges(),
            [&](const TWaitEdgeId& id) { return id.OwnerId.NodeId() != SelfId().NodeId(); });

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

    UnlinkWaitNode(lock.WaitNode);

    node->CookieToLock.erase(lock.Cookie);
    node->Locks.erase(itLock);
    if (Settings.Counters) {
        Settings.Counters->RemoteLockSubscriptions->Dec();
    }
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
            if (Settings.Counters) {
                Settings.Counters->RemoteLockSubscriptions->Dec();
            }
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

        auto subscribeEv = MakeHolder<TEvLongTxService::TEvSubscribeLock>(lockId, nodeId);
        for (const auto& edge : lock.WaitNode.Blockers) {
            if (edge.Id.OwnerId.NodeId() == SelfId().NodeId()) {
                subscribeEv->AddLocalWaitEdge(edge.Id, edge.Blocker.LockInfo(SelfId()));
            }
        }

        SendViaSession(
            node.Session, MakeLongTxServiceID(nodeId),
            subscribeEv.Release(), IEventHandle::FlagTrackDelivery, lock.Cookie);
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

    UnlinkWaitNode(lock.WaitNode);

    const bool erased = node.Locks.erase(lockId);
    if (Settings.Counters && erased) {
        Settings.Counters->RemoteLockSubscriptions->Dec();
    }
}

TLongTxServiceActor::TLockStateHandle TLongTxServiceActor::GetAwaiterHandle(const TLockInfo& awaiterInfo) {
    if (awaiterInfo.LockNodeId == SelfId().NodeId()) {
        auto lockIt = Locks.find(awaiterInfo.LockId);
        if (lockIt == Locks.end()) {
            TXLOG_WARN("Local awaiter id: " << awaiterInfo.LockId << " not found");
            return TLockStateHandle{};
        }
        return TLockStateHandle(lockIt->second);
    } else {
        auto& node = ConnectProxyNode(awaiterInfo.LockNodeId);
        if (node.State == EProxyState::Disconnected) {
            TXLOG_WARN("Proxy node for remote awaiter id: " << awaiterInfo << " not found");
            return TLockStateHandle{};
        }

        auto lockIt = node.Locks.find(awaiterInfo.LockId);
        if (lockIt == node.Locks.end()) {
            TXLOG_WARN("Proxy lock for remote awaiter id: " << awaiterInfo << " not found");
            return TLockStateHandle{};
        }
        return TLockStateHandle(lockIt->second);
    }
}

void TLongTxServiceActor::UpdateLockWaitEdges(
        TLockStateHandle awaiter, const TVector<TWaitEdgeInfo>& added, const TVector<TWaitEdgeId>& removed) {
    TLockInfo awaiterInfo = awaiter.LockInfo(SelfId());

    // 1. Update the local graph.

    bool deadlockPossible = false;
    TVector<TWaitEdgeInfo> actuallyAdded;
    for (const auto& addedEdge : added) {
        auto existingIt = WaitEdges.find(addedEdge.Id);
        if (existingIt != WaitEdges.end()) {
            if (existingIt->second.Blocker.LockInfo(SelfId()) != addedEdge.Blocker) {
                TXLOG_ERROR("Unexpected blocker: " << existingIt->second.Blocker.LockInfo(SelfId())
                    << " for duplicate added edge id: " << addedEdge.Id
                    << ", expected: " << addedEdge.Blocker.LockId);
            }
            continue;
        }

        TLockStateHandle blocker;
        if (addedEdge.Blocker.LockNodeId == SelfId().NodeId()) {
            auto lockIt = Locks.find(addedEdge.Blocker.LockId);
            if (lockIt == Locks.end()) {
                continue;
            }
            blocker = TLockStateHandle{lockIt->second};
        } else {
            auto& node = ConnectProxyNode(addedEdge.Blocker.LockNodeId);
            if (node.State == EProxyState::Disconnected) {
                continue;
            }

            auto lockIt = node.Locks.find(addedEdge.Blocker.LockId);
            if (lockIt != node.Locks.end()) {
                blocker = TLockStateHandle(lockIt->second);
            } else {
                blocker = TLockStateHandle(SubscribeToProxyLock(node, addedEdge.Blocker.LockId));
            }
        }

        auto insertHappened = WaitEdges.try_emplace(addedEdge.Id, addedEdge.Id, awaiter, blocker).second;
        Y_ABORT_UNLESS(insertHappened);
        actuallyAdded.push_back(addedEdge);

        if (!awaiter.WaitNode().Island && !blocker.WaitNode().Island) {
            // Create an island of two
            auto* island = new TLockIsland;
            LockIslands.PushBack(island);
            island->Locks.PushBack(&awaiter.WaitNode());
            island->Locks.PushBack(&blocker.WaitNode());
            island->LocksCount = 2;
            awaiter.WaitNode().Island = island;
            blocker.WaitNode().Island = island;
        } else if (!awaiter.WaitNode().Island && blocker.WaitNode().Island) {
            blocker.WaitNode().Island->Locks.PushBack(&awaiter.WaitNode());
            ++blocker.WaitNode().Island->LocksCount;
            awaiter.WaitNode().Island = blocker.WaitNode().Island;
        } else if (awaiter.WaitNode().Island && !blocker.WaitNode().Island) {
            awaiter.WaitNode().Island->Locks.PushBack(&blocker.WaitNode());
            ++awaiter.WaitNode().Island->LocksCount;
            blocker.WaitNode().Island = awaiter.WaitNode().Island;
        } else if (awaiter.WaitNode().Island != blocker.WaitNode().Island) {
            // Merge smaller island into the bigger one.
            auto [bigger, smaller] = std::pair(awaiter.WaitNode().Island, blocker.WaitNode().Island);
            if (bigger->LocksCount < smaller->LocksCount) {
                std::swap(bigger, smaller);
            }
            for (auto& lock : smaller->Locks) {
                lock.Island = bigger;
            }
            bigger->Locks.Append(smaller->Locks);
            bigger->LocksCount += smaller->LocksCount;
            delete smaller;
        } else {
            // Awaiter and blocker belong to the same island, deadlock is possible.
            deadlockPossible = true;
        }

        if (Settings.Counters) {
            Settings.Counters->WaitGraphEdges->Inc();
            if (addedEdge.Id.OwnerId.NodeId() == SelfId().NodeId()) {
                Settings.Counters->LocalWaitGraphEdges->Inc();
            }
        }

        TXLOG_DEBUG("Added wait edge id: " << addedEdge.Id
            << ", awaiter: " << awaiterInfo
            << ", blocker: " << addedEdge.Blocker);
    }

    TVector<TWaitEdgeId> actuallyRemoved;
    for (const auto& id : removed) {
        auto it = WaitEdges.find(id);
        if (it == WaitEdges.end()) {
            continue;
        }
        Y_ABORT_UNLESS(it->second.Awaiter);
        Y_ABORT_UNLESS(it->second.Blocker);

        if (it->second.Awaiter.Impl != awaiter.Impl) {
            TXLOG_ERROR("Unexpected awaiter: " << awaiterInfo
                << " for removed edge id: " << id
                << ", expected: " << awaiter.LockInfo(SelfId()));
            continue;
        }

        auto blockerInfo = it->second.Blocker.LockInfo(SelfId());
        UnlinkWaitEdge(it->second);
        WaitEdges.erase(it);
        actuallyRemoved.push_back(id);

        if (Settings.Counters) {
            Settings.Counters->WaitGraphEdges->Dec();
            if (id.OwnerId.NodeId() == SelfId().NodeId()) {
                Settings.Counters->LocalWaitGraphEdges->Dec();
            }
        }

        TXLOG_DEBUG("Removed wait edge id: " << id
            << ", awaiter: " << awaiterInfo
            << ", blocker: " << blockerInfo);
    }

    // 2. Send notifications

    if (TLockState* localAwaiter = awaiter.LocalState()) {
        if (!actuallyAdded.empty() || !actuallyRemoved.empty()) {
            for (const auto& [sessionId, subscribers] : localAwaiter->RemoteSubscribers) {
                for (const auto& [subscriber, _] : subscribers) {
                    auto updateEv = MakeHolder<TEvLongTxService::TEvUpdateLockWaitEdges>(awaiterInfo);
                    for (const auto& edge : actuallyAdded) {
                        if (edge.Id.OwnerId.NodeId() != subscriber.NodeId()) {
                            updateEv->AddAddedEdge(edge.Id, edge.Blocker);
                        }
                    }
                    for (const auto& edgeId : actuallyRemoved) {
                        if (edgeId.OwnerId.NodeId() != subscriber.NodeId()) {
                            updateEv->AddRemovedEdge(edgeId);
                        }
                    }

                    if (Settings.Counters) {
                        Settings.Counters->WaitGraphEdgesSent->Add(
                            updateEv->Record.GetAdded().size() + updateEv->Record.GetRemoved().size());
                    }

                    if (!updateEv->Empty()) {
                        SendViaSession(
                            sessionId, subscriber, updateEv.Release(), IEventHandle::FlagTrackDelivery);
                    }
                }
            }
        }
    } else {
        // Notify remote lock about new local edges.
        TProxyLockState* proxyAwaiter = awaiter.ProxyState();
        auto& node = proxyAwaiter->ProxyNode;
        if (node.State == EProxyState::Connected) {
            auto updateEv = MakeHolder<TEvLongTxService::TEvUpdateLockWaitEdges>(awaiterInfo);
            for (const auto& edge : actuallyAdded) {
                if (edge.Id.OwnerId.NodeId() == SelfId().NodeId()) {
                    updateEv->AddAddedEdge(edge.Id, edge.Blocker);
                }
            }
            for (const auto& edgeId : actuallyRemoved) {
                if (edgeId.OwnerId.NodeId() == SelfId().NodeId()) {
                    updateEv->AddRemovedEdge(edgeId);
                }
            }

            if (Settings.Counters) {
                Settings.Counters->WaitGraphEdgesSent->Add(
                    updateEv->Record.GetAdded().size() + updateEv->Record.GetRemoved().size());
            }

            if (!updateEv->Empty()) {
                SendViaSession(
                    node.Session, MakeLongTxServiceID(node.NodeId),
                    updateEv.Release(), IEventHandle::FlagTrackDelivery);
            }
        }

        // Otherwise the edges will be sent when we re-subscribe to locks in the TEvNodeConnected handler.
    }

    // 3. Run deadlock detection
    if (deadlockPossible) {
        RunDeadlockDetection();
    }
}

template<typename TProtoList, typename TFilter>
void TLongTxServiceActor::SyncLockWaitEdgesSubset(
        TLockStateHandle awaiter, const TProtoList& newEdges, TFilter edgeFilter) {
    THashSet<TWaitEdgeId> prevWaitEdges;
    for (auto& edge : awaiter.WaitNode().Blockers) {
        if (edgeFilter(edge.Id)) {
            prevWaitEdges.insert(edge.Id);
        }
    }

    TVector<TWaitEdgeInfo> addedEdges;
    for (const auto& edge : newEdges) {
        TWaitEdgeId edgeId(ActorIdFromProto(edge.GetId().GetOwner()), edge.GetId().GetRequestId());
        if (edgeFilter(edgeId)) {
            auto prevIt = prevWaitEdges.find(edgeId);
            if (prevIt == prevWaitEdges.end()) {
                addedEdges.push_back(TWaitEdgeInfo{
                    .Id = edgeId,
                    .Blocker = TLockInfo(edge.GetBlockerLockId(), edge.GetBlockerLockNode()),
                });
            } else {
                prevWaitEdges.erase(prevIt);
            }
        }
    }

    TVector<TWaitEdgeId> removedEdges(prevWaitEdges.begin(), prevWaitEdges.end());

    UpdateLockWaitEdges(awaiter, addedEdges, removedEdges);
}

void TLongTxServiceActor::UnlinkWaitEdge(TWaitEdge& edge) {
    static_cast<TIntrusiveListItem<TWaitEdge, TTagAwaiter>&>(edge).Unlink();
    static_cast<TIntrusiveListItem<TWaitEdge, TTagBlocker>&>(edge).Unlink();

    auto awaiter = edge.Awaiter;
    auto blocker = edge.Blocker;
    for (auto lock : {awaiter, blocker}) {
        // We don't try to detect the case when the island is split into two
        // (too expensive and not necessary for correctness), except the case
        // when a lock becomes isolated.
        auto& wn = lock.WaitNode();
        if (wn.Island && wn.Awaiters.Empty() && wn.Blockers.Empty()) {
            wn.Unlink();
            --wn.Island->LocksCount;
            if (!wn.Island->LocksCount) {
                delete wn.Island;
            }
            wn.Island = nullptr;
        }
    }

    if (Settings.Counters) {
        Settings.Counters->WaitGraphEdges->Dec();
        if (edge.Id.OwnerId.NodeId() == SelfId().NodeId()) {
            Settings.Counters->LocalWaitGraphEdges->Dec();
        }
    }
}

void TLongTxServiceActor::UnlinkWaitNode(TWaitNode& waitNode) {
    auto remove = [&](TWaitEdge& edge) {
        UnlinkWaitEdge(edge);
        WaitEdges.erase(edge.Id); // Unlinks the edge from the list.
    };
    while (!waitNode.Awaiters.Empty()) {
        remove(*waitNode.Awaiters.Back());
    }
    while (!waitNode.Blockers.Empty()) {
        remove(*waitNode.Blockers.Back());
    }
    if (waitNode.Island) {
        --waitNode.Island->LocksCount;
        if (!waitNode.Island->LocksCount) {
            delete waitNode.Island;
        }
    }
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvWaitingLockAdd::TPtr& ev) {
    auto edgeId = TWaitEdgeId(ev->Sender, ev->Get()->RequestId);
    TXLOG_DEBUG("Received TEvWaitingLockAdd for awaiter: " << ev->Get()->Lock
        << ", blocker: " << ev->Get()->OtherLock
        << ", edge id: " << edgeId);

    auto awaiter = GetAwaiterHandle(ev->Get()->Lock);
    if (!awaiter) {
        return;
    }

    TVector<TWaitEdgeInfo> added {
        TWaitEdgeInfo {
            .Id = edgeId,
            .Blocker = ev->Get()->OtherLock,
        },
    };
    UpdateLockWaitEdges(awaiter, added, {});
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvWaitingLockRemove::TPtr& ev) {
    auto edgeId = TWaitEdgeId(ev->Sender, ev->Get()->RequestId);
    TXLOG_DEBUG("Received TEvWaitingLockRemove for edge id: " << edgeId);

    auto edgeIt = WaitEdges.find(edgeId);
    if (edgeIt == WaitEdges.end()) {
        return;
    }
    auto& edge = edgeIt->second;
    UpdateLockWaitEdges(edge.Awaiter, {}, {edgeId});
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvUpdateLockWaitEdges::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    TLockInfo awaiterInfo(record.GetLockId(), record.GetLockNode());

    TXLOG_DEBUG("Received TEvUpdateLockWaitEdges from " << ev->Sender
        << " for awaiter: " << awaiterInfo
        << ", added count: " << record.GetAdded().size()
        << ", removed count: " << record.GetRemoved().size());

    if (Settings.Counters) {
        Settings.Counters->WaitGraphEdgesReceived->Add(
            record.GetAdded().size() + record.GetRemoved().size());
    }

    auto awaiter = GetAwaiterHandle(awaiterInfo);
    if (!awaiter) {
        return;
    }

    TVector<TWaitEdgeInfo> addedEdges;
    for (const auto& added : ev->Get()->Record.GetAdded()) {
        TWaitEdgeId id(ActorIdFromProto(added.GetId().GetOwner()), added.GetId().GetRequestId());
        TLockInfo blocker(added.GetBlockerLockId(), added.GetBlockerLockNode());
        addedEdges.push_back(TWaitEdgeInfo{
            .Id = id,
            .Blocker = blocker,
        });
    }

    TVector<TWaitEdgeId> removedEdges;
    for (const auto& removedId : ev->Get()->Record.GetRemoved()) {
        TWaitEdgeId id(ActorIdFromProto(removedId.GetOwner()), removedId.GetRequestId());
        removedEdges.push_back(id);
    }

    UpdateLockWaitEdges(awaiter, addedEdges, removedEdges);
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvGetLockWaitGraph::TPtr& ev) {
    auto response = MakeHolder<TEvLongTxService::TEvGetLockWaitGraphResult>();
    response->WaitEdges.reserve(WaitEdges.size());
    for (const auto& [id, edge] : WaitEdges) {
        response->WaitEdges.push_back(TEvLongTxService::TEvGetLockWaitGraphResult::TWaitEdge{
            .Id = id,
            .Awaiter = edge.Awaiter.LockInfo(SelfId()),
            .Blocker = edge.Blocker.LockInfo(SelfId()),
        });
    }

    Send(ev->Sender, response.Release(), 0, ev->Cookie);
}


void TLongTxServiceActor::RunDeadlockDetection() {
    auto hashFunc = [](const TLockStateHandle& lh) { return lh.Hash(); };

    // The higher priority, the more likely we are to break the wait by this lock.
    // We want to abort wait edges of younger locks, so that older transactions can finish their work.
    auto lockPriorityCmp = [](const TLockStateHandle& left, const TLockStateHandle& right) {
        if (left.Timestamp() != right.Timestamp()) {
            return left.Timestamp() > right.Timestamp();
        }
        return left.LockId() > right.LockId();
    };

    THashSet<TLockStateHandle, decltype(hashFunc)> awaitersWithLocalEdges;
    for (const auto& [id, edge] : WaitEdges) {
        if (id.OwnerId.NodeId() == SelfId().NodeId()) {
            awaitersWithLocalEdges.insert(edge.Awaiter);
        }
    }

    TVector<TLockStateHandle> current;
    TVector<TLockStateHandle> next;
    THashMap<TLockStateHandle, TWaitEdge*, decltype(hashFunc)> prev;
    THashSet<TWaitEdge*> toBreak;
    for (const auto& start : awaitersWithLocalEdges) {
        current.clear();
        current.push_back(start);
        next.clear();
        prev.clear();

        while (!current.empty()) {
            bool found = false;
            for (const auto& node : current) {
                for (auto& edge : node.WaitNode().Blockers) {
                    if (edge.Blocker == start) {
                        // Found a cycle, now find the best edge to break.
                        found = true;
                        TWaitEdge* bestEdge = nullptr;
                        TWaitEdge* curEdge = &edge;
                        while (true) {
                            if (!bestEdge
                                || lockPriorityCmp(curEdge->Awaiter, bestEdge->Awaiter)) {
                                bestEdge = curEdge;
                            }

                            if (curEdge->Awaiter == start) {
                                break;
                            } else {
                                curEdge = prev.at(curEdge->Awaiter);
                            }
                        }

                        if (bestEdge && !bestEdge->Broken
                            && bestEdge->Id.OwnerId.NodeId() == SelfId().NodeId()) {
                            toBreak.insert(bestEdge);
                        }
                    } else {
                        if (prev.emplace(edge.Blocker, &edge).second) {
                            next.push_back(edge.Blocker);
                        }
                    }
                }
            }

            if (found) {
                break;
            }

            std::swap(current, next);
            next.clear();
        }
    }

    for (auto edge : toBreak) {
        TXLOG_DEBUG("Breaking the wait edge id: " << edge->Id
            << ", awaiter: " << edge->Awaiter.LockInfo(SelfId())
            << ", blocker: " << edge->Blocker.LockInfo(SelfId()));
        edge->Broken = true;
        Send(
            edge->Id.OwnerId,
            new TEvLongTxService::TEvWaitingLockDeadlock(edge->Id.RequestId));
        if (Settings.Counters) {
            Settings.Counters->WaitGraphEdgesBroken->Inc();
        }
    }

    if (!toBreak.empty()) {
        TVector<std::tuple<ui64, ui32, ui64, ui32, ui32>> edges;
        for (const auto& [id, edge] : WaitEdges) {
            edges.emplace_back(
                edge.Awaiter.LockId(), edge.Awaiter.LockNodeId(SelfId()),
                edge.Blocker.LockId(), edge.Blocker.LockNodeId(SelfId()),
                id.OwnerId.NodeId());
        }
        std::sort(edges.begin(), edges.end());

        TStringBuilder sb;
        for (const auto& [al, an, bl, bn, en] : edges) {
            sb << "(" << al << "," << an << ") -> (" << bl << "," << bn << ") (en:" << en << ")\n";
        }
        TXLOG_DEBUG("FFF WG\n" << sb);
    }
}

void TLongTxServiceActor::Handle(TEvPrivate::TEvSnapshotMaintenance::TPtr&) {
    UpdateImmutableSnapshotsRegistry();
    TXLOG_DEBUG("Scheduled next TEvSnapshotMaintenance event in "
        << AppData()->LongTxServiceConfig.GetSnapshotsRegistryUpdateIntervalSeconds() << " seconds");
    Schedule(
        TDuration::Seconds(AppData()->LongTxServiceConfig.GetSnapshotsRegistryUpdateIntervalSeconds()),
        new TEvPrivate::TEvSnapshotMaintenance());
}

void TLongTxServiceActor::UpdateImmutableSnapshotsRegistry() {    
    if (!AppData()->FeatureFlags.GetEnableSnapshotsLocking()) {
        TXLOG_DEBUG("Snapshots locking is disabled, clearing local and remote snapshots storage");
        LocalSnapshotsStorage->Clear();
        RemoteSnapshotsStorage->Clear();
        AppData()->SnapshotRegistryHolder->Set(nullptr);
        return;
    }

    LocalSnapshotsStorage->CleanExpired();
    if (!RemoteSnapshotsStorage->IsReady()) {
        TXLOG_DEBUG("Remote snapshots storage is not ready, skipping update");
        return;
    }

    auto registryBuilder = CreateImmutableSnapshotRegistryBuilder();
    registryBuilder->SetSnapshotBorder(RemoteSnapshotsStorage->GetBorder());

    size_t localSnapshotsCount = 0;
    for (const auto& snapshotInfo : LocalSnapshotsStorage->View()) {
        registryBuilder->AddSnapshot(snapshotInfo.TableIds, snapshotInfo.Snapshot);
        ++localSnapshotsCount;
    }

    size_t remoteSnapshotsCount = 0;
    for (const auto& remoteSnapshotInfo : RemoteSnapshotsStorage->View()) {
        registryBuilder->AddSnapshot(
            remoteSnapshotInfo.TableIds,
            remoteSnapshotInfo.Snapshot);
        ++remoteSnapshotsCount;
    }

    if (Settings.Counters) {
        Settings.Counters->RemoteSnapshotsInRegistry->Set(remoteSnapshotsCount);
    }

    AppData()->SnapshotRegistryHolder->Set(std::move(*registryBuilder).Build());
    TXLOG_DEBUG("Updated immutable snapshots registry. "
        << "Local snapshots count: " << localSnapshotsCount
        << ", Remote snapshots count: " << remoteSnapshotsCount);
}

} // namespace NLongTxService
} // namespace NKikimr
