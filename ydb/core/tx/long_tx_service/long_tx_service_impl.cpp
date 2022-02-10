#include "long_tx_service_impl.h"

#include <ydb/core/base/appdata.h>
#include <library/cpp/actors/core/log.h>
#include <util/string/builder.h>

#define TXLOG_LOG(priority, stream) \
    LOG_LOG_S(*TlsActivationContext, priority, NKikimrServices::LONG_TX_SERVICE, LogPrefix << stream)
#define TXLOG_DEBUG(stream) TXLOG_LOG(NActors::NLog::PRI_DEBUG, stream)
#define TXLOG_NOTICE(stream) TXLOG_LOG(NActors::NLog::PRI_NOTICE, stream)
#define TXLOG_ERROR(stream) TXLOG_LOG(NActors::NLog::PRI_ERROR, stream)

namespace NKikimr {
namespace NLongTxService {

static constexpr size_t MaxAcquireSnapshotInFlight = 4;
static constexpr TDuration AcquireSnapshotBatchDelay = TDuration::MicroSeconds(500);

void TLongTxServiceActor::Bootstrap() {
    LogPrefix = TStringBuilder() << "TLongTxService [Node " << SelfId().NodeId() << "] ";
    Become(&TThis::StateWork);
}

void TLongTxServiceActor::HandlePoison() {
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
            Y_VERIFY(!Transactions.contains(txId.UniqueId));
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

    Y_VERIFY(tx.State == ETxState::Active);
    Y_VERIFY(!tx.CommitActor);
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
    Y_VERIFY(it != Transactions.end());
    auto& tx = it->second;
    Y_VERIFY_DEBUG(tx.TxId == msg->TxId);

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
        ui64 shardId = write.GetColumnShard();
        ui64 writeId = write.GetWriteId();
        auto it = tx.ColumnShardWrites.find(shardId);
        if (it == tx.ColumnShardWrites.end()) {
            tx.ColumnShardWrites[shardId] = writeId;
            continue;
        }
        if (it->second == writeId) {
            continue;
        }
        return SendReply(ERequestType::AttachColumnShardWrites, ev->Sender, ev->Cookie,
            Ydb::StatusIds::GENERIC_ERROR, "Shard write id change detected, transaction may be broken");
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
        Y_VERIFY(appData);
        Y_VERIFY(appData->DomainsInfo);
        // Usually there's exactly one domain
        if (appData->EnableMvccSnapshotWithLegacyDomainRoot && Y_LIKELY(appData->DomainsInfo->Domains.size() == 1)) {
            DefaultDatabaseName = appData->DomainsInfo->Domains.begin()->second->Name;
        }
    }

    return DefaultDatabaseName;
}

void TLongTxServiceActor::Handle(TEvLongTxService::TEvAcquireReadSnapshot::TPtr& ev) {
    const auto* msg = ev->Get();
    const TString& databaseName = GetDatabaseNameOrLegacyDefault(msg->Record.GetDatabaseName());
    TXLOG_DEBUG("Received TEvAcquireReadSnapshot from " << ev->Sender << " for database " << databaseName);

    if (databaseName.empty()) {
        NYql::TIssues issues;
        issues.AddIssue("Cannot acquire snapshot for an unspecified database");
        Send(ev->Sender, new TEvLongTxService::TEvAcquireReadSnapshotResult(Ydb::StatusIds::SCHEME_ERROR, std::move(issues)), 0, ev->Cookie);
        return;
    }

    // TODO: we need to filter allowed databases

    auto& state = DatabaseSnapshots[databaseName];
    {
        auto& req = state.PendingUserRequests.emplace_back();
        req.Sender = ev->Sender;
        req.Cookie = ev->Cookie;
    }
    ScheduleAcquireSnapshot(databaseName, state);
}

void TLongTxServiceActor::ScheduleAcquireSnapshot(const TString& databaseName, TDatabaseSnapshotState& state) {
    Y_VERIFY(state.PendingUserRequests || state.PendingBeginTxRequests);

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
    Y_VERIFY(state.FlushPending);
    Y_VERIFY(state.PendingUserRequests || state.PendingBeginTxRequests);
    state.FlushPending = false;

    StartAcquireSnapshotActor(msg->DatabaseName, state);
}

void TLongTxServiceActor::Handle(TEvPrivate::TEvAcquireSnapshotFinished::TPtr& ev) {
    const auto* msg = ev->Get();
    TXLOG_DEBUG("Received TEvAcquireSnapshotFinished, cookie = " << ev->Cookie);

    auto* req = AcquireSnapshotInFlight.FindPtr(ev->Cookie);
    Y_VERIFY(req, "Unexpected reply for request that is not inflight");
    TString databaseName = req->DatabaseName;

    auto* state = DatabaseSnapshots.FindPtr(databaseName);
    Y_VERIFY(state && state->ActiveRequests.contains(ev->Cookie), "Unexpected database snapshot state");

    if (msg->Status == Ydb::StatusIds::SUCCESS) {
        for (auto& userReq : req->UserRequests) {
            Send(userReq.Sender, new TEvLongTxService::TEvAcquireReadSnapshotResult(databaseName, msg->Snapshot), 0, userReq.Cookie);
        }
        for (auto& beginReq : req->BeginTxRequests) {
            auto txId = beginReq.TxId;
            txId.Snapshot = msg->Snapshot;
            if (txId.IsWritable()) {
                Y_VERIFY(!Transactions.contains(txId.UniqueId));
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
            Send(userReq.Sender, new TEvLongTxService::TEvAcquireReadSnapshotResult(msg->Status, msg->Issues), 0, userReq.Cookie);
        }
        for (auto& beginReq : req->BeginTxRequests) {
            Send(beginReq.Sender, new TEvLongTxService::TEvBeginTxResult(msg->Status, msg->Issues), 0, beginReq.Cookie);
        }
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

void TLongTxServiceActor::SendProxyRequest(ui32 nodeId, ERequestType type, THolder<IEventHandle> ev) {
    auto& node = ProxyNodes[nodeId];
    if (node.State == EProxyState::Unknown) {
        auto proxy = TActivationContext::InterconnectProxy(nodeId);
        if (!proxy) {
            return SendReplyUnavailable(type, ev->Sender, ev->Cookie, "Cannot forward request: node unknown");
        }
        Send(proxy, new TEvInterconnect::TEvConnectNode(), IEventHandle::FlagTrackDelivery, nodeId);
        node.State = EProxyState::Connecting;
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
    Y_VERIFY(pendingEv->Recipient.NodeId() == nodeId);

    if (node.State == EProxyState::Connecting) {
        auto& pending = node.Pending.emplace_back();
        pending.Ev = std::move(pendingEv);
        pending.Request = &req;
        return;
    }

    Y_VERIFY_DEBUG(node.State == EProxyState::Connected);
    pendingEv->Rewrite(TEvInterconnect::EvForward, node.Session);
    TActivationContext::Send(pendingEv.Release());
    req.State = ERequestState::Sent;
}

void TLongTxServiceActor::Handle(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    auto it = ProxyNodes.find(ev->Get()->NodeId);
    if (it == ProxyNodes.end()) {
        return;
    }
    auto& node = it->second;
    if (node.State != EProxyState::Connecting) {
        return;
    }
    node.State = EProxyState::Connected;
    node.Session = ev->Sender;
    auto pending = std::move(node.Pending);
    Y_VERIFY_DEBUG(node.Pending.empty());
    for (auto& req : pending) {
        req.Ev->Rewrite(TEvInterconnect::EvForward, node.Session);
        TActivationContext::Send(req.Ev.Release());
        req.Request->State = ERequestState::Sent;
    }
}

void TLongTxServiceActor::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    OnNodeDisconnected(ev->Get()->NodeId, ev->Sender);
}

void TLongTxServiceActor::OnNodeDisconnected(ui32 nodeId, const TActorId& sender) {
    auto it = ProxyNodes.find(nodeId);
    if (it == ProxyNodes.end()) {
        return;
    }
    auto& node = it->second;
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

        for (auto& pr : node.ActiveRequests) {
            auto& req = pr.second;
            const auto status = isRetriable(req) ? Ydb::StatusIds::UNAVAILABLE : Ydb::StatusIds::UNDETERMINED;
            const auto& issues = req.State == ERequestState::Pending ? issuesPending : issuesSent;
            SendReplyIssues(req.Type, req.Sender, req.Cookie, status, issues);
        }
    }
    ProxyNodes.erase(it);
}

void TLongTxServiceActor::Handle(TEvents::TEvUndelivered::TPtr& ev) {
    if (ev->Get()->SourceType == TEvInterconnect::EvConnectNode) {
        return OnNodeDisconnected(ev->Cookie, ev->Sender);
    }
    auto nodeId = ev->Sender.NodeId();
    auto it = ProxyNodes.find(nodeId);
    if (it == ProxyNodes.end()) {
        return;
    }
    auto& node = it->second;
    auto itReq = node.ActiveRequests.find(ev->Cookie);
    if (itReq == node.ActiveRequests.end()) {
        return;
    }
    auto& req = itReq->second;
    SendReplyUnavailable(req.Type, req.Sender, req.Cookie, "Cannot forward request: service unavailable");
    node.ActiveRequests.erase(itReq);
}

} // namespace NLongTxService
} // namespace NKikimr
