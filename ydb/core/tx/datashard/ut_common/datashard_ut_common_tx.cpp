#include "datashard_ut_common_tx.h"

#include <ydb/core/tx/data_events/payload_helper.h>

namespace NKikimr::NDataShard::NTxHelpers {

ui64 AllocateTxId(TTestActorRuntime& runtime, const TActorId& sender) {
    ui32 nodeId = sender.NodeId();
    ui32 nodeIndex = nodeId - runtime.GetNodeId(0);
    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, new TEvTxUserProxy::TEvAllocateTxId), nodeIndex, true);
    auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvAllocateTxIdResult>(sender);
    auto* msg = ev->Get();
    return msg->TxId;
}

TRowVersion AcquireSnapshot(TTestActorRuntime& runtime, const TActorId& sender, const TString& databaseName) {
    ui32 nodeId = sender.NodeId();
    ui32 nodeIndex = nodeId - runtime.GetNodeId(0);
    auto* req = new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(databaseName);
    runtime.Send(new IEventHandle(NLongTxService::MakeLongTxServiceID(nodeId), sender, req), nodeIndex, true);
    auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvAcquireReadSnapshotResult>(sender);
    auto* msg = ev->Get();
    UNIT_ASSERT_VALUES_EQUAL(msg->Status, Ydb::StatusIds::SUCCESS);
    return msg->Snapshot;
}

NLongTxService::TLockHandle MakeLockHandle(TTestActorRuntime& runtime, ui64 lockId, ui32 nodeIndex) {
    return NLongTxService::TLockHandle(lockId, runtime.GetActorSystem(nodeIndex));
}

void TWriteOperation::ApplyTo(const TTableId& tableId, NEvents::TDataEvents::TEvWrite* req) {
    TVector<TCell> cells;
    cells.reserve(Rows.size() * 2);
    for (const auto& row : Rows) {
        cells.push_back(TCell::Make(row.Key));
        cells.push_back(TCell::Make(row.Value));
    }

    TSerializedCellMatrix matrix(cells, Rows.size(), 2);
    TString blobData = matrix.ReleaseBuffer();

    ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*req).AddDataToPayload(std::move(blobData));
    req->AddOperation(Type, tableId, { 1, 2 }, payloadIndex, NKikimrDataEvents::FORMAT_CELLVEC);
}

TTransactionState::TTransactionState(TTestActorRuntime& runtime, NKikimrDataEvents::ELockMode lockMode)
    : Runtime(runtime)
    , LockMode(lockMode)
{
    Sender = Runtime.AllocateEdgeActor();
    LockTxId = AllocateTxId(Runtime, Sender);
    LockNodeId = Sender.NodeId();
    LockHandle = MakeLockHandle(Runtime, LockTxId, LockNodeId - Runtime.GetNodeId(0));
}

void TTransactionState::TReadPromise::SendAck(ui64 maxRows, ui64 maxBytes) {
    auto* msg = new TEvDataShard::TEvReadAck();
    msg->Record.SetReadId(0);
    msg->Record.SetSeqNo(LastSeqNo);
    msg->Record.SetMaxRows(maxRows);
    msg->Record.SetMaxBytes(maxBytes);
    ui32 nodeIndex = Sender.NodeId() - State.Runtime.GetNodeId(0);
    State.Runtime.Send(new IEventHandle(LastSender, Sender, msg), nodeIndex, true);
}

std::unique_ptr<TEvDataShard::TEvReadResult> TTransactionState::TReadPromise::NextResult(TDuration simTimeout) {
    auto ev = State.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(Sender, simTimeout);
    if (!ev) {
        return nullptr;
    }
    LastSender = ev->Sender;
    std::unique_ptr<TEvDataShard::TEvReadResult> msg(ev->Release().Release());
    LastSeqNo = msg->Record.GetSeqNo();
    for (const auto& lock : msg->Record.GetTxLocks()) {
        State.Locks.push_back(lock);
    }
    for (const auto& lock : msg->Record.GetBrokenTxLocks()) {
        State.Locks.push_back(lock);
    }
    return msg;
}

TString TTransactionState::TReadPromise::NextString(TDuration simTimeout) {
    auto msg = NextResult(simTimeout);
    if (!msg) {
        return "<timeout>";
    }
    auto status = msg->Record.GetStatus().GetCode();
    if (status != Ydb::StatusIds::SUCCESS) {
        return TStringBuilder() << "ERROR: " << status;
    }
    TString res = FormatIntReadResult(msg.get());
    if (msg->Record.GetFinished()) {
        res += "<end>";
    }
    return res;
}

TString TTransactionState::TReadPromise::AllString() {
    TString res;
    for (;;) {
        auto msg = NextResult();
        auto status = msg->Record.GetStatus().GetCode();
        if (status != Ydb::StatusIds::SUCCESS) {
            res += TStringBuilder() << "ERROR: " << status;
            break;
        }
        res += FormatIntReadResult(msg.get());
        if (msg->Record.GetFinished()) {
            break;
        }
        SendAck();
    }
    return res;
}

TTransactionState::TReadPromise TTransactionState::SendReadKey(const TTableId& tableId, ui64 shardId, i32 key) {
    if (!Snapshot) {
        Snapshot = AcquireSnapshot(Runtime, Sender);
    }

    TActorId sender = Runtime.AllocateEdgeActor();
    ui32 nodeIndex = sender.NodeId() - Runtime.GetNodeId(0);

    auto* req = new TEvDataShard::TEvRead();
    req->Record.SetReadId(0);
    req->Record.MutableTableId()->SetOwnerId(tableId.PathId.OwnerId);
    req->Record.MutableTableId()->SetTableId(tableId.PathId.LocalPathId);
    req->Record.MutableTableId()->SetSchemaVersion(tableId.SchemaVersion);
    req->Record.MutableSnapshot()->SetStep(Snapshot->Step);
    req->Record.MutableSnapshot()->SetTxId(Snapshot->TxId);
    req->Record.SetLockTxId(LockTxId);
    req->Record.SetLockNodeId(LockNodeId);
    req->Record.SetLockMode(LockMode);
    req->Record.AddColumns(1);
    req->Record.AddColumns(2);
    req->Record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);

    TVector<TCell> cells;
    cells.emplace_back(TCell::Make(key));
    req->Keys.emplace_back(cells);

    Runtime.SendToPipe(shardId, sender, req, nodeIndex);
    return { *this, sender };
}

TTransactionState::TReadPromise TTransactionState::SendReadRange(const TTableId& tableId, ui64 shardId, i32 minKey, i32 maxKey, ui64 maxRowsQuota) {
    if (!Snapshot) {
        Snapshot = AcquireSnapshot(Runtime, Sender);
    }

    TActorId sender = Runtime.AllocateEdgeActor();
    ui32 nodeIndex = sender.NodeId() - Runtime.GetNodeId(0);

    auto* req = new TEvDataShard::TEvRead();
    req->Record.SetReadId(0);
    req->Record.MutableTableId()->SetOwnerId(tableId.PathId.OwnerId);
    req->Record.MutableTableId()->SetTableId(tableId.PathId.LocalPathId);
    req->Record.MutableTableId()->SetSchemaVersion(tableId.SchemaVersion);
    req->Record.MutableSnapshot()->SetStep(Snapshot->Step);
    req->Record.MutableSnapshot()->SetTxId(Snapshot->TxId);
    req->Record.SetLockTxId(LockTxId);
    req->Record.SetLockNodeId(LockNodeId);
    req->Record.SetLockMode(LockMode);
    req->Record.AddColumns(1);
    req->Record.AddColumns(2);
    req->Record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
    req->Record.SetMaxRowsInResult(1);
    req->Record.SetMaxRows(maxRowsQuota);

    TVector<TCell> minCells, maxCells;
    minCells.emplace_back(TCell::Make(minKey));
    maxCells.emplace_back(TCell::Make(maxKey));
    req->Ranges.emplace_back(minCells, true, maxCells, true);

    Runtime.SendToPipe(shardId, sender, req, nodeIndex);
    return { *this, sender };
}

bool TTransactionState::HasLockConflicts(ui64 shardId) const {
    const NKikimrDataEvents::TLock* prev = nullptr;
    for (auto it = Locks.begin(); it != Locks.end(); ++it) {
        if (it->GetDataShard() == shardId) {
            if (prev) {
                if (prev->GetGeneration() != it->GetGeneration()) {
                    return true;
                }
                if (prev->GetCounter() != it->GetCounter()) {
                    return true;
                }
            }
            prev = &*it;
        }
    }
    return false;
}

const NKikimrDataEvents::TLock* TTransactionState::FindLastLock(ui64 shardId) const {
    for (auto it = Locks.rbegin(); it != Locks.rend(); ++it) {
        if (it->GetDataShard() == shardId) {
            return &*it;
        }
    }
    return nullptr;
}

std::unique_ptr<NEvents::TDataEvents::TEvLockRowsResult>
TTransactionState::TLockRowsPromise::NextResult(TDuration simTimeout) {
    auto ev = State.Runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(
        Sender, simTimeout);
    if (!ev) {
        return nullptr;
    }
    std::unique_ptr<NEvents::TDataEvents::TEvLockRowsResult> msg(ev->Release().Release());
    for (const auto& lock : msg->Record.GetLocks()) {
        State.Locks.push_back(lock);
    }
    return msg;
}

TString TTransactionState::TLockRowsPromise::NextString(TDuration simTimeout) {
    auto msg = NextResult(simTimeout);
    if (!msg) {
        return "<timeout>";
    }
    auto status = msg->Record.GetStatus();
    if (status != NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS) {
        return TStringBuilder() << "ERROR: " << status;
    }
    return "OK";
}

TTransactionState::TLockRowsPromise TTransactionState::SendLockRows(
    const TTableId& tableId, ui64 shardId, const TVector<i32>& keys,
    NKikimrDataEvents::ELockMode lockMode) {
        auto sender = Runtime.AllocateEdgeActor();
        ui32 nodeIdx = sender.NodeId() - Runtime.GetNodeId(0);

        ui64 requestId = 0;
        auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(requestId);
        req->Record.SetLockId(LockTxId);
        req->Record.SetLockNodeId(LockNodeId);
        req->Record.SetLockMode(lockMode);
        req->SetTableId(tableId);
        req->Record.AddColumnIds(1);

        req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        TVector<TCell> cells;
        cells.reserve(keys.size());
        for (const auto& key : keys) {
            cells.push_back(TCell::Make(key));
        }
        TSerializedCellMatrix matrix(cells, keys.size(), 1);
        req->SetCellMatrix(matrix.ReleaseBuffer());

        Runtime.SendToPipe(shardId, sender, req.release(), nodeIdx);
        return { *this, sender };
}


std::unique_ptr<NEvents::TDataEvents::TEvWriteResult> TTransactionState::TWritePromise::NextResult(TDuration simTimeout) {
    auto ev = State.Runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(Sender, simTimeout);
    if (!ev) {
        return nullptr;
    }
    std::unique_ptr<NEvents::TDataEvents::TEvWriteResult> msg(ev->Release().Release());
    for (const auto& lock : msg->Record.GetTxLocks()) {
        State.Locks.push_back(lock);
    }
    return msg;
}

TString TTransactionState::TWritePromise::NextString(TDuration simTimeout) {
    auto msg = NextResult(simTimeout);
    if (!msg) {
        return "<timeout>";
    }
    auto status = msg->Record.GetStatus();
    if (status != NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED) {
        return TStringBuilder() << "ERROR: " << status;
    }
    return "OK";
}

void TTransactionState::InitCommit(std::vector<ui64> participants) {
    CommitTxId = AllocateTxId(Runtime, Sender);
    Participants = std::move(participants);
}

void TTransactionState::SendPlan() {
    UNIT_ASSERT(Coordinator != 0);
    UNIT_ASSERT(MinStep <= MaxStep);
    UNIT_ASSERT(!Participants.empty());
    SendProposeToCoordinator(
        Runtime, Sender, Participants, {
            .TxId = CommitTxId,
            .Coordinator = Coordinator,
            .MinStep = MinStep,
            .MaxStep = MaxStep,
            .Volatile = true,
        });
}

}
