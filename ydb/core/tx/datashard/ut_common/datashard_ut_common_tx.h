#pragma once

#include "datashard_ut_common.h"

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>

namespace NKikimr::NDataShard::NTxHelpers {

ui64 AllocateTxId(TTestActorRuntime& runtime, const TActorId& sender);

TRowVersion AcquireSnapshot(TTestActorRuntime& runtime, const TActorId& sender, const TString& databaseName = "/Root");

NLongTxService::TLockHandle MakeLockHandle(TTestActorRuntime& runtime, ui64 lockId, ui32 nodeIndex = 0);

struct TWriteOperation {
    struct TKeyValue {
        i32 Key;
        i32 Value;
    };

    NKikimrDataEvents::TEvWrite::TOperation::EOperationType Type;
    TVector<TKeyValue> Rows;

    void ApplyTo(const TTableId& tableId, NEvents::TDataEvents::TEvWrite* req);

    static TWriteOperation Upsert(i32 key, i32 value) {
        return Upsert({ { key, value } });
    }

    static TWriteOperation Upsert(TVector<TKeyValue> rows) {
        return TWriteOperation{
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            std::move(rows),
        };
    }

    static TWriteOperation Insert(i32 key, i32 value) {
        return Insert({ { key, value } } );
    }

    static TWriteOperation Insert(TVector<TKeyValue> rows) {
        return TWriteOperation{
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT,
            std::move(rows),
        };
    }
};

class TTransactionState {
public:
    TTransactionState(TTestActorRuntime&, NKikimrDataEvents::ELockMode);

    struct TReadPromise {
        TTransactionState& State;
        TActorId Sender;
        TActorId LastSender = TActorId();
        ui64 LastSeqNo = 0;

        void SendAck(ui64 maxRows = Max<ui64>(), ui64 maxBytes = Max<ui64>());
        std::unique_ptr<TEvDataShard::TEvReadResult> NextResult(TDuration simTimeout = TDuration::Max());
        TString NextString(TDuration simTimeout = TDuration::Max());
        TString AllString();
    };

    TReadPromise SendReadKey(const TTableId& tableId, ui64 shardId, i32 key);

    TString ReadKey(const TTableId& tableId, ui64 shardId, i32 key) {
        auto promise = SendReadKey(tableId, shardId, key);
        return promise.AllString();
    }

    TReadPromise SendReadRange(const TTableId& tableId, ui64 shardId, i32 minKey, i32 maxKey, ui64 maxRowsQuota = Max<ui64>());

    TString ReadRange(const TTableId& tableId, ui64 shardId, i32 minKey, i32 maxKey) {
        auto promise = SendReadRange(tableId, shardId, minKey, maxKey);
        return promise.AllString();
    }

    void ResetSnapshot() {
        Snapshot = {};
    }

    bool HasLockConflicts(ui64 shardId) const;
    const NKikimrDataEvents::TLock* FindLastLock(ui64 shardId) const;

    struct TLockRowsPromise {
        TTransactionState& State;
        TActorId Sender;

        std::unique_ptr<NEvents::TDataEvents::TEvLockRowsResult> NextResult(
            TDuration simTimeout = TDuration::Max());
        TString NextString(TDuration simTimeout = TDuration::Max());
    };

    TLockRowsPromise SendLockRows(
        const TTableId& tableId, ui64 shardId, const TVector<i32>& keys,
        NKikimrDataEvents::ELockMode lockMode = NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);

    TString LockRows(
            const TTableId& tableId, ui64 shardId, const TVector<i32>& keys,
            NKikimrDataEvents::ELockMode lockMode = NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE) {
        auto promise = SendLockRows(tableId, shardId, keys, lockMode);
        return promise.NextString();
    }

    struct TWritePromise {
        TTransactionState& State;
        TActorId Sender;

        std::unique_ptr<NEvents::TDataEvents::TEvWriteResult> NextResult(TDuration simTimeout = TDuration::Max());
        TString NextString(TDuration simTimeout = TDuration::Max());
    };

    template<class... TOps>
    TWritePromise SendWrite(const TTableId& tableId, ui64 shardId, TOps&&... ops) {
        auto sender = Runtime.AllocateEdgeActor();
        ui32 nodeIndex = sender.NodeId() - Runtime.GetNodeId(0);

        auto* req = new NEvents::TDataEvents::TEvWrite(0, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        req->Record.SetLockTxId(LockTxId);
        req->Record.SetLockNodeId(LockNodeId);
        req->Record.SetLockMode(LockMode);
        if (Snapshot) {
            req->Record.MutableMvccSnapshot()->SetStep(Snapshot->Step);
            req->Record.MutableMvccSnapshot()->SetTxId(Snapshot->TxId);
        }

        (..., ops.ApplyTo(tableId, req));

        Runtime.SendToPipe(shardId, sender, req, nodeIndex);
        return { *this, sender };
    }

    template<class... TOps>
    TString Write(const TTableId& tableId, ui64 shardId, TOps&&... ops) {
        auto promise = SendWrite(tableId, shardId, std::forward<TOps>(ops)...);
        return promise.NextString();
    }

    template<class... TOps>
    TWritePromise SendWriteCommit(const TTableId& tableId, ui64 shardId, TOps&&... ops) {
        auto sender = Runtime.AllocateEdgeActor();
        ui32 nodeIndex = sender.NodeId() - Runtime.GetNodeId(0);

        auto* req = new NEvents::TDataEvents::TEvWrite(0, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        req->Record.SetLockMode(LockMode);
        if (Snapshot) {
            req->Record.MutableMvccSnapshot()->SetStep(Snapshot->Step);
            req->Record.MutableMvccSnapshot()->SetTxId(Snapshot->TxId);
        }

        // Try to find the last known lock state
        if (const auto* pLock = FindLastLock(shardId)) {
            req->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            req->Record.MutableLocks()->AddSendingShards(shardId);
            req->Record.MutableLocks()->AddReceivingShards(shardId);
            *req->Record.MutableLocks()->AddLocks() = *pLock;
        }

        (..., ops.ApplyTo(tableId, req));

        Runtime.SendToPipe(shardId, sender, req, nodeIndex);
        return { *this, sender };
    }

    template<class... TOps>
    TString WriteCommit(const TTableId& tableId, ui64 shardId, TOps&&... ops) {
        auto promise = SendWriteCommit(tableId, shardId, std::forward<TOps>(ops)...);
        return promise.NextString();
    }

    void InitCommit(std::vector<ui64> participants);

    template<class... TOps>
    TWritePromise SendPrepareCommit(const TTableId& tableId, ui64 shardId, TOps&&... ops) {
        auto sender = Runtime.AllocateEdgeActor();
        ui32 nodeIndex = sender.NodeId() - Runtime.GetNodeId(0);

        auto* req = new NEvents::TDataEvents::TEvWrite(CommitTxId, NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE);
        req->Record.SetLockMode(LockMode);
        if (Snapshot) {
            req->Record.MutableMvccSnapshot()->SetStep(Snapshot->Step);
            req->Record.MutableMvccSnapshot()->SetTxId(Snapshot->TxId);
        }

        req->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
        for (ui64 participant : Participants) {
            req->Record.MutableLocks()->AddSendingShards(participant);
            req->Record.MutableLocks()->AddReceivingShards(participant);
        }
        if (const auto* pLock = FindLastLock(shardId)) {
            *req->Record.MutableLocks()->AddLocks() = *pLock;
        }

        (..., ops.ApplyTo(tableId, req));

        Runtime.SendToPipe(shardId, sender, req, nodeIndex);
        return { *this, sender };
    }

    template<class... TOps>
    TWritePromise PrepareCommit(const TTableId& tableId, ui64 shardId, TOps&&... ops) {
        auto promise = SendPrepareCommit(tableId, shardId, std::forward<TOps>(ops)...);
        auto msg = promise.NextResult();
        UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
        MinStep = Max(MinStep, msg->Record.GetMinStep());
        MaxStep = Min(MaxStep, msg->Record.GetMaxStep());
        for (ui64 coordinator : msg->Record.GetDomainCoordinators()) {
            Coordinator = coordinator;
        }
        return promise;
    }

    void SendPlan();

public:
    TTestActorRuntime& Runtime;
    NKikimrDataEvents::ELockMode LockMode;
    TActorId Sender;
    ui64 LockTxId = 0;
    ui32 LockNodeId = 0;
    NLongTxService::TLockHandle LockHandle;
    std::optional<TRowVersion> Snapshot;
    std::vector<NKikimrDataEvents::TLock> Locks;
    std::vector<ui64> Participants;
    ui64 CommitTxId = 0;
    ui64 MinStep = 0;
    ui64 MaxStep = Max<ui64>();
    ui64 Coordinator = 0;
};

}
