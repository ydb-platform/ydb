#pragma once
#include "types.h"

#include <ydb/core/base/events.h>
#include <ydb/core/base/row_version.h>
#include <ydb/core/protos/long_tx_service.pb.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_handle.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <yql/essentials/public/issue/yql_issue_message.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <library/cpp/lwtrace/shuttle.h>

namespace NKikimr {
namespace NLongTxService {

    inline TActorId MakeLongTxServiceID(ui32 nodeId) {
        return TActorId(nodeId, TStringBuf("long_tx_svc"));
    }

    struct TEvLongTxService {
        enum EEv {
            EvBeginTx = EventSpaceBegin(TKikimrEvents::ES_LONG_TX_SERVICE),
            EvBeginTxResult,
            EvCommitTx,
            EvCommitTxResult,
            EvRollbackTx,
            EvRollbackTxResult,
            EvAttachColumnShardWrites,
            EvAttachColumnShardWritesResult,
            EvAcquireReadSnapshot,
            EvAcquireReadSnapshotResult,
            EvRegisterLock,
            EvUnregisterLock,
            EvSubscribeLock,
            EvLockStatus,
            EvUnsubscribeLock,
            EvWaitingLockAdd,
            EvWaitingLockRemove,
            EvWaitingLockDeadlock,
            EvCollectSnapshots,
            EvCollectSnapshotsResult,
            EvPropagateSnapshots,
            EvPropagateSnapshotsResult,
            EvUpdateLockWaitEdges,
            EvGetLockWaitGraph,
            EvGetLockWaitGraphResult,
            EvEnd,
        };

        static_assert(TKikimrEvents::ES_LONG_TX_SERVICE == 4207,
                      "expect TKikimrEvents::ES_LONG_TX_SERVICE == 4207");

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_LONG_TX_SERVICE),
                      "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_LONG_TX_SERVICE)");

        struct TEvBeginTx
            : TEventPB<TEvBeginTx, NKikimrLongTxService::TEvBeginTx, EvBeginTx>
        {
            TEvBeginTx() = default;

            explicit TEvBeginTx(const TString& databaseName, NKikimrLongTxService::TEvBeginTx::EMode mode) {
                Record.SetDatabaseName(databaseName);
                Record.SetMode(mode);
            }
        };

        struct TEvBeginTxResult
            : TEventPB<TEvBeginTxResult, NKikimrLongTxService::TEvBeginTxResult, EvBeginTxResult>
        {
            TEvBeginTxResult() = default;

            // Success
            explicit TEvBeginTxResult(const TLongTxId& txId) {
                Record.SetStatus(Ydb::StatusIds::SUCCESS);
                txId.ToProto(Record.MutableLongTxId());
            }

            // Failure
            explicit TEvBeginTxResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues = {}) {
                Record.SetStatus(status);
                if (issues) {
                    IssuesToMessage(issues, Record.MutableIssues());
                }
            }

            TLongTxId GetLongTxId() const {
                return TLongTxId::FromProto(Record.GetLongTxId());
            }
        };

        struct TEvCommitTx
            : TEventPB<TEvCommitTx, NKikimrLongTxService::TEvCommitTx, EvCommitTx>
        {
            TEvCommitTx() = default;

            explicit TEvCommitTx(const TLongTxId& txId) {
                txId.ToProto(Record.MutableLongTxId());
            }

            TLongTxId GetLongTxId() const {
                return TLongTxId::FromProto(Record.GetLongTxId());
            }
        };

        struct TEvCommitTxResult
            : TEventPB<TEvCommitTxResult, NKikimrLongTxService::TEvCommitTxResult, EvCommitTxResult>
        {
            TEvCommitTxResult() = default;

            explicit TEvCommitTxResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues = {}) {
                Record.SetStatus(status);
                if (issues) {
                    IssuesToMessage(issues, Record.MutableIssues());
                }
            }
        };

        struct TEvRollbackTx
            : TEventPB<TEvRollbackTx, NKikimrLongTxService::TEvRollbackTx, EvRollbackTx>
        {
            TEvRollbackTx() = default;

            explicit TEvRollbackTx(const TLongTxId& txId) {
                txId.ToProto(Record.MutableLongTxId());
            }

            TLongTxId GetLongTxId() const {
                return TLongTxId::FromProto(Record.GetLongTxId());
            }
        };

        struct TEvRollbackTxResult
            : TEventPB<TEvRollbackTxResult, NKikimrLongTxService::TEvRollbackTxResult, EvRollbackTxResult>
        {
            TEvRollbackTxResult() = default;

            explicit TEvRollbackTxResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues = {}) {
                Record.SetStatus(status);
                if (issues) {
                    IssuesToMessage(issues, Record.MutableIssues());
                }
            }
        };

        struct TEvAttachColumnShardWrites
            : TEventPB<TEvAttachColumnShardWrites, NKikimrLongTxService::TEvAttachColumnShardWrites, EvAttachColumnShardWrites>
        {
            TEvAttachColumnShardWrites() = default;

            explicit TEvAttachColumnShardWrites(const TLongTxId& txId) {
                txId.ToProto(Record.MutableLongTxId());
            }

            void AddWrite(ui64 columnShard, ui64 writeId) {
                auto* write = Record.AddWrites();
                write->SetColumnShard(columnShard);
                write->SetWriteId(writeId);
            }

            TLongTxId GetLongTxId() const {
                return TLongTxId::FromProto(Record.GetLongTxId());
            }
        };

        struct TEvAttachColumnShardWritesResult
            : TEventPB<TEvAttachColumnShardWritesResult, NKikimrLongTxService::TEvAttachColumnShardWritesResult, EvAttachColumnShardWritesResult>
        {
            TEvAttachColumnShardWritesResult() = default;

            explicit TEvAttachColumnShardWritesResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues = {}) {
                Record.SetStatus(status);
                if (issues) {
                    IssuesToMessage(issues, Record.MutableIssues());
                }
            }
        };

        struct TEvAcquireReadSnapshot
            : TEventLocal<TEvAcquireReadSnapshot, EvAcquireReadSnapshot>
        {
            TEvAcquireReadSnapshot() = default;

            template<class... TArgs>
            explicit TEvAcquireReadSnapshot(const TString& databaseName, TVector<NKikimr::TTableId> tableIds = {}, TArgs&&... args)
                : DatabaseName(databaseName)
                , TableIds(std::move(tableIds))
            {
                (SetOptionalArg(std::forward<TArgs>(args)), ...);
            }

            void SetOptionalArg(NLWTrace::TOrbit&& orbit) {
                Orbit = std::move(orbit);
            }

            TString DatabaseName;
            TVector<NKikimr::TTableId> TableIds;
            NLWTrace::TOrbit Orbit;
        };

        struct TEvAcquireReadSnapshotResult
            : TEventLocal<TEvAcquireReadSnapshotResult, EvAcquireReadSnapshotResult>
        {
            TEvAcquireReadSnapshotResult() = default;

            // Success
            explicit TEvAcquireReadSnapshotResult(const TString& databaseName, const TRowVersion& snapshot, NKqp::TSnapshotHandle&& snapshotHandle, NLWTrace::TOrbit&& orbit)
                : Status(Ydb::StatusIds::SUCCESS)
                , DatabaseName(databaseName)
                , Snapshot(snapshot)
                , SnapshotHandle(std::move(snapshotHandle))
                , Orbit(std::move(orbit))
            {
            }

            // Failure
            explicit TEvAcquireReadSnapshotResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues, NLWTrace::TOrbit&& orbit)
                : Status(status)
                , Issues(issues)
                , Orbit(std::move(orbit))
            {
            }

            Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
            TString DatabaseName;
            TRowVersion Snapshot;
            NKqp::TSnapshotHandle SnapshotHandle;
            NLWTrace::TOrbit Orbit;
        };

        struct TEvRegisterLock
            : TEventLocal<TEvRegisterLock, EvRegisterLock>
        {
            const ui64 LockId;
            const TInstant LockTimestamp;

            explicit TEvRegisterLock(ui64 lockId, TInstant lockTimestamp)
                : LockId(lockId)
                , LockTimestamp(lockTimestamp)
            { }
        };

        struct TEvUnregisterLock
            : TEventLocal<TEvUnregisterLock, EvUnregisterLock>
        {
            const ui64 LockId;

            explicit TEvUnregisterLock(ui64 lockId)
                : LockId(lockId)
            { }
        };

        struct TEvSubscribeLock
            : TEventPB<TEvSubscribeLock, NKikimrLongTxService::TEvSubscribeLock, EvSubscribeLock>
        {
            TEvSubscribeLock() = default;

            TEvSubscribeLock(ui64 lockId, ui32 lockNode) {
                Record.SetLockId(lockId);
                Record.SetLockNode(lockNode);
            }

            void AddLocalWaitEdge(const TWaitEdgeId& id, const TLockInfo& blocker) {
                auto* edge = Record.AddLocalWaitEdges();
                ActorIdToProto(id.OwnerId, edge->MutableId()->MutableOwner());
                edge->MutableId()->SetRequestId(id.RequestId);

                edge->SetBlockerLockId(blocker.LockId);
                edge->SetBlockerLockNode(blocker.LockNodeId);
            }
        };

        struct TEvLockStatus
            : TEventPB<TEvLockStatus, NKikimrLongTxService::TEvLockStatus, EvLockStatus>
        {
            using EStatus = NKikimrLongTxService::TEvLockStatus::EStatus;

            TEvLockStatus() = default;

            TEvLockStatus(ui64 lockId, ui32 lockNode, EStatus status, TInstant lockTimestamp = TInstant::Zero()) {
                Record.SetLockId(lockId);
                Record.SetLockNode(lockNode);
                Record.SetStatus(status);
                if (lockTimestamp) {
                    Record.SetLockTimestampUs(lockTimestamp.MicroSeconds());
                }
            }

            void AddWaitEdge(const TWaitEdgeId& id, const TLockInfo& blocker) {
                auto* edge = Record.AddWaitEdges();
                ActorIdToProto(id.OwnerId, edge->MutableId()->MutableOwner());
                edge->MutableId()->SetRequestId(id.RequestId);

                edge->SetBlockerLockId(blocker.LockId);
                edge->SetBlockerLockNode(blocker.LockNodeId);
            }

            TInstant GetLockTimestamp() const {
                return TInstant::MicroSeconds(Record.GetLockTimestampUs());
            }
        };

        struct TEvUnsubscribeLock
            : TEventPB<TEvUnsubscribeLock, NKikimrLongTxService::TEvUnsubscribeLock, EvUnsubscribeLock>
        {
            TEvUnsubscribeLock() = default;

            TEvUnsubscribeLock(ui64 lockId, ui32 lockNode) {
                Record.SetLockId(lockId);
                Record.SetLockNode(lockNode);
            }
        };

        struct TEvCollectSnapshots
            : TEventPB<TEvCollectSnapshots, NKikimrLongTxService::TEvCollectSnapshots, EvCollectSnapshots>
        {
            TEvCollectSnapshots() = default;
        };

        struct TEvCollectSnapshotsResult
            : TEventPB<TEvCollectSnapshotsResult, NKikimrLongTxService::TEvCollectSnapshotsResult, EvCollectSnapshotsResult>
        {
            TEvCollectSnapshotsResult() = default;
        };

        struct TEvPropagateSnapshots
            : TEventPB<TEvPropagateSnapshots, NKikimrLongTxService::TEvPropagateSnapshots, EvPropagateSnapshots>
        {
            TEvPropagateSnapshots() = default;
        };

        struct TEvPropagateSnapshotsResult
            : TEventPB<TEvPropagateSnapshotsResult, NKikimrLongTxService::TEvPropagateSnapshotsResult, EvPropagateSnapshotsResult>
        {
            TEvPropagateSnapshotsResult() = default;
        };

        struct TEvWaitingLockAdd
            : TEventLocal<TEvWaitingLockAdd, EvWaitingLockAdd>
        {
            TEvWaitingLockAdd(ui64 requestId, TLockInfo lock, TLockInfo otherLock)
                : RequestId(requestId)
                , Lock(lock)
                , OtherLock(otherLock)
            {}

            ui64 RequestId;
            TLockInfo Lock;
            TLockInfo OtherLock;
        };

        struct TEvWaitingLockRemove
            : TEventLocal<TEvWaitingLockRemove, EvWaitingLockRemove>
        {
            TEvWaitingLockRemove(ui64 requestId)
                : RequestId(requestId)
            {}

            ui64 RequestId;
        };

        struct TEvWaitingLockDeadlock
            : TEventLocal<TEvWaitingLockDeadlock, EvWaitingLockDeadlock>
        {
            TEvWaitingLockDeadlock(ui64 requestId)
                : RequestId(requestId)
            {}

            ui64 RequestId;
        };

        struct TEvUpdateLockWaitEdges
            : TEventPB<TEvUpdateLockWaitEdges,
                NKikimrLongTxService::TEvUpdateLockWaitEdges, EvUpdateLockWaitEdges>
        {
            TEvUpdateLockWaitEdges() = default;

            TEvUpdateLockWaitEdges(ui64 lockId, ui32 lockNodeId) {
                Record.SetLockId(lockId);
                Record.SetLockNode(lockNodeId);
            }

            TEvUpdateLockWaitEdges(const TLockInfo& lockInfo)
                : TEvUpdateLockWaitEdges(lockInfo.LockId, lockInfo.LockNodeId)
            {}

            void AddAddedEdge(const TWaitEdgeId& id, const TLockInfo& blocker) {
                auto* edge = Record.AddAdded();
                ActorIdToProto(id.OwnerId, edge->MutableId()->MutableOwner());
                edge->MutableId()->SetRequestId(id.RequestId);

                edge->SetBlockerLockId(blocker.LockId);
                edge->SetBlockerLockNode(blocker.LockNodeId);
            }

            void AddRemovedEdge(const TWaitEdgeId& id) {
                auto* edgeId = Record.AddRemoved();
                ActorIdToProto(id.OwnerId, edgeId->MutableOwner());
                edgeId->SetRequestId(id.RequestId);
            }

            bool Empty() const {
                return Record.GetAdded().empty() && Record.GetRemoved().empty();
            }
        };

        struct TEvGetLockWaitGraph : TEventLocal<TEvGetLockWaitGraph, EvGetLockWaitGraph> {};

        struct TEvGetLockWaitGraphResult
            : TEventLocal<TEvGetLockWaitGraphResult, EvGetLockWaitGraphResult> {
            struct TWaitEdge {
                TWaitEdgeId Id;
                TLockInfo Awaiter;
                TLockInfo Blocker;
            };

            TVector<TWaitEdge> WaitEdges;
        };
    };

} // namespace NLongTxService
} // namespace NKikimr
