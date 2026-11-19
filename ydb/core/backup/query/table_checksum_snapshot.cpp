#include "table_checksum_snapshot.h"
#include "table_checksum.h"

#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <util/generic/hash_set.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NBackup {
namespace {

    class TTableChecksumSnapshotActor : public TActorBootstrapped<TTableChecksumSnapshotActor> {
    public:
        enum class EAction {
            Confirm,
            CheckCopyCapture,
            Release,
        };

        TTableChecksumSnapshotActor(
            TActorId replyTo, const TTableId& tableId, ui64 schemeShardId, TRowVersion snapshot, EAction action)
            : ReplyTo(replyTo)
            , TableId(tableId)
            , SchemeShardId(schemeShardId)
            , Snapshot(snapshot)
            , Action(action)
        {}

        void Bootstrap() {
            auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeScheme>(
                TableId.PathId.OwnerId, TableId.PathId.LocalPathId);
            request->Record.MutableOptions()->SetReturnChildren(false);
            request->Record.MutableOptions()->SetReturnPartitioningInfo(true);
            request->Record.MutableOptions()->SetReturnRangeKey(false);
            request->Record.MutableOptions()->SetReturnPartitionConfig(false);
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), SchemeShardId, true));
            Schedule(TableChecksumSnapshotTimeout, new TEvents::TEvWakeup());
            Become(&TThis::StateWork);
        }

    private:
        void Finish(Ydb::StatusIds::StatusCode status, const TString& error = {}, bool unsupported = false) {
            if (ReplyTo) {
                Send(ReplyTo, new TEvTableChecksum::TEvSnapshotConfirmed(status, Snapshot, error, unsupported));
            }
            PassAway();
        }

        template <typename TRequest>
        void SendShardRequest(ui64 shardId) {
            auto request = MakeHolder<TRequest>();
            request->Record.SetOwnerId(TableId.PathId.OwnerId);
            request->Record.SetPathId(TableId.PathId.LocalPathId);
            request->Record.SetStep(Snapshot.Step);
            request->Record.SetTxId(Snapshot.TxId);
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), shardId, true));
        }

        void AddShard(ui64 shardId) {
            if (!SeenShards.insert(shardId).second) {
                return;
            }
            PendingShards.insert(shardId);
            if (Action != EAction::Release) {
                SendShardRequest<TEvDataShard::TEvRefreshVolatileSnapshotRequest>(shardId);
            } else {
                SendShardRequest<TEvDataShard::TEvDiscardVolatileSnapshotRequest>(shardId);
            }
        }

        void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
            const auto& record = ev->Get()->GetRecord();
            if (record.GetStatus() != NKikimrScheme::StatusSuccess) {
                return Finish(record.GetStatus() == NKikimrScheme::StatusNotAvailable
                                  ? Ydb::StatusIds::UNAVAILABLE
                                  : Ydb::StatusIds::PRECONDITION_FAILED,
                    "Cannot resolve table for checksum snapshot: " + record.GetReason());
            }
            const auto& path = record.GetPathDescription();
            if (path.GetSelf().GetSchemeshardId() != TableId.PathId.OwnerId ||
                path.GetSelf().GetPathId() != TableId.PathId.LocalPathId || path.TablePartitionsSize() == 0) {
                return Finish(
                    Ydb::StatusIds::PRECONDITION_FAILED, "Table checksum snapshot has no matching table shards");
            }
            for (const auto& partition : path.GetTablePartitions()) {
                AddShard(partition.GetDatashardId());
            }
        }

        void Handle(TEvDataShard::TEvRefreshVolatileSnapshotResponse::TPtr& ev) {
            using TResponse = NKikimrTxDataShard::TEvRefreshVolatileSnapshotResponse;
            const auto& record = ev->Get()->Record;
            if (!PendingShards.erase(record.GetOrigin())) {
                return;
            }
            if (Action != EAction::CheckCopyCapture && !record.GetSupportsTableChecksumSnapshots()) {
                return Finish(
                    Ydb::StatusIds::UNSUPPORTED, "A DataShard does not support table checksum snapshots", true);
            }
            if (record.GetStatus() == TResponse::SNAPSHOT_TRANSFERRED) {
                if (record.TransferredToShardsSize() == 0) {
                    return Finish(Ydb::StatusIds::UNAVAILABLE, "Table checksum snapshot is being transferred");
                }
                for (ui64 shard : record.GetTransferredToShards()) {
                    AddShard(shard);
                }
            } else if (Action == EAction::CheckCopyCapture &&
                       (record.GetStatus() == TResponse::REFRESHED ||
                           record.GetStatus() == TResponse::SNAPSHOT_NOT_FOUND)) {
                if (record.HasTableChecksumSnapshot() &&
                    TRowVersion::FromProto(record.GetTableChecksumSnapshot()) != Snapshot) {
                    return Finish(Ydb::StatusIds::PRECONDITION_FAILED,
                        "Backup copy checksum capture does not match the requested snapshot");
                }
                UnsupportedCopy = UnsupportedCopy || !record.GetSupportsTableChecksumSnapshots() ||
                                  !record.HasTableChecksumSnapshot();
            } else if (record.GetStatus() == TResponse::REFRESHED) {
                const TRowVersion version(record.GetStep(), record.GetTxId());
                if (!version.Step || version.TxId != Snapshot.TxId || (Snapshot.Step && version != Snapshot)) {
                    return Finish(
                        Ydb::StatusIds::PRECONDITION_FAILED, "Table checksum snapshot versions differ across shards");
                }
                Snapshot = version;
            } else {
                return Finish(record.GetStatus() == TResponse::SNAPSHOT_NOT_FOUND ? Ydb::StatusIds::PRECONDITION_FAILED
                                                                                  : Ydb::StatusIds::UNAVAILABLE,
                    TStringBuilder() << "Table checksum snapshot is unavailable on DataShard " << record.GetOrigin()
                                     << ": " << TResponse::EStatus_Name(record.GetStatus()));
            }
            if (PendingShards.empty()) {
                if (UnsupportedCopy) {
                    return Finish(Ydb::StatusIds::UNSUPPORTED,
                        "A backup copy was created without table checksum snapshot support", true);
                }
                if (!Snapshot.Step) {
                    return Finish(Ydb::StatusIds::UNAVAILABLE, "Table checksum snapshot is being transferred");
                }
                Finish(Ydb::StatusIds::SUCCESS);
            }
        }

        void Handle(TEvDataShard::TEvDiscardVolatileSnapshotResponse::TPtr& ev) {
            using TResponse = NKikimrTxDataShard::TEvDiscardVolatileSnapshotResponse;
            const auto& record = ev->Get()->Record;
            if (!PendingShards.erase(record.GetOrigin())) {
                return;
            }
            if (record.GetStatus() == TResponse::SNAPSHOT_TRANSFERRED) {
                if (record.TransferredToShardsSize() == 0) {
                    return Unavailable();
                }
                for (ui64 shard : record.GetTransferredToShards()) {
                    AddShard(shard);
                }
            } else if (record.GetStatus() != TResponse::DISCARDED &&
                       record.GetStatus() != TResponse::SNAPSHOT_NOT_FOUND) {
                return Unavailable();
            }
            if (PendingShards.empty()) {
                Finish(Ydb::StatusIds::SUCCESS);
            }
        }

        void Unavailable() {
            Finish(Ydb::StatusIds::UNAVAILABLE, "Cannot confirm table checksum snapshot retention");
        }

        void PassAway() override {
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(SchemeShardId));
            for (ui64 shard : SeenShards) {
                Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(shard));
            }
            TActorBootstrapped::PassAway();
        }

        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
                hFunc(TEvDataShard::TEvRefreshVolatileSnapshotResponse, Handle);
                hFunc(TEvDataShard::TEvDiscardVolatileSnapshotResponse, Handle);
                sFunc(TEvPipeCache::TEvDeliveryProblem, Unavailable);
                sFunc(TEvents::TEvWakeup, Unavailable);
                sFunc(TEvents::TEvPoisonPill, PassAway);
            }
        }

        const TActorId ReplyTo;
        const TTableId TableId;
        const ui64 SchemeShardId;
        TRowVersion Snapshot;
        const EAction Action;
        bool UnsupportedCopy = false;
        THashSet<ui64> PendingShards;
        THashSet<ui64> SeenShards;
    };

} // namespace

IActor* CreateTableChecksumSnapshotReader(
    TActorId replyTo, const TTableId& tableId, ui64 schemeShardId, TRowVersion snapshot, bool checkCopyCapture) {
    return new TTableChecksumSnapshotActor(replyTo, tableId, schemeShardId, snapshot,
        checkCopyCapture ? TTableChecksumSnapshotActor::EAction::CheckCopyCapture
                         : TTableChecksumSnapshotActor::EAction::Confirm);
}

IActor* CreateTableChecksumSnapshotReleaser(const TTableId& tableId, ui64 schemeShardId, TRowVersion snapshot) {
    return new TTableChecksumSnapshotActor(
        {}, tableId, schemeShardId, snapshot, TTableChecksumSnapshotActor::EAction::Release);
}

} // namespace NKikimr::NBackup
