#include "schemeshard_build_index.h"
#include "schemeshard_impl.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"

#include <ydb/core/tx/datashard/range_ops.h>

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/core/ydb_convert/table_description.h>


namespace NKikimr {
namespace NSchemeShard {

THolder<TEvSchemeShard::TEvModifySchemeTransaction> LockPropose(
    TSchemeShard* ss, const TIndexBuildInfo::TPtr buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo->LockTxId), ss->TabletID());
    propose->Record.SetFailOnExist(false);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateLock);
    modifyScheme.SetInternal(true);

    TPath path = TPath::Init(buildInfo->TablePathId, ss);
    modifyScheme.SetWorkingDir(path.Parent().PathString());

    auto& lockConfig = *modifyScheme.MutableLockConfig();
    lockConfig.SetName(path.LeafName());

    if (buildInfo->IsBuildIndex()) {
        buildInfo->SerializeToProto(ss, modifyScheme.MutableInitiateIndexBuild());
    } else if (buildInfo->IsBuildColumn()) {
        buildInfo->SerializeToProto(ss, modifyScheme.MutableInitiateColumnBuild());
    } else if (buildInfo->IsCheckingNotNull()) {
        buildInfo->SerializeToProto(ss, modifyScheme.MutableInitiateCheckingNotNull());
    } else {
        Y_ABORT("Unknown operation kind while building LockPropose");
    }

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> InitiatePropose(
    TSchemeShard* ss, const TIndexBuildInfo::TPtr buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo->InitiateTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    if (buildInfo->IsBuildIndex()) {
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateIndexBuild);
        modifyScheme.SetInternal(true);

        modifyScheme.SetWorkingDir(TPath::Init(buildInfo->DomainPathId, ss).PathString());

        modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo->LockTxId));

        buildInfo->SerializeToProto(ss, modifyScheme.MutableInitiateIndexBuild());
    } else if (buildInfo->IsBuildColumn()) {
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateColumnBuild);
        modifyScheme.SetInternal(true);
        modifyScheme.SetWorkingDir(TPath::Init(buildInfo->DomainPathId, ss).PathString());
        modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo->LockTxId));

        buildInfo->SerializeToProto(ss, modifyScheme.MutableInitiateColumnBuild());
    } else if (buildInfo->IsCheckingNotNull()) {
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCheckingNotNull);
        modifyScheme.SetInternal(true);
        modifyScheme.SetWorkingDir(TPath::Init(buildInfo->DomainPathId, ss).PathString());
        modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo->LockTxId));

        buildInfo->SerializeToProto(ss, modifyScheme.MutableInitiateCheckingNotNull());
    } else {
        Y_ABORT("Unknown operation kind while building InitiatePropose");
    }

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> AlterMainTablePropose(
    TSchemeShard* ss, const TIndexBuildInfo::TPtr buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo->AlterMainTableTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    if (buildInfo->IsBuildColumn()) {
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
        modifyScheme.SetInternal(true);
        modifyScheme.SetWorkingDir(TPath::Init(buildInfo->TablePathId, ss).Parent().PathString());
        modifyScheme.MutableAlterTable()->SetName(TPath::Init(buildInfo->TablePathId, ss).LeafName());
        for (auto& colInfo : buildInfo->BuildColumns) {
            auto col = modifyScheme.MutableAlterTable()->AddColumns();
            NScheme::TTypeInfo typeInfo;
            TString typeMod;
            Ydb::StatusIds::StatusCode status;
            TString error;
            if (!ExtractColumnTypeInfo(typeInfo, typeMod, colInfo.DefaultFromLiteral.type(), status, error)) {
                // todo gvit fix that
                Y_ABORT("failed to extract column type info");
            }

            col->SetType(NScheme::TypeName(typeInfo, typeMod));
            col->SetName(colInfo.ColumnName);
            col->MutableDefaultFromLiteral()->CopyFrom(colInfo.DefaultFromLiteral);
            col->SetIsBuildInProgress(true);

            if (!colInfo.FamilyName.empty()) {
                col->SetFamilyName(colInfo.FamilyName);
            }

            if (colInfo.NotNull) {
                col->SetNotNull(colInfo.NotNull);
            }

        }

    } else if (buildInfo->IsCheckingNotNull()) {
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
        modifyScheme.SetInternal(true);
        modifyScheme.SetWorkingDir(TPath::Init(buildInfo->TablePathId, ss).Parent().PathString());
        modifyScheme.MutableAlterTable()->SetName(TPath::Init(buildInfo->TablePathId, ss).LeafName());

        for (auto& colInfo : buildInfo->CheckingNotNullColumns) {
            auto col = modifyScheme.MutableAlterTable()->AddColumns();
            col->SetName(colInfo.ColumnName);
            col->SetIsCheckingNotNullInProgress(true);
        }
    } else {
        Y_ABORT("Unknown operation kind while building AlterMainTablePropose");
    }

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> ApplyPropose(
    TSchemeShard* ss, const TIndexBuildInfo::TPtr buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo->ApplyTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpApplyIndexBuild);
    modifyScheme.SetInternal(true);

    modifyScheme.SetWorkingDir(TPath::Init(buildInfo->DomainPathId, ss).PathString());

    modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo->LockTxId));

    auto& indexBuild = *modifyScheme.MutableApplyIndexBuild();
    indexBuild.SetTablePath(TPath::Init(buildInfo->TablePathId, ss).PathString());

    if (buildInfo->IsBuildIndex()) {
        indexBuild.SetIndexName(buildInfo->IndexName);
    }

    indexBuild.SetSnaphotTxId(ui64(buildInfo->InitiateTxId));
    indexBuild.SetBuildIndexId(ui64(buildInfo->Id));

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> UnlockPropose(
    TSchemeShard* ss, const TIndexBuildInfo::TPtr buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo->UnlockTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropLock);
    modifyScheme.SetInternal(true);

    modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo->LockTxId));

    TPath path = TPath::Init(buildInfo->TablePathId, ss);
    modifyScheme.SetWorkingDir(path.Parent().PathString());

    auto& lockConfig = *modifyScheme.MutableLockConfig();
    lockConfig.SetName(path.LeafName());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CancelPropose(
    TSchemeShard* ss, const TIndexBuildInfo::TPtr buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo->ApplyTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCancelIndexBuild);
    modifyScheme.SetInternal(true);

    modifyScheme.SetWorkingDir(TPath::Init(buildInfo->DomainPathId, ss).PathString());

    modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo->LockTxId));

    auto& indexBuild = *modifyScheme.MutableCancelIndexBuild();
    indexBuild.SetTablePath(TPath::Init(buildInfo->TablePathId, ss).PathString());
    indexBuild.SetIndexName(buildInfo->IndexName);
    indexBuild.SetSnaphotTxId(ui64(buildInfo->InitiateTxId));
    indexBuild.SetBuildIndexId(ui64(buildInfo->Id));

    return propose;
}

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxProgress: public TSchemeShard::TIndexBuilder::TTxBase  {
private:
    TIndexBuildId BuildId;

    TDeque<std::tuple<TTabletId, ui64, THolder<IEventBase>>> ToTabletSend;

public:
    explicit TTxProgress(TSelf* self, TIndexBuildId id)
        : TTxBase(self, TXTYPE_PROGRESS_INDEX_BUILD)
        , BuildId(id)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_ABORT_UNLESS(Self->IndexBuilds.contains(BuildId));
        TIndexBuildInfo::TPtr buildInfo = Self->IndexBuilds.at(BuildId);

        LOG_I("TTxBuildProgress: Resume"
              << ": id# " << BuildId);
        LOG_D("TTxBuildProgress: Resume"
              << ": " << *buildInfo);

        switch (buildInfo->State) {
        case TIndexBuildInfo::EState::Invalid:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::AlterMainTable:
            if (buildInfo->AlterMainTableTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo->AlterMainTableTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), AlterMainTablePropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo->AlterMainTableTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo->AlterMainTableTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Locking);
                Progress(BuildId);
            }
            break;

        case TIndexBuildInfo::EState::Locking:
            if (buildInfo->LockTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo->LockTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), LockPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo->LockTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo->LockTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Initiating);
                Progress(BuildId);
            }

            break;
        case TIndexBuildInfo::EState::GatheringStatistics:
            ChangeState(BuildId, TIndexBuildInfo::EState::Initiating);
            Progress(BuildId);
            break;
        case TIndexBuildInfo::EState::Initiating:
            if (buildInfo->InitiateTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo->InitiateTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), InitiatePropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo->InitiateTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo->InitiateTxId)));
            } else {
                // TODO add vector index filling
                if (buildInfo->IndexType == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree) {
                    ChangeState(BuildId, TIndexBuildInfo::EState::Applying);
                    Progress(BuildId);
                    break;
                }

                ChangeState(BuildId, TIndexBuildInfo::EState::Filling);
                Progress(BuildId);
            }

            break;
        case TIndexBuildInfo::EState::Filling:
            if (buildInfo->IsCancellationRequested()) {
                buildInfo->DoneShards.clear();
                buildInfo->InProgressShards.clear();

                Self->IndexBuildPipes.CloseAll(BuildId, ctx);

                ChangeState(BuildId, TIndexBuildInfo::EState::Cancellation_Applying);
                Progress(BuildId);

                // make final bill
                Bill(buildInfo);

                break;
            }

            if (buildInfo->Shards.empty()) {
                NIceDb::TNiceDb db(txc.DB);
                InitiateShards(db, buildInfo);
            }

            if (buildInfo->ToUploadShards.empty()
                && buildInfo->DoneShards.empty()
                && buildInfo->InProgressShards.empty())
            {
                for (const auto& item: buildInfo->Shards) {
                    const TIndexBuildInfo::TShardStatus& shardStatus = item.second;
                    switch (shardStatus.Status) {
                    case NKikimrTxDataShard::EBuildIndexStatus::INVALID:
                    case NKikimrTxDataShard::EBuildIndexStatus::ACCEPTED:
                    case NKikimrTxDataShard::EBuildIndexStatus::INPROGRESS:
                    case NKikimrTxDataShard::EBuildIndexStatus::ABORTED:
                        buildInfo->ToUploadShards.push_back(item.first);
                        break;
                    case NKikimrTxDataShard::EBuildIndexStatus::DONE:
                        buildInfo->DoneShards.insert(item.first);
                        break;
                    case NKikimrTxDataShard::EBuildIndexStatus::CHECKING_NOT_NULL_ERROR:
                    case NKikimrTxDataShard::EBuildIndexStatus::BUILD_ERROR:
                    case NKikimrTxDataShard::EBuildIndexStatus::BAD_REQUEST:
                        Y_ABORT("Unreachable");
                        break;
                    }
                }
            }

            if (!buildInfo->SnapshotTxId || !buildInfo->SnapshotStep) {
                Y_ABORT_UNLESS(Self->TablesWithSnapshots.contains(buildInfo->TablePathId));
                Y_ABORT_UNLESS(Self->TablesWithSnapshots.at(buildInfo->TablePathId) == buildInfo->InitiateTxId);

                buildInfo->SnapshotTxId = buildInfo->InitiateTxId;
                Y_ABORT_UNLESS(buildInfo->SnapshotTxId);
                buildInfo->SnapshotStep = Self->SnapshotsStepIds.at(buildInfo->SnapshotTxId);
                Y_ABORT_UNLESS(buildInfo->SnapshotStep);
            }

            if (buildInfo->ImplTablePath.Empty() && buildInfo->IsBuildIndex()) {
                TPath implTable = TPath::Init(buildInfo->TablePathId, Self).Dive(buildInfo->IndexName).Dive(NTableIndex::ImplTable);
                buildInfo->ImplTablePath = implTable.PathString();

                TTableInfo::TPtr implTableInfo = Self->Tables.at(implTable.Base()->PathId);
                buildInfo->ImplTableColumns = NTableIndex::ExtractInfo(implTableInfo);
            }

            while (!buildInfo->ToUploadShards.empty()
                   && buildInfo->InProgressShards.size() < buildInfo->Limits.MaxShards)
            {
                TShardIdx shardIdx = buildInfo->ToUploadShards.front();
                buildInfo->ToUploadShards.pop_front();
                buildInfo->InProgressShards.insert(shardIdx);

                if (buildInfo->IsCheckingNotNull()) {
                    auto ev = MakeHolder<TEvDataShard::TEvCheckConstraintCreateRequest>();
                    ev->Record.SetBuildIndexId(ui64(BuildId));

                    TTabletId shardId = Self->ShardInfos.at(shardIdx).TabletID;
                    ev->Record.SetTabletId(ui64(shardId));

                    ev->Record.SetOwnerId(buildInfo->TablePathId.OwnerId);
                    ev->Record.SetPathId(buildInfo->TablePathId.LocalPathId);

                    ev->Record.SetTargetName(TPath::Init(buildInfo->TablePathId, Self).PathString());

                    buildInfo->SerializeToProto(Self, ev->Record.MutableCheckingNotNullSettings());

                    TIndexBuildInfo::TShardStatus& shardStatus = buildInfo->Shards.at(shardIdx);
                    if (shardStatus.LastKeyAck) {
                        TSerializedTableRange range = TSerializedTableRange(shardStatus.LastKeyAck, "", false, false);
                        range.Serialize(*ev->Record.MutableKeyRange());
                    } else {
                        shardStatus.Range.Serialize(*ev->Record.MutableKeyRange());
                    }

                    ev->Record.SetSnapshotTxId(ui64(buildInfo->SnapshotTxId));
                    ev->Record.SetSnapshotStep(ui64(buildInfo->SnapshotStep));

                    ev->Record.SetSeqNoGeneration(Self->Generation());
                    ev->Record.SetSeqNoRound(++shardStatus.SeqNoRound);

                    LOG_D("TTxBuildProgress: TEvCheckConstraintCreateRequest"
                        << ": " << ev->Record.ShortDebugString());

                    ToTabletSend.emplace_back(shardId, ui64(BuildId), std::move(ev));
                } else {
                    auto ev = MakeHolder<TEvDataShard::TEvBuildIndexCreateRequest>();
                    ev->Record.SetBuildIndexId(ui64(BuildId));

                    TTabletId shardId = Self->ShardInfos.at(shardIdx).TabletID;
                    ev->Record.SetTabletId(ui64(shardId));

                    ev->Record.SetOwnerId(buildInfo->TablePathId.OwnerId);
                    ev->Record.SetPathId(buildInfo->TablePathId.LocalPathId);

                    if (buildInfo->IsBuildColumn()) {
                        ev->Record.SetTargetName(TPath::Init(buildInfo->TablePathId, Self).PathString());
                    } else if (buildInfo->IsBuildIndex()) {
                        ev->Record.SetTargetName(buildInfo->ImplTablePath);
                    }

                    if (buildInfo->IsBuildIndex()) {
                        THashSet<TString> columns = buildInfo->ImplTableColumns.Columns;
                        for (const auto& x: buildInfo->ImplTableColumns.Keys) {
                            *ev->Record.AddIndexColumns() = x;
                            columns.erase(x);
                        }
                        for (const auto& x: columns) {
                            *ev->Record.AddDataColumns() = x;
                        }
                    } else if (buildInfo->IsBuildColumn()) {
                        buildInfo->SerializeToProto(Self, ev->Record.MutableColumnBuildSettings());
                    }

                    TIndexBuildInfo::TShardStatus& shardStatus = buildInfo->Shards.at(shardIdx);
                    if (shardStatus.LastKeyAck) {
                        TSerializedTableRange range = TSerializedTableRange(shardStatus.LastKeyAck, "", false, false);
                        range.Serialize(*ev->Record.MutableKeyRange());
                    } else {
                        shardStatus.Range.Serialize(*ev->Record.MutableKeyRange());
                    }

                    ev->Record.SetMaxBatchRows(buildInfo->Limits.MaxBatchRows);
                    ev->Record.SetMaxBatchBytes(buildInfo->Limits.MaxBatchBytes);
                    ev->Record.SetMaxRetries(buildInfo->Limits.MaxRetries);

                    ev->Record.SetSnapshotTxId(ui64(buildInfo->SnapshotTxId));
                    ev->Record.SetSnapshotStep(ui64(buildInfo->SnapshotStep));

                    ev->Record.SetSeqNoGeneration(Self->Generation());
                    ev->Record.SetSeqNoRound(++shardStatus.SeqNoRound);

                    LOG_D("TTxBuildProgress: TEvBuildIndexCreateRequest"
                        << ": " << ev->Record.ShortDebugString());

                    ToTabletSend.emplace_back(shardId, ui64(BuildId), std::move(ev));
                }
            }

            if (buildInfo->InProgressShards.empty() && buildInfo->ToUploadShards.empty()
                && buildInfo->DoneShards.size() == buildInfo->Shards.size()) {
                // all done
                Y_ABORT_UNLESS(0 == Self->IndexBuildPipes.CloseAll(BuildId, ctx));

                ChangeState(BuildId, TIndexBuildInfo::EState::Applying);
                Progress(BuildId);

                // make final bill
                Bill(buildInfo);
            } else {
                AskToScheduleBilling(buildInfo);
            }

            break;
        case TIndexBuildInfo::EState::Applying:
            if (buildInfo->ApplyTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo->ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), ApplyPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo->ApplyTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo->ApplyTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Unlocking);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Unlocking:
            if (buildInfo->UnlockTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo->UnlockTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), UnlockPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo->UnlockTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo->UnlockTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Done);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Done:
            SendNotificationsIfFinished(buildInfo);
            // stay calm keep status/issues
            break;

        case TIndexBuildInfo::EState::Cancellation_Applying:
            if (buildInfo->ApplyTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo->ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), CancelPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo->ApplyTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo->ApplyTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Cancellation_Unlocking);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
            if (buildInfo->UnlockTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo->UnlockTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), UnlockPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo->UnlockTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo->UnlockTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Cancelled);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Cancelled:
            SendNotificationsIfFinished(buildInfo);
            // stay calm keep status/issues
            break;

        case TIndexBuildInfo::EState::Rejection_Applying:
            if (buildInfo->ApplyTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo->ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), CancelPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo->ApplyTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo->ApplyTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Rejection_Unlocking);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Rejection_Unlocking:
            if (buildInfo->UnlockTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo->UnlockTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), UnlockPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo->UnlockTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo->UnlockTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Rejected);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Rejected:
            SendNotificationsIfFinished(buildInfo);
            // stay calm keep status/issues
            break;
        }

        return true;
    }

    TSerializedTableRange InfiniteRange(ui32 columns) {
        TVector<TCell> vec(columns, TCell());
        TArrayRef<TCell> cells(vec);
        return TSerializedTableRange(TSerializedCellVec::Serialize(cells), "", true, false);
    }

    void InitiateShards(NIceDb::TNiceDb& db, TIndexBuildInfo::TPtr& buildInfo) {
        TTableInfo::TPtr table = Self->Tables.at(buildInfo->TablePathId);

        auto tableColumns = NTableIndex::ExtractInfo(table); // skip dropped columns
        const TSerializedTableRange infiniteRange = InfiniteRange(tableColumns.Keys.size());

        for (const auto& x: table->GetPartitions()) {
            Y_ABORT_UNLESS(Self->ShardInfos.contains(x.ShardIdx));

            buildInfo->Shards.emplace(x.ShardIdx, TIndexBuildInfo::TShardStatus(infiniteRange, ""));
            Self->PersistBuildIndexUploadInitiate(db, buildInfo, x.ShardIdx);
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        for (auto& x: ToTabletSend) {
            Self->IndexBuildPipes.Create(BuildId, std::get<0>(x), std::move(std::get<2>(x)), ctx);
        }
        ToTabletSend.clear();
    }
};

ITransaction* TSchemeShard::CreateTxProgress(TIndexBuildId id) {
    return new TIndexBuilder::TTxProgress(this, id);
}

struct TSchemeShard::TIndexBuilder::TTxBilling: public TSchemeShard::TIndexBuilder::TTxBase  {
private:
    TIndexBuildId BuildIndexId;
    TInstant ScheduledAt;

public:
    explicit TTxBilling(TSelf* self, TEvPrivate::TEvIndexBuildingMakeABill::TPtr& ev)
        : TTxBase(self, TXTYPE_PROGRESS_INDEX_BUILD)
        , BuildIndexId(ev->Get()->BuildId)
        , ScheduledAt(ev->Get()->SendAt)
    {
    }

    bool DoExecute(TTransactionContext& , const TActorContext& ctx) override {
        LOG_I("TTxReply : TTxBilling"
              << ", buildIndexId# " << BuildIndexId);

        if (!Self->IndexBuilds.contains(BuildIndexId)) {
            return true;
        }

        TIndexBuildInfo::TPtr buildInfo = Self->IndexBuilds.at(BuildIndexId);

        if (!GotScheduledBilling(buildInfo)) {
            return true;
        }

        Bill(buildInfo, ScheduledAt, ctx.Now());

        AskToScheduleBilling(buildInfo);

        return true;
    }

    void DoComplete(const TActorContext&) override {
    }
};

struct TSchemeShard::TIndexBuilder::TTxReply: public TSchemeShard::TIndexBuilder::TTxBase  {
private:
    TEvTxAllocatorClient::TEvAllocateResult::TPtr AllocateResult;
    TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr ModifyResult;
    TTxId CompletedTxId = InvalidTxId;
    TEvDataShard::TEvBuildIndexProgressResponse::TPtr ShardBuildIndexProgress;
    TEvDataShard::TEvCheckConstraintProgressResponse::TPtr ShardCheckConstraintProgress;

    struct {
        TIndexBuildId BuildIndexId;
        TTabletId TabletId;
        explicit operator bool() const { return BuildIndexId && TabletId; }
    } PipeRetry;

    bool OnProgressImpl(TTransactionContext& txc, const TActorContext& ctx, const NKikimrTxDataShard::TEvCheckConstraintProgressResponse& record) {
        LOG_I("TTxReply : TEvCheckConstraintProgressResponse"
              << ", buildIndexId# " << record.GetBuildIndexId());

        const auto buildId = TIndexBuildId(record.GetBuildIndexId());
        if (!Self->IndexBuilds.contains(buildId)) {
            return true;
        }

        TIndexBuildInfo::TPtr buildInfo = Self->IndexBuilds.at(buildId);
        LOG_D("TTxReply : TEvCheckConstraintProgressResponse"
              << ", TIndexBuildInfo: " << *buildInfo
              << ", record: " << record.ShortDebugString());

        TTabletId shardId = TTabletId(record.GetTabletId());
        if (!Self->TabletIdToShardIdx.contains(shardId)) {
            return true;
        }

        TShardIdx shardIdx = Self->TabletIdToShardIdx.at(shardId);
        if (!buildInfo->Shards.contains(shardIdx)) {
            return true;
        }

        switch (buildInfo->State) {
        case TIndexBuildInfo::EState::AlterMainTable:
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::Locking:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Initiating:
            Y_ABORT("Unreachable");
        case TIndexBuildInfo::EState::Filling:
        {
            TIndexBuildInfo::TShardStatus& shardStatus = buildInfo->Shards.at(shardIdx);

            auto actualSeqNo = std::pair<ui64, ui64>(Self->Generation(), shardStatus.SeqNoRound);
            auto recordSeqNo = std::pair<ui64, ui64>(record.GetRequestSeqNoGeneration(), record.GetRequestSeqNoRound());

            if (actualSeqNo != recordSeqNo) {
                LOG_D("TTxReply : TEvCheckConstraintProgressResponse"
                      << " ignore progress message by seqNo"
                      << ", TIndexBuildInfo: " << *buildInfo
                      << ", actual seqNo for the shard " << shardId << " (" << shardIdx << ") is: "  << Self->Generation() << ":" <<  shardStatus.SeqNoRound
                      << ", record: " << record.ShortDebugString());
                Y_ABORT_UNLESS(actualSeqNo > recordSeqNo);
                return true;
            }

            shardStatus.Status = record.GetStatus();
            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            shardStatus.DebugMessage = issues.ToString();

            NIceDb::TNiceDb db(txc.DB);

            switch (shardStatus.Status) {
            case NKikimrTxDataShard::EBuildIndexStatus::INVALID:
            case NKikimrTxDataShard::EBuildIndexStatus::BUILD_ERROR:
            case NKikimrTxDataShard::EBuildIndexStatus::BAD_REQUEST:
            case NKikimrTxDataShard::EBuildIndexStatus::ABORTED:
                Y_ABORT("Unreachable");
            case NKikimrTxDataShard::EBuildIndexStatus::ACCEPTED:
            case NKikimrTxDataShard::EBuildIndexStatus::INPROGRESS:
                // no oop, wait resolution. Progress key are persisted
                break;

            case NKikimrTxDataShard::EBuildIndexStatus::DONE:
                if (buildInfo->InProgressShards.contains(shardIdx)) {
                    buildInfo->InProgressShards.erase(shardIdx);
                    buildInfo->DoneShards.emplace(shardIdx);

                    Self->IndexBuildPipes.Close(buildId, shardId, ctx);

                    Progress(buildId);
                }
                break;

            case NKikimrTxDataShard::EBuildIndexStatus::CHECKING_NOT_NULL_ERROR:
                buildInfo->Issue += TStringBuilder()
                    << "One of the shards report CHECKING_NOT_NULL_ERROR at Filling stage, process has to be canceled"
                    << ", shardId: " << shardId
                    << ", shardIdx: " << shardIdx;

                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo->Id, TIndexBuildInfo::EState::Rejection_Applying);

                Progress(buildId);
                break;
            }
            break;
        }
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_ABORT("Unreachable");
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejection_Applying:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        case TIndexBuildInfo::EState::Rejected:
            LOG_D("TTxReply : TEvCheckConstraintProgressResponse"
                  << " superfluous message " << record.ShortDebugString());
            break;
        }

        return true;
    }

    bool OnProgressImpl(TTransactionContext& txc, const TActorContext& ctx, const NKikimrTxDataShard::TEvBuildIndexProgressResponse& record) {
        LOG_I("TTxReply : TEvBuildIndexProgressResponse"
              << ", buildIndexId# " << record.GetBuildIndexId());

        const auto buildId = TIndexBuildId(record.GetBuildIndexId());
        if (!Self->IndexBuilds.contains(buildId)) {
            return true;
        }

        TIndexBuildInfo::TPtr buildInfo = Self->IndexBuilds.at(buildId);
        LOG_D("TTxReply : TEvBuildIndexProgressResponse"
              << ", TIndexBuildInfo: " << *buildInfo
              << ", record: " << record.ShortDebugString());

        TTabletId shardId = TTabletId(record.GetTabletId());
        if (!Self->TabletIdToShardIdx.contains(shardId)) {
            return true;
        }

        TShardIdx shardIdx = Self->TabletIdToShardIdx.at(shardId);
        if (!buildInfo->Shards.contains(shardIdx)) {
            return true;
        }

        switch (buildInfo->State) {
        case TIndexBuildInfo::EState::AlterMainTable:
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::Locking:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Initiating:
            Y_ABORT("Unreachable");
        case TIndexBuildInfo::EState::Filling:
        {
            TIndexBuildInfo::TShardStatus& shardStatus = buildInfo->Shards.at(shardIdx);

            auto actualSeqNo = std::pair<ui64, ui64>(Self->Generation(), shardStatus.SeqNoRound);
            auto recordSeqNo = std::pair<ui64, ui64>(record.GetRequestSeqNoGeneration(), record.GetRequestSeqNoRound());

            if (actualSeqNo != recordSeqNo) {
                LOG_D("TTxReply : TEvBuildIndexProgressResponse"
                      << " ignore progress message by seqNo"
                      << ", TIndexBuildInfo: " << *buildInfo
                      << ", actual seqNo for the shard " << shardId << " (" << shardIdx << ") is: "  << Self->Generation() << ":" <<  shardStatus.SeqNoRound
                      << ", record: " << record.ShortDebugString());
                Y_ABORT_UNLESS(actualSeqNo > recordSeqNo);
                return true;
            }

            if (record.HasLastKeyAck()) {
                if (shardStatus.LastKeyAck) {
                    //check that all LastKeyAcks are monotonously increase
                    TTableInfo::TPtr tableInfo = Self->Tables.at(buildInfo->TablePathId);
                    TVector<NScheme::TTypeInfo> keyTypes;
                    for (ui32 keyPos: tableInfo->KeyColumnIds) {
                        keyTypes.push_back(tableInfo->Columns.at(keyPos).PType);
                    }

                    TSerializedCellVec last;
                    last.Parse(shardStatus.LastKeyAck);

                    TSerializedCellVec update;
                    update.Parse(record.GetLastKeyAck());

                    int cmp = CompareBorders<true, true>(last.GetCells(),
                                                         update.GetCells(),
                                                         true,
                                                         true,
                                                         keyTypes);
                    Y_VERIFY_S(cmp < 0,
                               "check that all LastKeyAcks are monotonously increase"
                                   << ", last: " << DebugPrintPoint(keyTypes, last.GetCells(), *AppData()->TypeRegistry)
                                   << ", update: " <<  DebugPrintPoint(keyTypes, update.GetCells(), *AppData()->TypeRegistry));
                }

                shardStatus.LastKeyAck = record.GetLastKeyAck();
            }

            if (record.HasRowsDelta() || record.HasBytesDelta()) {
                TBillingStats delta(record.GetRowsDelta(), record.GetBytesDelta());
                shardStatus.Processed += delta;
                buildInfo->Processed += delta;
            }

            shardStatus.Status = record.GetStatus();
            shardStatus.UploadStatus = record.GetUploadStatus();
            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            shardStatus.DebugMessage = issues.ToString();

            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexUploadProgress(db, buildInfo, shardIdx);

            switch (shardStatus.Status) {
            case NKikimrTxDataShard::EBuildIndexStatus::INVALID:
            case  NKikimrTxDataShard::EBuildIndexStatus::CHECKING_NOT_NULL_ERROR:

                Y_ABORT("Unreachable");

            case NKikimrTxDataShard::EBuildIndexStatus::ACCEPTED:
            case NKikimrTxDataShard::EBuildIndexStatus::INPROGRESS:
                // no oop, wait resolution. Progress key are persisted
                break;

            case NKikimrTxDataShard::EBuildIndexStatus::DONE:
                if (buildInfo->InProgressShards.contains(shardIdx)) {
                    buildInfo->InProgressShards.erase(shardIdx);
                    buildInfo->DoneShards.emplace(shardIdx);

                    Self->IndexBuildPipes.Close(buildId, shardId, ctx);

                    Progress(buildId);
                }
                break;

            case NKikimrTxDataShard::EBuildIndexStatus::ABORTED:
                // datashard gracefully rebooted, reschedule shard
                if (buildInfo->InProgressShards.contains(shardIdx)) {
                    buildInfo->ToUploadShards.push_front(shardIdx);
                    buildInfo->InProgressShards.erase(shardIdx);

                    Self->IndexBuildPipes.Close(buildId, shardId, ctx);

                    Progress(buildId);
                }
                break;
            case NKikimrTxDataShard::EBuildIndexStatus::BUILD_ERROR:
                buildInfo->Issue += TStringBuilder()
                    << "One of the shards report BUILD_ERROR at Filling stage, process has to be canceled"
                    << ", shardId: " << shardId
                    << ", shardIdx: " << shardIdx;
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo->Id, TIndexBuildInfo::EState::Rejection_Applying);

                Progress(buildId);
                break;
            case NKikimrTxDataShard::EBuildIndexStatus::BAD_REQUEST:
                buildInfo->Issue += TStringBuilder()
                    << "One of the shards report BAD_REQUEST at Filling stage, process has to be canceled"
                    << ", shardId: " << shardId
                    << ", shardIdx: " << shardIdx;
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo->Id, TIndexBuildInfo::EState::Rejection_Applying);

                Progress(buildId);
                break;
            }

            break;
        }
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_ABORT("Unreachable");
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejection_Applying:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        case TIndexBuildInfo::EState::Rejected:
            LOG_D("TTxReply : TEvBuildIndexProgressResponse"
                  << " superfluous message " << record.ShortDebugString());
            break;
        }

        return true;
    }
public:
    explicit TTxReply(TSelf* self, TEvTxAllocatorClient::TEvAllocateResult::TPtr& allocateResult)
        : TTxBase(self, TXTYPE_PROGRESS_INDEX_BUILD)
        , AllocateResult(allocateResult)
    {
    }

    explicit TTxReply(TSelf* self, TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& modifyResult)
        : TTxBase(self, TXTYPE_PROGRESS_INDEX_BUILD)
        , ModifyResult(modifyResult)
    {
    }

    explicit TTxReply(TSelf* self, TTxId completedTxId)
        : TTxBase(self, TXTYPE_PROGRESS_INDEX_BUILD)
        , CompletedTxId(completedTxId)
    {
    }

    explicit TTxReply(TSelf* self, TEvDataShard::TEvBuildIndexProgressResponse::TPtr& shardBuildIndexProgress)
        : TTxBase(self, TXTYPE_PROGRESS_INDEX_BUILD)
        , ShardBuildIndexProgress(shardBuildIndexProgress)
    {
    }

    explicit TTxReply(TSelf* self, TEvDataShard::TEvCheckConstraintProgressResponse::TPtr& shardCheckConstraintProgress)
        : TTxBase(self, TXTYPE_PROGRESS_INDEX_BUILD)
        , ShardCheckConstraintProgress(shardCheckConstraintProgress)
    {
    }

    explicit TTxReply(TSelf* self, TIndexBuildId buildId, TTabletId tabletId)
        : TSchemeShard::TIndexBuilder::TTxBase(self, TXTYPE_PROGRESS_INDEX_BUILD)
        , PipeRetry({buildId, tabletId})
    {
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        if (AllocateResult) {
            return OnAllocation(txc, ctx);
        } else if (ModifyResult) {
            return OnModifyResult(txc, ctx);
        } else if (CompletedTxId) {
            return OnNotification(txc, ctx);
        } else if (ShardBuildIndexProgress || ShardCheckConstraintProgress) {
            return OnProgress(txc, ctx);
        } else if (PipeRetry) {
            return OnPipeRetry(txc, ctx);
        }

        return true;
    }

    bool OnPipeRetry(TTransactionContext&, const TActorContext& ctx) {
        const auto& buildId = PipeRetry.BuildIndexId;
        const auto& tabletId = PipeRetry.TabletId;
        const auto& shardIdx = Self->GetShardIdx(tabletId);

        LOG_I("TTxReply : PipeRetry"
              << ", buildIndexId# " << buildId
              << ", tabletId# " << tabletId
              << ", shardIdx# " << shardIdx);

        if (!Self->IndexBuilds.contains(buildId)) {
            return true;
        }

        TIndexBuildInfo::TPtr buildInfo = Self->IndexBuilds.at(buildId);

        if (!buildInfo->Shards.contains(shardIdx)) {
            return true;
        }

        LOG_D("TTxReply : PipeRetry"
              << ", TIndexBuildInfo: " << *buildInfo);

        switch (buildInfo->State) {
        case TIndexBuildInfo::EState::AlterMainTable:
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::Locking:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Initiating:
            Y_ABORT("Unreachable");
        case TIndexBuildInfo::EState::Filling:
        {
            // reschedule shard
            if (buildInfo->InProgressShards.contains(shardIdx)) {
                buildInfo->ToUploadShards.push_front(shardIdx);
                buildInfo->InProgressShards.erase(shardIdx);

                Self->IndexBuildPipes.Close(buildId, tabletId, ctx);

                // generate new message with actual LastKeyAck to continue scan
                Progress(buildId);
            }
            break;
        }
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_ABORT("Unreachable");
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejection_Applying:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        case TIndexBuildInfo::EState::Rejected:
            LOG_D("TTxReply : PipeRetry"
                  << " superfluous event"
                  << ", buildIndexId# " << buildId
                  << ", tabletId# " << tabletId
                  << ", shardIdx# " << shardIdx);
            break;
        }

        return true;
    }

    bool OnProgress(TTransactionContext& txc, const TActorContext& ctx) {
        if (ShardBuildIndexProgress) {
            const NKikimrTxDataShard::TEvBuildIndexProgressResponse& record = ShardBuildIndexProgress->Get()->Record;
            return OnProgressImpl(txc, ctx, record);
        } else { // if (ShardCheckConstraintProgress)
            const NKikimrTxDataShard::TEvCheckConstraintProgressResponse& record = ShardCheckConstraintProgress->Get()->Record;
            return OnProgressImpl(txc, ctx, record);
        }
    }

    bool OnNotification(TTransactionContext& txc, const TActorContext&) {
        const auto txId = CompletedTxId;
        if (!Self->TxIdToIndexBuilds.contains(txId)) {
            LOG_I("TTxReply : TEvNotifyTxCompletionResult superfluous message"
                  << ", txId: " << txId
                  << ", buildInfoId not found");
            return true;
        }

        const auto buildId = Self->TxIdToIndexBuilds.at(txId);
        Y_ABORT_UNLESS(Self->IndexBuilds.contains(buildId));

        TIndexBuildInfo::TPtr buildInfo = Self->IndexBuilds.at(buildId);
        LOG_I("TTxReply : TEvNotifyTxCompletionResult"
              << ", txId# " << txId
              << ", buildInfoId: " << buildInfo->Id);
        LOG_D("TTxReply : TEvNotifyTxCompletionResult"
              << ", txId# " << txId
              << ", buildInfo: " << *buildInfo);

        switch (buildInfo->State) {
        case TIndexBuildInfo::EState::Invalid:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::AlterMainTable:
        {
            Y_ABORT_UNLESS(txId == buildInfo->AlterMainTableTxId);

            buildInfo->AlterMainTableTxDone = true;
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexAlterMainTableTxDone(db, buildInfo);
            break;
        }

        case TIndexBuildInfo::EState::Locking:
        {
            Y_ABORT_UNLESS(txId == buildInfo->LockTxId);

            buildInfo->LockTxDone = true;
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexLockTxDone(db, buildInfo);

            break;
        }
        case TIndexBuildInfo::EState::GatheringStatistics:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::Initiating:
        {
            Y_ABORT_UNLESS(txId == buildInfo->InitiateTxId);

            buildInfo->InitiateTxDone = true;
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexInitiateTxDone(db, buildInfo);

            break;
        }
        case TIndexBuildInfo::EState::Filling:
            Y_ABORT("Unreachable");
        case TIndexBuildInfo::EState::Applying:
        {
            Y_ABORT_UNLESS(txId == buildInfo->ApplyTxId);

            buildInfo->ApplyTxDone = true;
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexApplyTxDone(db, buildInfo);

            break;
        }
        case TIndexBuildInfo::EState::Unlocking:
        {
            Y_ABORT_UNLESS(txId == buildInfo->UnlockTxId);

            buildInfo->UnlockTxDone = true;
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexUnlockTxDone(db, buildInfo);

            break;
        }
        case TIndexBuildInfo::EState::Done:
            Y_ABORT("Unreachable");
        case TIndexBuildInfo::EState::Cancellation_Applying:
        {
            Y_ABORT_UNLESS(txId == buildInfo->ApplyTxId);

            buildInfo->ApplyTxDone = true;
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexApplyTxDone(db, buildInfo);

            break;
        }
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        {
            Y_ABORT_UNLESS(txId == buildInfo->UnlockTxId);

            buildInfo->UnlockTxDone = true;
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexUnlockTxDone(db, buildInfo);

            break;
        }
        case TIndexBuildInfo::EState::Cancelled:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::Rejection_Applying:
        {
            Y_ABORT_UNLESS(txId == buildInfo->ApplyTxId);

            buildInfo->ApplyTxDone = true;
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexApplyTxDone(db, buildInfo);

            break;
        }
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        {
            Y_ABORT_UNLESS(txId == buildInfo->UnlockTxId);

            buildInfo->UnlockTxDone = true;
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexUnlockTxDone(db, buildInfo);

            break;
        }
        case TIndexBuildInfo::EState::Rejected:
            Y_ABORT("Unreachable");
        }

        Progress(buildId);

        return true;
    }

    void ReplyOnCreation(const TIndexBuildInfo::TPtr buildInfo,
                         const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS)
    {
        auto responseEv = MakeHolder<TEvIndexBuilder::TEvCreateResponse>(ui64(buildInfo->Id));

        Fill(*responseEv->Record.MutableIndexBuild(), buildInfo);

        auto& response = responseEv->Record;
        response.SetStatus(status);

        if (buildInfo->Issue) {
            AddIssue(response.MutableIssues(), buildInfo->Issue);
        }

        LOG_N("TIndexBuilder::TTxReply: ReplyOnCreation"
              << ", BuildIndexId: " << buildInfo->Id
              << ", status: " << Ydb::StatusIds::StatusCode_Name(status)
              << ", error: " << buildInfo->Issue
              << ", replyTo: " << buildInfo->CreateSender.ToString());
        LOG_D("Message:\n" << responseEv->Record.ShortDebugString());

        Send(buildInfo->CreateSender, std::move(responseEv), 0, buildInfo->SenderCookie);
    }

    bool OnModifyResult(TTransactionContext& txc, const TActorContext&) {
        const auto& record = ModifyResult->Get()->Record;

        const auto txId = TTxId(record.GetTxId());
        if (!Self->TxIdToIndexBuilds.contains(txId)) {
            LOG_I("TTxReply : TEvModifySchemeTransactionResult superfluous message"
                  << ", cookie: " << ModifyResult->Cookie
                  << ", txId: " << record.GetTxId()
                  << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus())
                  << ", BuildIndexId not found");
            return true;
        }

        const auto buildId = Self->TxIdToIndexBuilds.at(txId);
        Y_ABORT_UNLESS(Self->IndexBuilds.contains(buildId));

        LOG_I("TTxReply : TEvModifySchemeTransactionResult"
              << ", BuildIndexId: " << buildId
              << ", cookie: " << ModifyResult->Cookie
              << ", txId: " << record.GetTxId()
              << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus()));

        TIndexBuildInfo::TPtr buildInfo = Self->IndexBuilds.at(buildId);
        LOG_D("TTxReply : TEvModifySchemeTransactionResult"
              << ", buildInfo: " << *buildInfo
              << ", record: " << record.ShortDebugString());

        switch (buildInfo->State) {
        case TIndexBuildInfo::EState::Invalid:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::AlterMainTable:
        {
            Y_ABORT_UNLESS(txId == buildInfo->AlterMainTableTxId);

            buildInfo->AlterMainTableTxStatus = record.GetStatus();
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexAlterMainTableTxStatus(db, buildInfo);

            auto statusCode = TranslateStatusCode(record.GetStatus());

            if (statusCode != Ydb::StatusIds::SUCCESS) {
                buildInfo->Issue += TStringBuilder()
                    << "At alter main table state got unsuccess propose result"
                    << ", status: " << NKikimrScheme::EStatus_Name(buildInfo->AlterMainTableTxStatus)
                    << ", reason: " << record.GetReason();
                Self->PersistBuildIndexIssue(db, buildInfo);
                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexForget(db, buildInfo);
                EraseBuildInfo(buildInfo);
            }

            ReplyOnCreation(buildInfo, statusCode);
            break;
        }

        case TIndexBuildInfo::EState::Locking:
        {
            Y_ABORT_UNLESS(txId == buildInfo->LockTxId);

            buildInfo->LockTxStatus = record.GetStatus();
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexLockTxStatus(db, buildInfo);

            auto statusCode = TranslateStatusCode(record.GetStatus());

            if (statusCode != Ydb::StatusIds::SUCCESS) {
                buildInfo->Issue += TStringBuilder()
                    << "At locking state got unsuccess propose result"
                    << ", status: " << NKikimrScheme::EStatus_Name(buildInfo->LockTxStatus)
                    << ", reason: " << record.GetReason();
                Self->PersistBuildIndexIssue(db, buildInfo);

                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexForget(db, buildInfo);

                EraseBuildInfo(buildInfo);
            }

            ReplyOnCreation(buildInfo, statusCode);

            break;
        }
        case TIndexBuildInfo::EState::GatheringStatistics:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::Initiating:
        {
            Y_ABORT_UNLESS(txId == buildInfo->InitiateTxId);

            buildInfo->InitiateTxStatus = record.GetStatus();
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexInitiateTxStatus(db, buildInfo);

            if (record.GetStatus() == NKikimrScheme::StatusAccepted) {
                // no op
            } else if (record.GetStatus() == NKikimrScheme::StatusAlreadyExists) {
                Y_ABORT("NEED MORE TESTING");
               // no op
            } else {
                buildInfo->Issue += TStringBuilder()
                    << "At initiating state got unsuccess propose result"
                    << ", status: " << NKikimrScheme::EStatus_Name(buildInfo->InitiateTxStatus)
                    << ", reason: " << record.GetReason();
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo->Id, TIndexBuildInfo::EState::Rejection_Unlocking);
            }

            break;
        }
        case TIndexBuildInfo::EState::Filling:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::Applying:
        {
            Y_ABORT_UNLESS(txId == buildInfo->ApplyTxId);

            buildInfo->ApplyTxStatus = record.GetStatus();
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexApplyTxStatus(db, buildInfo);

            if (record.GetStatus() == NKikimrScheme::StatusAccepted) {
                // no op
            } else if (record.GetStatus() == NKikimrScheme::StatusAlreadyExists) {
                Y_ABORT("NEED MORE TESTING");
                // no op
            } else {
                buildInfo->Issue += TStringBuilder()
                    << "At applying state got unsuccess propose result"
                    << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus())
                    << ", reason: " << record.GetReason();
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo->Id, TIndexBuildInfo::EState::Rejection_Unlocking);
            }

            break;
        }
        case TIndexBuildInfo::EState::Unlocking:
        {
            Y_ABORT_UNLESS(txId == buildInfo->UnlockTxId);

            buildInfo->UnlockTxStatus = record.GetStatus();
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexUnlockTxStatus(db, buildInfo);

            if (record.GetStatus() == NKikimrScheme::StatusAccepted) {
                // no op
            } else if (record.GetStatus() == NKikimrScheme::StatusAlreadyExists) {
                // no op
            } else {
                buildInfo->Issue += TStringBuilder()
                    << "At unlocking state got unsuccess propose result"
                    << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus())
                    << ", reason: " << record.GetReason();
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo->Id, TIndexBuildInfo::EState::Rejection_Unlocking);
            }

            break;
        }
        case TIndexBuildInfo::EState::Done:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::Cancellation_Applying:
        {
            Y_ABORT_UNLESS(txId == buildInfo->ApplyTxId);

            buildInfo->ApplyTxStatus = record.GetStatus();
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexApplyTxStatus(db, buildInfo);

            if (record.GetStatus() == NKikimrScheme::StatusAccepted) {
                // no op
            } else if (record.GetStatus() == NKikimrScheme::StatusAlreadyExists) {
                Y_ABORT("NEED MORE TESTING");
                // no op
            } else {
                buildInfo->Issue += TStringBuilder()
                    << "At cancellation applying state got unsuccess propose result"
                    << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus())
                    << ", reason: " << record.GetReason();
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo->Id, TIndexBuildInfo::EState::Cancellation_Unlocking);
            }

            break;
        }
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        {
            Y_ABORT_UNLESS(txId == buildInfo->UnlockTxId);

            buildInfo->UnlockTxStatus = record.GetStatus();
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexUnlockTxStatus(db, buildInfo);

            if (record.GetStatus() == NKikimrScheme::StatusAccepted) {
                // no op
            } else if (record.GetStatus() == NKikimrScheme::StatusAlreadyExists) {
                // no op
            } else {
                buildInfo->Issue += TStringBuilder()
                    << "At cancellation unlocking state got unsuccess propose result"
                    << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus())
                    << ", reason: " << record.GetReason();
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo->Id, TIndexBuildInfo::EState::Cancelled);
            }

            break;
        }
        case TIndexBuildInfo::EState::Cancelled:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::Rejection_Applying:
        {
            Y_ABORT_UNLESS(txId == buildInfo->ApplyTxId);

            buildInfo->ApplyTxStatus = record.GetStatus();
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexApplyTxStatus(db, buildInfo);

            if (record.GetStatus() == NKikimrScheme::StatusAccepted) {
                // no op
            } else if (record.GetStatus() == NKikimrScheme::StatusAlreadyExists) {
                Y_ABORT("NEED MORE TESTING");
                // no op
            } else {
                buildInfo->Issue += TStringBuilder()
                    << "At rejection_applying state got unsuccess propose result"
                    << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus())
                    << ", reason: " << record.GetReason();
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo->Id, TIndexBuildInfo::EState::Rejection_Unlocking);
            }

            break;
        }
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        {
            Y_ABORT_UNLESS(txId == buildInfo->UnlockTxId);

            buildInfo->UnlockTxStatus = record.GetStatus();
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexUnlockTxStatus(db, buildInfo);

            if (record.GetStatus() == NKikimrScheme::StatusAccepted) {
                // no op
            } else if (record.GetStatus() == NKikimrScheme::StatusAlreadyExists) {
                // no op
            } else {
                buildInfo->Issue += TStringBuilder()
                    << "At rejection_unlocking state got unsuccess propose result"
                    << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus())
                    << ", reason: " << record.GetReason();
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo->Id, TIndexBuildInfo::EState::Rejected);
            }

            break;
        }
        case TIndexBuildInfo::EState::Rejected:
            Y_ABORT("Unreachable");
        }

        Progress(buildId);

        return true;
    }

    bool OnAllocation(TTransactionContext& txc, const TActorContext&) {
        TIndexBuildId buildId = TIndexBuildId(AllocateResult->Cookie);
        const auto txId = TTxId(AllocateResult->Get()->TxIds.front());

        LOG_I("TTxReply : TEvAllocateResult"
              << ", BuildIndexId: " << buildId
              << ", txId# " << txId);

        Y_ABORT_UNLESS(Self->IndexBuilds.contains(buildId));
        TIndexBuildInfo::TPtr buildInfo = Self->IndexBuilds.at(buildId);

        LOG_D("TTxReply : TEvAllocateResult"
              << ", buildInfo: " << *buildInfo);

        switch (buildInfo->State) {
        case TIndexBuildInfo::EState::Invalid:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::AlterMainTable:
            if (!buildInfo->AlterMainTableTxId) {
                buildInfo->AlterMainTableTxId = txId;
                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexAlterMainTableTxId(db, buildInfo);

            }
            break;

        case TIndexBuildInfo::EState::Locking:
            if (!buildInfo->LockTxId) {
                buildInfo->LockTxId = txId;
                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexLockTxId(db, buildInfo);
            }
            break;

        case TIndexBuildInfo::EState::GatheringStatistics:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::Initiating:
            if (!buildInfo->InitiateTxId) {
                buildInfo->InitiateTxId = txId;
                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexInitiateTxId(db, buildInfo);
            }
            break;

        case TIndexBuildInfo::EState::Filling:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::Applying:
            if (!buildInfo->ApplyTxId) {
                buildInfo->ApplyTxId = txId;
                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexApplyTxId(db, buildInfo);
            }
            break;

        case TIndexBuildInfo::EState::Unlocking:
            if (!buildInfo->UnlockTxId) {
                buildInfo->UnlockTxId = txId;
                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexUnlockTxId(db, buildInfo);
            }
            break;

        case TIndexBuildInfo::EState::Done:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::Cancellation_Applying:
            if (!buildInfo->ApplyTxId) {
                buildInfo->ApplyTxId = txId;
                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexApplyTxId(db, buildInfo);
            }
            break;

        case TIndexBuildInfo::EState::Cancellation_Unlocking:
            if (!buildInfo->UnlockTxId) {
                buildInfo->UnlockTxId = txId;
                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexUnlockTxId(db, buildInfo);
            }
            break;

        case TIndexBuildInfo::EState::Cancelled:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::Rejection_Applying:
            if (!buildInfo->ApplyTxId) {
                buildInfo->ApplyTxId = txId;
                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexApplyTxId(db, buildInfo);
            }
            break;
        case TIndexBuildInfo::EState::Rejection_Unlocking:
            if (!buildInfo->UnlockTxId) {
                buildInfo->UnlockTxId = txId;
                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexUnlockTxId(db, buildInfo);
            }
            break;

        case TIndexBuildInfo::EState::Rejected:
            Y_ABORT("Unreachable");
        }

        Progress(buildId);

        return true;
    }

    void DoComplete(const TActorContext&) override {
    }
};


ITransaction* TSchemeShard::CreateTxReply(TEvTxAllocatorClient::TEvAllocateResult::TPtr& allocateResult) {
    return new TIndexBuilder::TTxReply(this, allocateResult);
}

ITransaction* TSchemeShard::CreateTxReply(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& modifyResult) {
    return new TIndexBuilder::TTxReply(this, modifyResult);
}

ITransaction* TSchemeShard::CreateTxReply(TTxId completedTxId) {
    return new TIndexBuilder::TTxReply(this, completedTxId);
}

ITransaction* TSchemeShard::CreateTxReply(TEvDataShard::TEvBuildIndexProgressResponse::TPtr& progress) {
    return new TIndexBuilder::TTxReply(this, progress);
}

ITransaction* TSchemeShard::CreateTxReply(TEvDataShard::TEvCheckConstraintProgressResponse::TPtr& progress) {
    return new TIndexBuilder::TTxReply(this, progress);
}

ITransaction* TSchemeShard::CreatePipeRetry(TIndexBuildId indexBuildId, TTabletId tabletId) {
    return new TIndexBuilder::TTxReply(this, indexBuildId, tabletId);
}

ITransaction* TSchemeShard::CreateTxBilling(TEvPrivate::TEvIndexBuildingMakeABill::TPtr& ev) {
    return new TIndexBuilder::TTxBilling(this, ev);
}


} // NSchemeShard
} // NKikimr
