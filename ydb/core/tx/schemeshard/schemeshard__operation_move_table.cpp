#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/mind/hive/hive.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TBaseShardMoveHelper {
protected:
    TSchemeShard* SS;
    const TPath& SrcPath;
    const TPath& DstPath;
public:
    TBaseShardMoveHelper(TSchemeShard* ss, const TPath& srcPath, const TPath& dstPath)
        : SS(ss)
        , SrcPath(srcPath)
        , DstPath(dstPath)
    {}
    virtual ~TBaseShardMoveHelper() = default;
    virtual TTabletTypes::EType GetTabletType() const = 0;
    virtual std::vector<TShardIdx> GetSrcShards() const = 0;//TODO Move me to ss
    virtual TString GetSerializedTxMsg(const TMessageSeqNo& seqNo) const = 0;
    virtual TAutoPtr<::NActors::IEventBase> MakeProposalEvent(const TPathId& targetPathId, const TOperationId& operationId, const TString& txBody, const TMessageSeqNo& seqNo, const TActorContext& actorCtx) = 0;
    virtual TPath CopyTable(NIceDb::TNiceDb& db) = 0;
};

class TDataShardMoveHelper: public TBaseShardMoveHelper {
private:
    TTableInfo::TPtr SrcTable;
public:
    TDataShardMoveHelper(TSchemeShard* ss, const TPath& srcPath, const TPath& dstPath)
        : TBaseShardMoveHelper(ss, srcPath, dstPath)
        , SrcTable(ss->Tables.at(srcPath->PathId))
    {}
    TTabletTypes::EType GetTabletType() const override {
        return ETabletType::DataShard;
    }
    std::vector<TShardIdx> GetSrcShards() const override {
        std::vector<TShardIdx> idxs;
        for (const auto& shard : SrcTable->GetPartitions()) {
            idxs.emplace_back(shard.ShardIdx);
        }
        return idxs;
    }
    TString GetSerializedTxMsg(const TMessageSeqNo& seqNo) const override {
        NKikimrTxDataShard::TFlatSchemeTransaction tx;
        SS->FillSeqNo(tx, seqNo);
        auto move = tx.MutableMoveTable();
        SrcPath->PathId.ToProto(move->MutablePathId());
        move->SetTableSchemaVersion(SrcTable->AlterVersion+1);

        DstPath->PathId.ToProto(move->MutableDstPathId());
        move->SetDstPath(TPath::Init(DstPath->PathId, SS).PathString());

        for (const auto& child: SrcPath->GetChildren()) {
            auto name = child.first;

            TPath srcChildPath = SrcPath.Child(name);
            Y_ABORT_UNLESS(srcChildPath.IsResolved());

            if (srcChildPath.IsDeleted()) {
                continue;
            }
            if (srcChildPath.IsSequence()) {
                continue;
            }

            TPath dstIndexPath = DstPath.Child(name);
            Y_ABORT_UNLESS(dstIndexPath.IsResolved());

            auto remap = move->AddReMapIndexes();
            srcChildPath->PathId.ToProto(remap->MutableSrcPathId());
            dstIndexPath->PathId.ToProto(remap->MutableDstPathId());
        }
        TString result;
        Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&result);
        return result;
    }
    TAutoPtr<::NActors::IEventBase> MakeProposalEvent(const TPathId& targetPathId, const TOperationId& operationId, const TString& txBody, const TMessageSeqNo& seqNo, const TActorContext& actorCtx) override {
        Y_UNUSED(seqNo); //???
        return SS->MakeDataShardProposal(targetPathId, operationId, txBody, actorCtx).Release();
    }
    TPath CopyTable(NIceDb::TNiceDb& db) override {
        Y_ABORT_UNLESS(SS->Tables.contains(SrcPath.Base()->PathId));
        TTableInfo::TPtr tableInfo = TTableInfo::DeepCopy(*SS->Tables.at(SrcPath.Base()->PathId));
        tableInfo->ResetDescriptionCache();
        tableInfo->AlterVersion += 1;
        // copy table info
        SS->Tables[DstPath.Base()->PathId] = tableInfo;
        SS->PersistTable(db, DstPath.Base()->PathId);
        SS->PersistTablePartitionStats(db, DstPath.Base()->PathId, tableInfo);
        return DstPath;
    }
};

class TColumnShardMoveHelper: public TBaseShardMoveHelper {
private:
    NKikimr::NSchemeShard::TTablesStorage::TTableReadGuard SrcTable;
public:
    TColumnShardMoveHelper(TSchemeShard* ss, const TPath& srcPath, const TPath& dstPath)
        : TBaseShardMoveHelper(ss, srcPath, dstPath)
        , SrcTable(ss->ColumnTables.GetVerified(srcPath.Base()->PathId))
    {}
    TTabletTypes::EType GetTabletType() const override {
        return ETabletType::ColumnShard;
    }

    std::vector<TShardIdx> GetSrcShards() const override {
        std::vector<TShardIdx> idxs;
        for (const auto& id: SrcTable->GetShardIdsSet()) {
            idxs.emplace_back(SS->TabletIdToShardIdx.at(TTabletId(id)));
        }
        return idxs;
    }
    TString GetSerializedTxMsg(const TMessageSeqNo& seqNo) const override {
        NKikimrTxColumnShard::TSchemaTxBody tx;
        SS->FillSeqNo(tx, seqNo);
        auto move = tx.MutableMoveTable();
        move->SetSrcPathId(SrcPath->PathId.LocalPathId);
        move->SetDstPathId(DstPath->PathId.LocalPathId);
        TString result;
        Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&result);
        return result;
    }
    TAutoPtr<::NActors::IEventBase> MakeProposalEvent(const TPathId& targetPathId, const TOperationId& operationId, const TString& txBody, const TMessageSeqNo& seqNo, const TActorContext& actorCtx) override {
        return std::make_unique<TEvColumnShard::TEvProposeTransaction>(
            NKikimrTxColumnShard::TX_KIND_SCHEMA,
            SS->TabletID(),
            actorCtx.SelfID,
            ui64(operationId.GetTxId()),
            txBody, seqNo,
            SS->SelectProcessingParams(targetPathId),
            0, 0
        ).release();
    }
    TPath CopyTable(NIceDb::TNiceDb& db) override {
        auto srcTable = SS->ColumnTables.GetVerified(SrcPath.Base()->PathId);
        auto tableInfo = SS->ColumnTables.BuildNew(DstPath.Base()->PathId, srcTable.GetPtr());
        //tableInfo->ResetDescriptionCache();
        tableInfo->AlterVersion += 1;
        SS->PersistColumnTable(db, DstPath.Base()->PathId, *tableInfo, false);
        //SS->PersistTablePartitionStats(db, DstPath.Base()->PathId, tableInfo.GetPtr());
        return DstPath;
    }
};

std::unique_ptr<TBaseShardMoveHelper> MakeMoveHelper(TSchemeShard* ss, const TPath& srcPath, const TPath& dstPath) {
    if (srcPath->IsTable()) {
        return std::make_unique<TDataShardMoveHelper>(ss, srcPath, dstPath);
    }
    if (srcPath->IsColumnTable()) {
        return std::make_unique<TColumnShardMoveHelper>(ss, srcPath, dstPath);
    }
    AFL_VERIFY(false);
    return {};
}

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TMoveTable TConfigureParts"
            << ", operationId: " << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvColumnShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvProposeTransactionResult"
                               << " at tabletId# " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvProposeTransactionResult"
                                << " message# " << ev->Get()->Record.ShortDebugString());

        if (!NTableState::CollectProposeTransactionResults(OperationId, ev, context)) {
            return false;
        }

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveTable);
        //Y_ABORT_UNLESS(txState->MinStep); // we have to have right minstep

        return true;

    }

    bool HandleReply(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvProposeTransactionResult"
                               << " at tabletId# " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvProposeTransactionResult"
                                << " message# " << ev->Get()->Record.ShortDebugString());

        if (!NTableState::CollectProposeTransactionResults(OperationId, ev, context)) {
            return false;
        }

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveTable);
        Y_ABORT_UNLESS(txState->MinStep); // we have to have right minstep

        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at tablet# " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveTable);
        Y_ABORT_UNLESS(txState->SourcePathId);

        TPath dstPath = TPath::Init(txState->TargetPathId, context.SS);
        Y_ABORT_UNLESS(dstPath.IsResolved());
        TPath srcPath = TPath::Init(txState->SourcePathId, context.SS);
        Y_ABORT_UNLESS(srcPath.IsResolved());

        NIceDb::TNiceDb db(context.GetDB());
        const auto moveHelper = MakeMoveHelper(context.SS, srcPath, dstPath);
        // txState catches table shards
        if (!txState->Shards) {
            const auto shards = moveHelper->GetSrcShards();
            txState->Shards.reserve(shards.size());
            for (const auto& idx: shards) {
                auto& shardInfo = context.SS->ShardInfos[idx];

                txState->Shards.emplace_back(idx, moveHelper->GetTabletType(), TTxState::ConfigureParts);

                shardInfo.CurrentTxId = OperationId.GetTxId();
                context.SS->PersistShardTx(db, idx, OperationId.GetTxId());
            }
            context.SS->PersistTxState(db, OperationId);
        }
        Y_ABORT_UNLESS(txState->Shards.size());
        const auto& seqNo = context.SS->StartRound(*txState);
        TString txBody = moveHelper->GetSerializedTxMsg(seqNo);
        // send messages
        txState->ClearShardsInProgress();
        //TODO moveto moveHelper
        if (srcPath->IsColumnTable()) {
            for (const auto& shard: txState->Shards) {
                const auto& tabletId = context.SS->ShardInfos[shard.Idx].TabletID;
                auto event = moveHelper->MakeProposalEvent(txState->TargetPathId, OperationId, txBody, seqNo, context.Ctx);
                context.OnComplete.BindMsgToPipe(OperationId, tabletId, shard.Idx, event);
            }
        } else {
            for (ui32 i = 0; i < txState->Shards.size(); ++i) {
                auto idx = txState->Shards[i].Idx;
                auto datashardId = context.SS->ShardInfos[idx].TabletID;

                auto event = context.SS->MakeDataShardProposal(txState->TargetPathId, OperationId, txBody, context.Ctx);
                context.OnComplete.BindMsgToPipe(OperationId, datashardId, idx, event.Release());
            }
        }

        txState->UpdateShardsInProgress(TTxState::ConfigureParts);
        return false;
    }
};

void MarkSrcDropped(NIceDb::TNiceDb& db,
                    TOperationContext& context,
                    TOperationId operationId,
                    const TTxState& txState,
                    TPath& srcPath)
{
    const auto isBackupTable = context.SS->IsBackupTable(srcPath->PathId);
    DecAliveChildrenDirect(operationId, srcPath.Parent().Base(), context, isBackupTable);
    srcPath.DomainInfo()->DecPathsInside(context.SS, 1, isBackupTable);

    srcPath->SetDropped(txState.PlanStep, operationId.GetTxId());
    context.SS->PersistDropStep(db, srcPath->PathId, txState.PlanStep, operationId);
    if (srcPath->IsTable()) {
        context.SS->Tables.at(srcPath->PathId)->DetachShardsStats();
    } else {
        //TODO
    }
    context.SS->PersistRemoveTable(db, srcPath->PathId, context.Ctx);
    context.SS->PersistUserAttributes(db, srcPath->PathId, srcPath->UserAttrs, nullptr);

    IncParentDirAlterVersionWithRepublish(operationId, srcPath, context);
}

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;
    TTxState::ETxState& NextState;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TMoveTable TPropose"
            << ", operationId: " << OperationId;
    }
public:
    TPropose(TOperationId id, TTxState::ETxState& nextState)
        : OperationId(id)
        , NextState(nextState)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, TEvDataShard::TEvProposeTransactionResult::EventType, TEvColumnShard::TEvProposeTransactionResult::EventType});
    }

    bool HandleReply(TEvColumnShard::TEvNotifyTxCompletionResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        const auto& evRecord = ev->Get()->Record;

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " HandleReply TEvNotifyTxCompletionResult"
                     << " at tablet: " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvNotifyTxCompletionResult"
                     << " triggered early"
                     << ", message: " << evRecord.ShortDebugString());

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        const auto& evRecord = ev->Get()->Record;

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " HandleReply TEvSchemaChanged"
                     << " at tablet: " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvSchemaChanged"
                     << " triggered early"
                     << ", message: " << evRecord.ShortDebugString());

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveTable);

        auto srcPath = TPath::Init(txState->SourcePathId, context.SS);
        auto dstPath = TPath::Init(txState->TargetPathId, context.SS);

        NIceDb::TNiceDb db(context.GetDB());

        txState->PlanStep = step;
        context.SS->PersistTxPlanStep(db, OperationId, step);

        // move shards
        for (const auto& shard : txState->Shards) {
            auto shardIdx = shard.Idx;
            TShardInfo& shardInfo = context.SS->ShardInfos[shardIdx];

            shardInfo.PathId = dstPath->PathId;
            context.SS->DecrementPathDbRefCount(srcPath.Base()->PathId, "move shard");
            context.SS->IncrementPathDbRefCount(dstPath.Base()->PathId, "move shard");
            context.SS->PersistShardPathId(db, shardIdx, dstPath.Base()->PathId);

            srcPath.Base()->DecShardsInside();
            dstPath.Base()->IncShardsInside();
        }

        Y_ABORT_UNLESS(!context.SS->Tables.contains(dstPath.Base()->PathId));
        const auto moveHelper = MakeMoveHelper(context.SS, srcPath, dstPath);
        moveHelper->CopyTable(db);
        context.SS->IncrementPathDbRefCount(dstPath.Base()->PathId, "move table info");

        dstPath->StepCreated = step;
        context.SS->PersistCreateStep(db, dstPath.Base()->PathId, step);
        dstPath.DomainInfo()->IncPathsInside(context.SS);

        dstPath.Activate();
        IncParentDirAlterVersionWithRepublish(OperationId, dstPath, context);

        NextState = TTxState::WaitShadowPathPublication;
        context.SS->ChangeTxState(db, OperationId, TTxState::WaitShadowPathPublication);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveTable);
        Y_ABORT_UNLESS(txState->SourcePathId);
        //Y_ABORT_UNLESS(txState->MinStep);

        TSet<TTabletId> shardSet;
        for (const auto& shard : txState->Shards) {
            TShardIdx idx = shard.Idx;
            TTabletId tablet = context.SS->ShardInfos.at(idx).TabletID;
            TPath srcPath = TPath::Init(txState->SourcePathId, context.SS);
            if (srcPath->IsColumnTable()) {
                auto event = std::make_unique<TEvColumnShard::TEvNotifyTxCompletion>(ui64(OperationId.GetTxId()));
                context.OnComplete.BindMsgToPipe(OperationId, tablet, shard.Idx, event.release());
            }
            shardSet.insert(tablet);
        }

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, txState->MinStep, std::move(shardSet));
        return false;
    }
};

class TWaitRenamedPathPublication: public TSubOperationState {
private:
    TOperationId OperationId;

    TPathId ActivePathId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TMoveTable TWaitRenamedPathPublication"
                << " operationId: " << OperationId;
    }

public:
    TWaitRenamedPathPublication(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, TEvDataShard::TEvProposeTransactionResult::EventType, TEvPrivate::TEvOperationPlan::EventType});
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvDataShard::TEvSchemaChanged"
                               << ", save it"
                               << ", at schemeshard: " << ssId);

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvColumnShard::TEvNotifyTxCompletionResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        const auto& evRecord = ev->Get()->Record;

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " HandleReply TEvNotifyTxCompletionResult"
                     << " at tablet: " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvNotifyTxCompletionResult"
                     << " triggered early"
                     << ", message: " << evRecord.ShortDebugString());

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvCompletePublication::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvPrivate::TEvCompletePublication"
                               << ", msg: " << ev->Get()->ToString()
                               << ", at tablet# " << ssId);

        Y_ABORT_UNLESS(ActivePathId == ev->Get()->PathId);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::DeletePathBarrier);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        context.OnComplete.RouteByTabletsFromOperation(OperationId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", operation type: " << TTxState::TypeName(txState->TxType)
                               << ", at tablet# " << ssId);

        TPath srcPath = TPath::Init(txState->SourcePathId, context.SS);

        if (srcPath.IsActive()) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << ", no renaming has been detected for this operation");

            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::DeletePathBarrier);
            return true;
        }

        auto activePath = TPath::Resolve(srcPath.PathString(), context.SS);
        Y_ABORT_UNLESS(activePath.IsResolved());

        Y_ABORT_UNLESS(activePath != srcPath);

        ActivePathId = activePath->PathId;
        context.OnComplete.PublishAndWaitPublication(OperationId, activePath->PathId);

        return false;
    }
};

class TDeleteTableBarrier: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TMoveTable TDeleteTableBarrier"
                << " operationId: " << OperationId;
    }

public:
    TDeleteTableBarrier(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, TEvDataShard::TEvProposeTransactionResult::EventType, TEvPrivate::TEvOperationPlan::EventType});
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvDataShard::TEvSchemaChanged"
                               << ", save it"
                               << ", at schemeshard: " << ssId);

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvColumnShard::TEvNotifyTxCompletionResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        const auto& evRecord = ev->Get()->Record;

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " HandleReply TEvNotifyTxCompletionResult"
                     << " at tablet: " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvNotifyTxCompletionResult"
                     << " triggered early"
                     << ", message: " << evRecord.ShortDebugString());

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvCompleteBarrier::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvPrivate:TEvCompleteBarrier"
                               << ", msg: " << ev->Get()->ToString()
                               << ", at tablet# " << ssId);

        NIceDb::TNiceDb db(context.GetDB());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        auto srcPath = TPath::Init(txState->SourcePathId, context.SS);
        auto dstPath = TPath::Init(txState->TargetPathId, context.SS);

        Y_ABORT_UNLESS(txState->PlanStep);

        MarkSrcDropped(db, context, OperationId, *txState, srcPath);
        if (srcPath->IsTable()) {
            Y_ABORT_UNLESS(context.SS->Tables.contains(dstPath.Base()->PathId));
            auto tableInfo = context.SS->Tables.at(dstPath.Base()->PathId);

            if (tableInfo->IsTTLEnabled() && !context.SS->TTLEnabledTables.contains(dstPath.Base()->PathId)) {
                context.SS->TTLEnabledTables[dstPath.Base()->PathId] = tableInfo;
                // MarkSrcDropped() removes srcPath from TTLEnabledTables & decrements the counters
                context.SS->TabletCounters->Simple()[COUNTER_TTL_ENABLED_TABLE_COUNT].Add(1);

                const auto now = context.Ctx.Now();
                for (auto& shard : tableInfo->GetPartitions()) {
                    auto& lag = shard.LastCondEraseLag;
                    lag = now - shard.LastCondErase;
                    context.SS->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].IncrementFor(lag->Seconds());
                }
            }
        } else {
            const auto tableInfo = context.SS->ColumnTables.GetVerified(dstPath.Base()->PathId);
            Y_ABORT_UNLESS(tableInfo);
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        context.OnComplete.RouteByTabletsFromOperation(OperationId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", operation type: " << TTxState::TypeName(txState->TxType)
                               << ", at tablet# " << ssId);

        context.OnComplete.Barrier(OperationId, "RenamePathBarrier");
        return false;
    }
};

class TDone: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TMoveTable TDone"
            << ", operationId: " << OperationId;
    }
public:
    TDone(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), AllIncomingEvents());
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        const TActorId& ackTo = ev->Sender;

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TProposedDeletePart"
                               << " repeated message, ack it anyway"
                               << " at tablet: " << ssId);

        THolder<TEvDataShard::TEvSchemaChangedResult> event = MakeHolder<TEvDataShard::TEvSchemaChangedResult>();
        event->Record.SetTxId(ui64(OperationId.GetTxId()));

        context.OnComplete.Send(ackTo, std::move(event));
        return false;
    }

    bool HandleReply(TEvColumnShard::TEvNotifyTxCompletionResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        const auto& evRecord = ev->Get()->Record;

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " HandleReply TEvNotifyTxCompletionResult"
                     << " at tablet: " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvNotifyTxCompletionResult"
                     << " triggered early"
                     << ", message: " << evRecord.ShortDebugString());

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveTable);

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", SourcePathId: " << txState->SourcePathId
                               << ", TargetPathId: " << txState->TargetPathId
                               << ", at schemeshard: " << ssId);

        // clear resources on src
        NIceDb::TNiceDb db(context.GetDB());
        TPathElement::TPtr srcPath = context.SS->PathsById.at(txState->SourcePathId);
        context.OnComplete.ReleasePathState(OperationId, srcPath->PathId, TPathElement::EPathState::EPathStateNotExist);

        TPathElement::TPtr dstPath = context.SS->PathsById.at(txState->TargetPathId);
        context.OnComplete.ReleasePathState(OperationId, dstPath->PathId, TPathElement::EPathState::EPathStateNoChanges);

        context.OnComplete.DoneOperation(OperationId);
        return true;
    }
};

class TMoveTable: public TSubOperation {
    TTxState::ETxState AfterPropose = TTxState::Invalid;

    static TTxState::ETxState NextState() {
        return TTxState::ConfigureParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return AfterPropose;
        case TTxState::WaitShadowPathPublication:
            return TTxState::DeletePathBarrier;
        case TTxState::DeletePathBarrier:
            return TTxState::ProposedWaitParts;
        case TTxState::ProposedWaitParts:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigureParts>(OperationId);
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId, AfterPropose);
        case TTxState::WaitShadowPathPublication:
            return MakeHolder<TWaitRenamedPathPublication>(OperationId);
        case TTxState::DeletePathBarrier:
            return MakeHolder<TDeleteTableBarrier>(OperationId);
        case TTxState::ProposedWaitParts:
            return MakeHolder<NTableState::TProposedWaitParts>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto acceptExisted = !Transaction.GetFailOnExist();
        const auto& opDescr = Transaction.GetMoveTable();

        const TString& srcPathStr = opDescr.GetSrcPath();
        const TString& dstPathStr = opDescr.GetDstPath();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TMoveTable Propose"
                         << ", from: "<< srcPathStr
                         << ", to: " << dstPathStr
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        THolder<TProposeResponse> result;
        result.Reset(new TEvSchemeShard::TEvModifySchemeTransactionResult(
            NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId)));

        TString errStr;

        TPath srcPath = TPath::Resolve(srcPathStr, context.SS);
        {
            if (!srcPath->IsTable() && !srcPath->IsColumnTable()) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed, "Cannot move non-tables");
            }
            TPath::TChecker checks = srcPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotBackupTable()
                .NotAsyncReplicaTable()
                .NotUnderTheSameOperation(OperationId.GetTxId())
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TPath dstPath = TPath::Resolve(dstPathStr, context.SS);
        TPath dstParent = dstPath.Parent();

        {
            TPath::TChecker checks = dstParent.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .FailOnRestrictedCreateInTempZone(Transaction.GetAllowCreateInTempDir());

                if (dstParent.IsUnderDeleting()) {
                    checks
                        .IsUnderDeleting()
                        .IsUnderTheSameOperation(OperationId.GetTxId());
                } else if (dstParent.IsUnderMoving()) {
                    // it means that dstPath is free enough to be the move destination
                    checks
                        .IsUnderMoving()
                        .IsUnderTheSameOperation(OperationId.GetTxId());
                } else if (dstParent.IsUnderCreating()) {
                    checks
                        .IsUnderCreating()
                        .IsUnderTheSameOperation(OperationId.GetTxId());
                } else {
                    checks
                        .NotUnderOperation();
                }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        if (dstParent.IsUnderOperation()) {
            dstPath = TPath::ResolveWithInactive(OperationId, dstPathStr, context.SS);
        }

        {
            TPath::TChecker checks = dstPath.Check();
            checks.IsAtLocalSchemeShard();
            if (dstPath.IsResolved()) {
                checks
                    .IsResolved();

                if (dstPath.IsUnderDeleting()) {
                    checks
                        .IsUnderDeleting()
                        .IsUnderTheSameOperation(OperationId.GetTxId());
                } else if (dstPath.IsUnderMoving()) {
                    // it means that dstPath is free enough to be the move destination
                    checks
                        .IsUnderMoving()
                        .IsUnderTheSameOperation(OperationId.GetTxId());
                } else {
                    checks
                        .NotUnderTheSameOperation(OperationId.GetTxId())
                        .FailOnExist(srcPath->IsColumnTable() ? TPathElement::EPathType::EPathTypeColumnTable : TPathElement::EPathType::EPathTypeTable, acceptExisted);
                }
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                checks
                    .DepthLimit()
                    .IsValidLeafName();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (!context.SS->CheckLocks(srcPath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        auto guard = context.DbGuard();
        TPathId allocatedPathId = context.SS->AllocatePathId();
        context.MemChanges.GrabNewPath(context.SS, allocatedPathId);
        context.MemChanges.GrabPath(context.SS, dstParent.Base()->PathId);
        context.MemChanges.GrabPath(context.SS, srcPath.Base()->PathId);
        context.MemChanges.GrabPath(context.SS, srcPath.Base()->ParentPathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabNewIndex(context.SS, allocatedPathId);

        context.DbChanges.PersistPath(allocatedPathId);
        context.DbChanges.PersistPath(dstParent.Base()->PathId);
        context.DbChanges.PersistPath(srcPath.Base()->PathId);
        context.DbChanges.PersistPath(srcPath.Base()->ParentPathId);
        context.DbChanges.PersistApplyUserAttrs(allocatedPathId);
        context.DbChanges.PersistTxState(OperationId);

        // create new path and inherit properties from src
        dstPath.MaterializeLeaf(srcPath.Base()->Owner, allocatedPathId, /*allowInactivePath*/ true);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);
        dstPath.Base()->CreateTxId = OperationId.GetTxId();
        dstPath.Base()->LastTxId = OperationId.GetTxId();
        dstPath.Base()->PathState = TPathElement::EPathState::EPathStateCreate;
        dstPath.Base()->PathType = srcPath.Base()->PathType;
        dstPath.Base()->UserAttrs->AlterData = srcPath.Base()->UserAttrs;
        dstPath.Base()->ACL = srcPath.Base()->ACL;

        IncAliveChildrenSafeWithUndo(OperationId, dstParent, context); // for correct discard of ChildrenExist prop

        // create tx state, do not catch shards right now
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxMoveTable, dstPath.Base()->PathId, srcPath.Base()->PathId);
        txState.State = TTxState::ConfigureParts;

        srcPath->PathState = TPathElement::EPathState::EPathStateMoving;
        srcPath->LastTxId = OperationId.GetTxId();

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);
        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, srcPath, context.SS, context.OnComplete);

        const bool isColumnTable = srcPath->IsColumnTable();
        // wait splits
        if (isColumnTable) {
            auto columnTableInfo = context.SS->ColumnTables.GetVerified(srcPath.Base()->PathId);
            //TODO handle ongoing resharding
            Y_UNUSED(columnTableInfo);
        } else {
            TTableInfo::TPtr tableSrc = context.SS->Tables.at(srcPath.Base()->PathId);
            for (auto splitTx: tableSrc->GetSplitOpsInFlight()) {
                context.OnComplete.Dependence(splitTx.GetTxId(), OperationId.GetTxId());
            }
        }

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TMoveTable AbortPropose"
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << context.SS->TabletID());
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TMoveTable AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateMoveTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TMoveTable>(id, tx);
}

ISubOperation::TPtr CreateMoveTable(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TMoveTable>(id, state);
}

}
