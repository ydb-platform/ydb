#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_olap_types.h"
#include "schemeshard_impl.h"

#include <ydb/core/scheme/scheme_types_proto.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;


class TTableInfoConstructor {
    NKikimrSchemeOp::TAlterColumnTable AlterRequest;

public:
    bool Deserialize(const NKikimrSchemeOp::TModifyScheme& modify, IErrorCollector& errors) {
        if (modify.HasAlterColumnTable()) {
            if (modify.GetOperationType() != NKikimrSchemeOp::ESchemeOpAlterColumnTable) {
                errors.AddError(NKikimrScheme::StatusSchemeError, "Invalid operation type");
                return false;
            }
            AlterRequest = modify.GetAlterColumnTable();
        } else {
            // from DDL (not known table type)
            if (modify.GetOperationType() != NKikimrSchemeOp::ESchemeOpAlterTable) {
                errors.AddError(NKikimrScheme::StatusSchemeError, "Invalid operation type");
                return false;
            }
            if (!ParseFromDSRequest(modify.GetAlterTable(), AlterRequest, errors)) {
                return false;
            }
        }

        if (!AlterRequest.HasName()) {
            errors.AddError(NKikimrScheme::StatusInvalidParameter, "No table name in Alter");
            return false;
        }

        if (AlterRequest.HasAlterSchemaPresetName()) {
            errors.AddError(NKikimrScheme::StatusSchemeError, "Changing table schema is not supported");
            return false;
        }

        if (AlterRequest.HasRESERVED_AlterTtlSettingsPresetName()) {
            errors.AddError(NKikimrScheme::StatusSchemeError, "TTL presets are not supported");
            return false;
        }

        return true;
    }

    const NKikimrSchemeOp::TColumnTableSchema* GetTableSchema(const TTablesStorage::TTableExtractedGuard& tableInfo, const TOlapStoreInfo::TPtr& storeInfo, IErrorCollector& errors) const {
        if (storeInfo) {
            if (!storeInfo->SchemaPresets.count(tableInfo->Description.GetSchemaPresetId())) {
                errors.AddError(NKikimrScheme::StatusSchemeError, "No preset for in-store column table");
                return nullptr;
            }

            auto& preset = storeInfo->SchemaPresets.at(tableInfo->Description.GetSchemaPresetId());
            auto& presetProto = storeInfo->GetDescription().GetSchemaPresets(preset.GetProtoIndex());
            if (!presetProto.HasSchema()) {
                errors.AddError(NKikimrScheme::StatusSchemeError, "No schema in preset for in-store column table");
                return nullptr;
            }
            return &presetProto.GetSchema();
        } else {
            if (!tableInfo->Description.HasSchema()) {
                errors.AddError(NKikimrScheme::StatusSchemeError, "No schema for standalone column table");
                return nullptr;
            }
            return &tableInfo->Description.GetSchema();
        }
    }

    TColumnTableInfo::TPtr BuildTableInfo(const TTablesStorage::TTableExtractedGuard& tableInfo, const TOlapStoreInfo::TPtr& storeInfo, IErrorCollector& errors) const {
        auto alterData = TColumnTableInfo::BuildTableWithAlter(*tableInfo, AlterRequest);

        const NKikimrSchemeOp::TColumnTableSchema* tableSchema = GetTableSchema(tableInfo, storeInfo, errors);
        if (!tableSchema) {
            return nullptr;
        }

        if (storeInfo && AlterRequest.HasAlterSchema()) {
            errors.AddError(NKikimrScheme::StatusSchemeError, "Can't modify schema for table in store");
            return nullptr;
        }

        TOlapSchema currentSchema;
        currentSchema.Parse(*tableSchema);
        if (!storeInfo) {
            TOlapSchemaUpdate schemaUpdate;
            if (!schemaUpdate.Parse(AlterRequest.GetAlterSchema(), errors)) {
                return nullptr;
            }

            if (!currentSchema.Update(schemaUpdate, errors)) {
                return nullptr;
            }
            NKikimrSchemeOp::TColumnTableSchema schemaUdpateProto;
            currentSchema.Serialize(schemaUdpateProto);
            *alterData->Description.MutableSchema() = schemaUdpateProto;
        }
        
        if (AlterRequest.HasAlterTtlSettings()) {
            if (!currentSchema.ValidateTtlSettings(AlterRequest.GetAlterTtlSettings(), errors)) {
                return nullptr;
            }
            const ui64 currentTtlVersion = alterData->Description.HasTtlSettings() ? alterData->Description.GetTtlSettings().GetVersion() : 0;
            *alterData->Description.MutableTtlSettings() = AlterRequest.GetAlterTtlSettings();
            alterData->Description.MutableTtlSettings()->SetVersion(currentTtlVersion + 1);
        }
        return alterData;
    }

private:
    bool ParseFromDSRequest(const NKikimrSchemeOp::TTableDescription& dsDescription, NKikimrSchemeOp::TAlterColumnTable& olapDescription, IErrorCollector& errors) const {
        olapDescription.SetName(dsDescription.GetName());

        if (dsDescription.HasTTLSettings()) {
            auto& tableTtl = dsDescription.GetTTLSettings();
            NKikimrSchemeOp::TColumnDataLifeCycle* alterTtl = olapDescription.MutableAlterTtlSettings();
            if (tableTtl.HasEnabled()) {
                auto& enabled = tableTtl.GetEnabled();
                auto* alterEnabled = alterTtl->MutableEnabled();
                if (enabled.HasColumnName()) {
                    alterEnabled->SetColumnName(enabled.GetColumnName());
                }
                if (enabled.HasExpireAfterSeconds()) {
                    alterEnabled->SetExpireAfterSeconds(enabled.GetExpireAfterSeconds());
                }
                if (enabled.HasColumnUnit()) {
                    alterEnabled->SetColumnUnit(enabled.GetColumnUnit());
                }
            } else if (tableTtl.HasDisabled()) {
                alterTtl->MutableDisabled();
            }
            if (tableTtl.HasUseTiering()) {
                alterTtl->SetUseTiering(tableTtl.GetUseTiering());
            }
        }

        for (auto&& dsColumn : dsDescription.GetColumns()) {
            NKikimrSchemeOp::TAlterColumnTableSchema* alterSchema = olapDescription.MutableAlterSchema();
            NKikimrSchemeOp::TOlapColumnDescription* olapColumn = alterSchema->AddAddColumns();
            if (!ParseFromDSRequest(dsColumn, *olapColumn, errors)) {
                return false;
            }
        }

        for (auto&& dsColumn : dsDescription.GetDropColumns()) {
            NKikimrSchemeOp::TAlterColumnTableSchema* alterSchema = olapDescription.MutableAlterSchema();
            NKikimrSchemeOp::TOlapColumnDescription* olapColumn = alterSchema->AddDropColumns();
            if (!ParseFromDSRequest(dsColumn, *olapColumn, errors)) {
                return false;
            }
        }
        return true;
    }

    bool ParseFromDSRequest(const NKikimrSchemeOp::TColumnDescription& dsColumn, NKikimrSchemeOp::TOlapColumnDescription& olapColumn, IErrorCollector& errors) const {
        olapColumn.SetName(dsColumn.GetName());
        olapColumn.SetType(dsColumn.GetType());
        if (dsColumn.HasTypeId()) { 
            olapColumn.SetTypeId(dsColumn.GetTypeId());
        }
        if (dsColumn.HasTypeInfo()) {
            *olapColumn.MutableTypeInfo() = dsColumn.GetTypeInfo();
        }
        if (dsColumn.HasNotNull()) {
            olapColumn.SetNotNull(dsColumn.GetNotNull());
        }
        if (dsColumn.HasId()) {
            olapColumn.SetId(dsColumn.GetId());
        }
        if (dsColumn.HasDefaultFromSequence()) {
            errors.AddError(NKikimrScheme::StatusInvalidParameter, "DefaultFromSequence not supported");
            return false;
        }
        if (dsColumn.HasFamilyName() || dsColumn.HasFamily()) {
            errors.AddError(NKikimrScheme::StatusInvalidParameter, "FamilyName and Family not supported");
            return false;
        }
        return true;
    }
};

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TAlterColumnTable TConfigureParts"
                << " operationId#" << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(TEvColumnShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
         return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                   << " at tabletId# " << ssId);

        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable); 
        
        TPathId pathId = txState->TargetPathId;
        TPath path = TPath::Init(pathId, context.SS);
        TString pathString = path.PathString();

        auto tableInfo = context.SS->ColumnTables.TakeVerified(pathId);
        TColumnTableInfo::TPtr alterData = tableInfo->AlterData;
        Y_VERIFY(alterData);

        TOlapStoreInfo::TPtr storeInfo;
        if (auto olapStorePath = path.FindOlapStore()) {
            storeInfo = context.SS->OlapStores.at(olapStorePath->PathId);
        }

        txState->ClearShardsInProgress();
        TString columnShardTxBody;
        {
            auto seqNo = context.SS->StartRound(*txState);
            NKikimrTxColumnShard::TSchemaTxBody tx;
            context.SS->FillSeqNo(tx, seqNo);
            
            auto* alter = tx.MutableAlterTable();

            alter->SetPathId(pathId.LocalPathId);
            *alter->MutableAlterBody() = *alterData->AlterBody;
            if (alterData->Description.HasSchema()) {
              Y_VERIFY(!storeInfo,
                       "Unexpected olap store with schema specified");
              *alter->MutableSchema() = alterData->Description.GetSchema();
            }
            if (alterData->Description.HasSchemaPresetId()) {
              const ui32 presetId = alterData->Description.GetSchemaPresetId();
              Y_VERIFY(storeInfo,
                       "Unexpected schema preset without olap store");
              Y_VERIFY(storeInfo->SchemaPresets.contains(presetId),
                       "Failed to find schema preset %" PRIu32
                       " in an olap store",
                       presetId);
              auto &preset = storeInfo->SchemaPresets.at(presetId);
              size_t presetIndex = preset.GetProtoIndex();
              *alter->MutableSchemaPreset() =
                  storeInfo->GetDescription().GetSchemaPresets(presetIndex);
            }
            if (alterData->Description.HasTtlSettings()) {
              *alter->MutableTtlSettings() =
                  alterData->Description.GetTtlSettings();
            }
            if (alterData->Description.HasSchemaPresetVersionAdj()) {
              alter->SetSchemaPresetVersionAdj(
                  alterData->Description.GetSchemaPresetVersionAdj());
            }

            Y_VERIFY(tx.SerializeToString(&columnShardTxBody));
        }

        for (auto& shard : txState->Shards) {
            TTabletId tabletId = context.SS->ShardInfos[shard.Idx].TabletID;

            if (shard.TabletType == ETabletType::ColumnShard) {
                auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
                    NKikimrTxColumnShard::TX_KIND_SCHEMA,
                    context.SS->TabletID(),
                    context.Ctx.SelfID,
                    ui64(OperationId.GetTxId()),
                    columnShardTxBody,
                    context.SS->SelectProcessingParams(txState->TargetPathId));

                context.OnComplete.BindMsgToPipe(OperationId, tabletId, shard.Idx, event.release());
            } else {
                Y_FAIL("unexpected tablet type");
            }

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << " Propose modify scheme on shard"
                                    << " tabletId: " << tabletId);
        }

        txState->UpdateShardsInProgress();
        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TAlterColumnTable TPropose"
                << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
            {TEvHive::TEvCreateTabletReply::EventType,
             TEvColumnShard::TEvProposeTransactionResult::EventType});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " HandleReply TEvOperationPlan"
                     << " at tablet: " << ssId
                     << ", stepId: " << step);

        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable); 
        
        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        auto tableInfo = context.SS->ColumnTables.TakeAlterVerified(pathId);
        context.SS->PersistColumnTableAlterRemove(db, pathId);
        context.SS->PersistColumnTable(db, pathId, *tableInfo);

        auto parentDir = context.SS->PathsById.at(path->ParentPathId);
        if (parentDir->IsLikeDirectory()) {
            ++parentDir->DirAlterVersion;
            context.SS->PersistPathDirAlterVersion(db, parentDir);
        }
        context.SS->ClearDescribePathCaches(parentDir);
        context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);

        ++path->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, path);
        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, path->PathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " HandleReply ProgressState"
                     << " at tablet: " << ssId);

        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable); 

        TSet<TTabletId> shardSet;
        for (const auto& shard : txState->Shards) {
            TShardIdx idx = shard.Idx;
            TTabletId tablet = context.SS->ShardInfos.at(idx).TabletID;
            shardSet.insert(tablet);
        }

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, txState->MinStep, shardSet);
        return false;
    }
};

class TProposedWaitParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TAlterColumnTable TProposedWaitParts"
                << " operationId#" << OperationId;
    }

public:
    TProposedWaitParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
            {TEvHive::TEvCreateTabletReply::EventType,
             TEvColumnShard::TEvProposeTransactionResult::EventType,
             TEvPrivate::TEvOperationPlan::EventType});
    }

    bool HandleReply(TEvColumnShard::TEvNotifyTxCompletionResult::TPtr& ev, TOperationContext& context) override {
        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable); 
        auto shardId = TTabletId(ev->Get()->Record.GetOrigin());
        auto shardIdx = context.SS->MustGetShardIdx(shardId);
        Y_VERIFY(context.SS->ShardInfos.contains(shardIdx));

        txState->ShardsInProgress.erase(shardIdx);
        return txState->ShardsInProgress.empty();
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " ProgressState"
                     << " at tablet: " << ssId);

        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable);
        txState->ClearShardsInProgress();

        for (auto& shard : txState->Shards) {
            TTabletId tabletId = context.SS->ShardInfos[shard.Idx].TabletID;
            switch (shard.TabletType) {
                case ETabletType::ColumnShard: {
                    auto event = std::make_unique<TEvColumnShard::TEvNotifyTxCompletion>(ui64(OperationId.GetTxId()));

                    context.OnComplete.BindMsgToPipe(OperationId, tabletId, shard.Idx, event.release());
                    txState->ShardsInProgress.insert(shard.Idx);
                    break;
                }
                default: {
                    Y_FAIL("unexpected tablet type");
                }
            }

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << " wait for NotifyTxCompletionResult"
                                    << " tabletId: " << tabletId);
        }

        return false;
    }
};

class TAlterColumnTable: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::ConfigureParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::ProposedWaitParts;
        case TTxState::ProposedWaitParts:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigureParts>(OperationId);
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::ProposedWaitParts:
            return MakeHolder<TProposedWaitParts>(OperationId);
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

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));
 
        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = Transaction.HasAlterColumnTable() ? Transaction.GetAlterColumnTable().GetName() : Transaction.GetAlterTable().GetName();
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterColumnTable Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        TPath path = TPath::Resolve(parentPathStr, context.SS).Dive(name);
        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsColumnTable()
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        auto tableInfo = context.SS->ColumnTables.TakeVerified(path.Base()->PathId);

        if (tableInfo->AlterVersion == 0) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "Table is not created yet");
            return result;
        }
        if (tableInfo->AlterData) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "There's another Alter in flight");
            return result;
        }

        TProposeErrorCollector errors(*result);
        TTableInfoConstructor schemaConstructor;
        if (!schemaConstructor.Deserialize(Transaction, errors)) {
            return result;
        }

        TOlapStoreInfo::TPtr storeInfo;
        if (tableInfo->OlapStorePathId) {
            auto& storePathId = *tableInfo->OlapStorePathId;
            TPath storePath = TPath::Init(storePathId, context.SS);
            {
                TPath::TChecker checks = storePath.Check();
                checks
                    .NotEmpty()
                    .IsResolved()
                    .IsOlapStore()
                    .NotUnderOperation();

                if (!checks) {
                    result->SetError(checks.GetStatus(), checks.GetError());
                    return result;
                }
            }

            Y_VERIFY(context.SS->OlapStores.contains(storePathId));
            storeInfo = context.SS->OlapStores.at(storePathId);
        }

        TColumnTableInfo::TPtr alterData = schemaConstructor.BuildTableInfo(tableInfo, storeInfo, errors);
        if (!alterData) {
            return result;
        }
        tableInfo->AlterData = alterData;

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        NIceDb::TNiceDb db(context.GetDB());

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterColumnTable, path->PathId);
        txState.State = TTxState::ConfigureParts;

        // TODO: we need to know all shards where this table is currently active
        for (ui64 columnShardId : tableInfo->ColumnShards) {
            auto tabletId = TTabletId(columnShardId);
            auto shardIdx = context.SS->TabletIdToShardIdx.at(tabletId);

            Y_VERIFY_S(context.SS->ShardInfos.contains(shardIdx), "Unknown shardIdx " << shardIdx);
            txState.Shards.emplace_back(shardIdx, context.SS->ShardInfos[shardIdx].TabletType, TTxState::ConfigureParts);

            context.SS->ShardInfos[shardIdx].CurrentTxId = OperationId.GetTxId();
            context.SS->PersistShardTx(db, shardIdx, OperationId.GetTxId());
        }

        path->LastTxId = OperationId.GetTxId();
        path->PathState = TPathElement::EPathState::EPathStateAlter;
        context.SS->PersistLastTxId(db, path.Base());

        if (storeInfo) {
            auto& storePathId = *tableInfo->OlapStorePathId;
            TPath storePath = TPath::Init(storePathId, context.SS);

            Y_VERIFY(storeInfo->ColumnTables.contains(path->PathId));
            storeInfo->ColumnTablesUnderOperation.insert(path->PathId);

            // Sequentially chain operations in the same olap store
            if (context.SS->Operations.contains(storePath.Base()->LastTxId)) {
                context.OnComplete.Dependence(storePath.Base()->LastTxId, OperationId.GetTxId());
            }
            storePath.Base()->LastTxId = OperationId.GetTxId();
            context.SS->PersistLastTxId(db, storePath.Base());
        }

        context.SS->PersistColumnTableAlter(db, path->PathId, *alterData);
        context.SS->PersistTxState(db, OperationId);

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TAlterColumnTable");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterColumnTable AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterColumnTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterColumnTable>(id, tx);
}

ISubOperation::TPtr CreateAlterColumnTable(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return MakeSubOperation<TAlterColumnTable>(id, state);
}

}
