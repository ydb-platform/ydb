#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

TColumnTableInfo::TPtr ParseParams(
        const TPath& path, const TColumnTableInfo::TPtr& tableInfo, const TOlapStoreInfo::TPtr& storeInfo,
        const NKikimrSchemeOp::TAlterColumnTable& alter, const TSubDomainInfo& subDomain,
        NKikimrScheme::EStatus& status, TString& errStr, TOperationContext& context)
{
    Y_UNUSED(path);
    Y_UNUSED(context);
    Y_UNUSED(subDomain);

    if (alter.HasAlterSchema() || alter.HasAlterSchemaPresetName()) {
        status = NKikimrScheme::StatusInvalidParameter;
        errStr = "Changing table schema is not supported";
        return nullptr;
    }

    if (alter.HasRESERVED_AlterTtlSettingsPresetName()) {
        status = NKikimrScheme::StatusInvalidParameter;
        errStr = "TTL presets are not supported";
        return nullptr;
    }

    TColumnTableInfo::TPtr alterData = new TColumnTableInfo(*tableInfo);
    alterData->AlterBody.ConstructInPlace(alter);
    ++alterData->AlterVersion;

    ui64 currentTtlVersion = 0;
    if (alterData->Description.HasTtlSettings()) {
        currentTtlVersion = alterData->Description.GetTtlSettings().GetVersion();
    }

    if (alter.HasAlterTtlSettings()) {
        const NKikimrSchemeOp::TColumnTableSchema* tableSchema = nullptr;
        if (tableInfo->Description.HasSchema()) {
            tableSchema = &tableInfo->Description.GetSchema();
        } else {
            auto& preset = storeInfo->SchemaPresets.at(tableInfo->Description.GetSchemaPresetId());
            auto& presetProto = storeInfo->Description.GetSchemaPresets(preset.ProtoIndex);
            tableSchema = &presetProto.GetSchema();
        }

        THashSet<TString> knownTiers;
        for (const auto& tier : tableSchema->GetStorageTiers()) {
            knownTiers.insert(tier.GetName());
        }

        THashMap<ui32, TOlapSchema::TColumn> columns;
        THashMap<TString, ui32> columnsByName;
        for (const auto& col : tableSchema->GetColumns()) {
            ui32 id = col.GetId();
            TString name = col.GetName();
            columns[id] = TOlapSchema::TColumn{id, name, static_cast<NScheme::TTypeId>(col.GetTypeId()), Max<ui32>()};
            columnsByName[name] = id;
        }

        if (!ValidateTtlSettings(alter.GetAlterTtlSettings(), columns, columnsByName, knownTiers, errStr)) {
            status = NKikimrScheme::StatusInvalidParameter;
            return nullptr;
        }

        if (!ValidateTtlSettingsChange(tableInfo->Description.GetTtlSettings(), alter.GetAlterTtlSettings(), errStr)) {
            status = NKikimrScheme::StatusInvalidParameter;
            return nullptr;
        }

        *alterData->Description.MutableTtlSettings() = alter.GetAlterTtlSettings();
        alterData->Description.MutableTtlSettings()->SetVersion(currentTtlVersion + 1);
    }
    if (alter.HasRESERVED_AlterTtlSettingsPresetName()) {
        status = NKikimrScheme::StatusInvalidParameter;
        errStr = "TTL presets are not supported";
        return nullptr;
    }

    tableInfo->AlterData = alterData;
    return alterData;
}

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

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState->TxType == TTxState::TxAlterColumnTable);

        TPathId pathId = txState->TargetPathId;
        TPath path = TPath::Init(pathId, context.SS);
        TString pathString = path.PathString();

        TColumnTableInfo::TPtr tableInfo = context.SS->ColumnTables[pathId];
        Y_VERIFY(tableInfo);
        TColumnTableInfo::TPtr alterInfo = tableInfo->AlterData;
        Y_VERIFY(alterInfo);

        auto olapStorePath = path.FindOlapStore();
        Y_VERIFY(olapStorePath, "Unexpected failure to find an olap store");
        auto storeInfo = context.SS->OlapStores.at(olapStorePath->PathId);

        txState->ClearShardsInProgress();

        auto seqNo = context.SS->StartRound(*txState);

        TString columnShardTxBody;
        {
            NKikimrTxColumnShard::TSchemaTxBody tx;
            context.SS->FillSeqNo(tx, seqNo);

            auto* alter = tx.MutableAlterTable();

            alter->SetPathId(pathId.LocalPathId);
            *alter->MutableAlterBody() = *alterInfo->AlterBody;
            if (alterInfo->Description.HasSchema()) {
                *alter->MutableSchema() = alterInfo->Description.GetSchema();
            }
            if (alterInfo->Description.HasSchemaPresetId()) {
                const ui32 presetId = alterInfo->Description.GetSchemaPresetId();
                Y_VERIFY(storeInfo->SchemaPresets.contains(presetId),
                    "Failed to find schema preset %" PRIu32 " in an olap store", presetId);
                auto& preset = storeInfo->SchemaPresets.at(presetId);
                size_t presetIndex = preset.ProtoIndex;
                *alter->MutableSchemaPreset() = storeInfo->Description.GetSchemaPresets(presetIndex);
            }
            if (alterInfo->Description.HasTtlSettings()) {
                *alter->MutableTtlSettings() = alterInfo->Description.GetTtlSettings();
            }
            if (alterInfo->Description.HasSchemaPresetVersionAdj()) {
                alter->SetSchemaPresetVersionAdj(alterInfo->Description.GetSchemaPresetVersionAdj());
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
                    context.SS->SelectProcessingPrarams(txState->TargetPathId));

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

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState->TxType == TTxState::TxAlterColumnTable);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        TColumnTableInfo::TPtr tableInfo = context.SS->ColumnTables[pathId];
        Y_VERIFY(tableInfo);
        TColumnTableInfo::TPtr alterInfo = tableInfo->AlterData;
        Y_VERIFY(alterInfo);
        alterInfo->AlterBody.Clear();
        context.SS->ColumnTables[pathId] = alterInfo;

        context.SS->PersistColumnTableAlterRemove(db, pathId);
        context.SS->PersistColumnTable(db, pathId, *alterInfo);

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

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxAlterColumnTable);

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
        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxAlterColumnTable);

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

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxAlterColumnTable);

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
private:
    const TOperationId OperationId;
    const TTxTransaction Transaction;
    TTxState::ETxState State = TTxState::Invalid;

    TTxState::ETxState NextState() {
        return TTxState::ConfigureParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) {
        switch(state) {
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::ProposedWaitParts;
        case TTxState::ProposedWaitParts:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
        return TTxState::Invalid;
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) {
        switch(state) {
        case TTxState::ConfigureParts:
            return THolder(new TConfigureParts(OperationId));
        case TTxState::Propose:
            return THolder(new TPropose(OperationId));
        case TTxState::ProposedWaitParts:
            return THolder(new TProposedWaitParts(OperationId));
        case TTxState::Done:
            return THolder(new TDone(OperationId));
        default:
            return nullptr;
        }
    }

    void StateDone(TOperationContext& context) override {
        State = NextState(State);

        if (State != TTxState::Invalid) {
            SetState(SelectStateFunc(State));
            context.OnComplete.ActivateTx(OperationId);
        }
    }

public:
    TAlterColumnTable(TOperationId id, const TTxTransaction& tx)
        : OperationId(id)
        , Transaction(tx)
    {
    }

    TAlterColumnTable(TOperationId id, TTxState::ETxState state)
        : OperationId(id)
        , State(state)
    {
        SetState(SelectStateFunc(state));
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& alter = Transaction.GetAlterColumnTable();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = alter.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterColumnTable Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        if (!alter.HasName()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "No table name in Alter");
            return result;
        }

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
                TString explain = TStringBuilder() << "path fail checks"
                                                   << ", path: " << path.PathString();
                auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
                return result;
            }
        }

        Y_VERIFY(context.SS->ColumnTables.contains(path.Base()->PathId));
        TColumnTableInfo::TPtr tableInfo = context.SS->ColumnTables.at(path.Base()->PathId);

        TPath storePath = TPath::Init(tableInfo->OlapStorePathId, context.SS);
        {
            TPath::TChecker checks = storePath.Check();
            checks
                .NotEmpty()
                .IsResolved()
                .IsOlapStore()
                .NotUnderOperation();

            if (!checks) {
                TString explain = TStringBuilder() << "store path fail checks"
                                                   << ", path: " << storePath.PathString();
                auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
                return result;
            }
        }

        Y_VERIFY(context.SS->OlapStores.contains(tableInfo->OlapStorePathId));
        TOlapStoreInfo::TPtr storeInfo = context.SS->OlapStores.at(tableInfo->OlapStorePathId);

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (tableInfo->AlterVersion == 0) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "Table is not created yet");
            return result;
        }
        if (tableInfo->AlterData) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "There's another Alter in flight");
            return result;
        }

        NKikimrScheme::EStatus status;
        TColumnTableInfo::TPtr alterData = ParseParams(path, tableInfo, storeInfo, alter, *path.DomainInfo(), status, errStr, context);
        if (!alterData) {
            result->SetError(status, errStr);
            return result;
        }
        if (!context.SS->CheckInFlightLimit(TTxState::TxAlterColumnTable, errStr)) {
            result->SetError(NKikimrScheme::StatusResourceExhausted, errStr);
            return result;
        }

        Y_VERIFY(storeInfo->ColumnTables.contains(path->PathId));
        storeInfo->ColumnTablesUnderOperation.insert(path->PathId);

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

        // Sequentially chain operations in the same olap store
        if (context.SS->Operations.contains(storePath.Base()->LastTxId)) {
            context.OnComplete.Dependence(storePath.Base()->LastTxId, OperationId.GetTxId());
        }
        storePath.Base()->LastTxId = OperationId.GetTxId();
        context.SS->PersistLastTxId(db, storePath.Base());

        context.SS->PersistColumnTableAlter(db, path->PathId, *alterData);
        context.SS->PersistTxState(db, OperationId);

        context.OnComplete.ActivateTx(OperationId);

        State = NextState();
        SetState(SelectStateFunc(State));
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

namespace NKikimr {
namespace NSchemeShard {

ISubOperationBase::TPtr CreateAlterColumnTable(TOperationId id, const TTxTransaction& tx) {
    return new TAlterColumnTable(id, tx);
}

ISubOperationBase::TPtr CreateAlterColumnTable(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return new TAlterColumnTable(id, state);
}

}
}
