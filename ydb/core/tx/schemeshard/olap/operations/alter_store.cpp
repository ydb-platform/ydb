#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

TOlapStoreInfo::TPtr ParseParams(const TOlapStoreInfo::TPtr& storeInfo,
        const NKikimrSchemeOp::TAlterColumnStore& alter,
        IErrorCollector& errors)
{
    if (!alter.GetRemoveSchemaPresets().empty()) {
        errors.AddError(NKikimrScheme::StatusInvalidParameter, "Removing schema presets is not supported yet");
        return nullptr;
    }

    for (const auto& addSchemaPreset : alter.GetAddSchemaPresets()) {
        Y_UNUSED(addSchemaPreset);
        errors.AddError(NKikimrScheme::StatusInvalidParameter, "Adding schema presets is not supported yet");
        return nullptr;
    }

    for (const auto& removeTtlSettingsPreset : alter.GetRESERVED_RemoveTtlSettingsPresets()) {
        Y_UNUSED(removeTtlSettingsPreset);
        errors.AddError(NKikimrScheme::StatusInvalidParameter, "TTL presets are not supported");
        return nullptr;
    }

    for (const auto& alterTtlSettingsPreset : alter.GetRESERVED_AlterTtlSettingsPresets()) {
        Y_UNUSED(alterTtlSettingsPreset);
        errors.AddError(NKikimrScheme::StatusInvalidParameter, "TTL presets are not supported");
        return nullptr;
    }

    for (const auto& addTtlSettingsPreset : alter.GetRESERVED_AddTtlSettingsPresets()) {
        Y_UNUSED(addTtlSettingsPreset);
        errors.AddError(NKikimrScheme::StatusInvalidParameter, "TTL presets are not supported");
        return nullptr;
    }

    auto alterData = TOlapStoreInfo::BuildStoreWithAlter(*storeInfo, alter);
    THashSet<TString> alteredSchemaPresets;
    for (const auto& alterProto : alter.GetAlterSchemaPresets()) {
        const TString& presetName = alterProto.GetName();
        if (alteredSchemaPresets.contains(presetName)) {
            errors.AddError(NKikimrScheme::StatusInvalidParameter, TStringBuilder() << "Cannot alter schema preset '" << presetName << "' multiple times");
            return nullptr;
        }

        if (!storeInfo->SchemaPresetByName.contains(presetName)) {
            errors.AddError(NKikimrScheme::StatusInvalidParameter, TStringBuilder() << "Cannot alter unknown schema preset '" << presetName << "'");
        }

        TOlapSchemaUpdate schemaUpdate;
        if (!schemaUpdate.Parse(alterProto.GetAlterSchema(), errors)) {
            return nullptr;
        }
        if (!alterData->UpdatePreset(presetName, schemaUpdate, errors)) {
            return nullptr;
        }
    }
    return alterData;
}

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TAlterOlapStore TConfigureParts"
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

        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterOlapStore);
        TOlapStoreInfo::TPtr storeInfo = context.SS->OlapStores[txState->TargetPathId];
        Y_ABORT_UNLESS(storeInfo);
        TOlapStoreInfo::TPtr alterData = storeInfo->AlterData;
        Y_ABORT_UNLESS(alterData);

        txState->ClearShardsInProgress();

        TVector<ui32> droppedSchemaPresets;
        for (const auto& presetProto : storeInfo->GetDescription().GetSchemaPresets()) {
            const ui32 presetId = presetProto.GetId();
            if (!alterData->SchemaPresets.contains(presetId)) {
                droppedSchemaPresets.push_back(presetId);
            }
        }
        THashSet<ui32> updatedSchemaPresets;
        for (const auto& proto : alterData->AlterBody->GetAlterSchemaPresets()) {
            const TString& presetName = proto.GetName();
            const ui32 presetId = alterData->SchemaPresetByName.at(presetName);
            updatedSchemaPresets.insert(presetId);
        }
        for (const auto& proto : alterData->AlterBody->GetAddSchemaPresets()) {
            const TString& presetName = proto.GetName();
            const ui32 presetId = alterData->SchemaPresetByName.at(presetName);
            updatedSchemaPresets.insert(presetId);
        }

        TString columnShardTxBody;
        const auto seqNo = context.SS->StartRound(*txState);
        {
            NKikimrTxColumnShard::TSchemaTxBody tx;
            context.SS->FillSeqNo(tx, seqNo);

            auto* alter = tx.MutableAlterStore();
            alter->SetStorePathId(txState->TargetPathId.LocalPathId);

            for (ui32 id : droppedSchemaPresets) {
                alter->AddDroppedSchemaPresets(id);
            }
            for (const auto& presetProto : alterData->GetDescription().GetSchemaPresets()) {
                if (updatedSchemaPresets.contains(presetProto.GetId())) {
                    *alter->AddSchemaPresets() = presetProto;
                }
            }

            Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&columnShardTxBody);
        }

        for (auto& shard : txState->Shards) {
            TTabletId tabletId = context.SS->ShardInfos[shard.Idx].TabletID;

            if (shard.TabletType == ETabletType::ColumnShard) {
                auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
                    NKikimrTxColumnShard::TX_KIND_SCHEMA,
                    context.SS->TabletID(),
                    context.Ctx.SelfID,
                    ui64(OperationId.GetTxId()),
                    columnShardTxBody, seqNo,
                    context.SS->SelectProcessingParams(txState->TargetPathId));

                context.OnComplete.BindMsgToPipe(OperationId, tabletId, shard.Idx, event.release());
            } else {
                Y_ABORT("unexpected tablet type");
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
                << "TAlterOlapStore TPropose"
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterOlapStore);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        TOlapStoreInfo::TPtr storeInfo = context.SS->OlapStores[pathId];
        Y_ABORT_UNLESS(storeInfo);
        TOlapStoreInfo::TPtr alterData = storeInfo->AlterData;
        Y_ABORT_UNLESS(alterData);

        NIceDb::TNiceDb db(context.GetDB());

        // TODO: make a new FinishPropose method or something like that
        alterData->AlterBody.Clear();
        alterData->ColumnTables = storeInfo->ColumnTables;
        alterData->ColumnTablesUnderOperation = storeInfo->ColumnTablesUnderOperation;
        context.SS->OlapStores[pathId] = alterData;

        context.SS->PersistOlapStoreAlterRemove(db, pathId);
        context.SS->PersistOlapStore(db, pathId, *alterData);

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
                     DebugHint() << " ProgressState"
                     << " at tablet: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterOlapStore);

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
    static constexpr ui32 UpdateBatchSize = 100;

private:
    TOperationId OperationId;
    bool MessagesSent = false;
    TDeque<TPathId> TablesToUpdate;
    bool TablesInitialized = false;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TAlterOlapStore TProposedWaitParts"
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
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterOlapStore);

        auto shardId = TTabletId(ev->Get()->Record.GetOrigin());
        auto shardIdx = context.SS->MustGetShardIdx(shardId);
        Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));

        txState->ShardsInProgress.erase(shardIdx);
        return MaybeFinish(context);
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " ProgressState"
                     << " at tablet: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterOlapStore);

        if (!MessagesSent) {
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
                        Y_ABORT("unexpected tablet type");
                    }
                }

                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            DebugHint() << " ProgressState"
                                        << " wait for NotifyTxCompletionResult"
                                        << " tabletId: " << tabletId);
            }

            MessagesSent = true;
        }

        if (!TablesInitialized) {
            TPathId pathId = txState->TargetPathId;

            TOlapStoreInfo::TPtr storeInfo = context.SS->OlapStores[pathId];
            Y_ABORT_UNLESS(storeInfo);

            for (TPathId tablePathId : storeInfo->ColumnTables) {
                TablesToUpdate.emplace_back(tablePathId);
            }

            TablesInitialized = true;
        }

        return UpdateTables(context);
    }

    bool UpdateTables(TOperationContext& context) {
        NIceDb::TNiceDb db(context.GetDB());

        ui32 updated = 0;
        while (!TablesToUpdate.empty() && updated < UpdateBatchSize) {
            auto pathId = TablesToUpdate.front();
            TablesToUpdate.pop_front();

            TPathElement::TPtr path = context.SS->PathsById.at(pathId);
            if (path->Dropped()) {
                continue; // ignore tables that are dropped
            }

            if (!context.SS->ColumnTables.contains(pathId)) {
                continue; // ignore tables that don't exist
            }
            auto tableInfo = context.SS->ColumnTables.at(pathId);
            if (tableInfo->AlterData) {
                continue; // ignore tables that have some alter
            }

            // TODO: don't republish tables that are not updated

            context.SS->ClearDescribePathCaches(path);
            context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
            ++updated;
        }

        if (!TablesToUpdate.empty()) {
            // Wait for a new ProgressState call
            context.OnComplete.ActivateTx(OperationId);
            return false;
        }

        return MaybeFinish(context);
    }

    bool MaybeFinish(TOperationContext& context) {
        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterOlapStore);

        if (txState->ShardsInProgress.empty() && TablesToUpdate.empty()) {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::Done);
            return true;
        }

        return false;
    }
};

class TAlterOlapStore: public TSubOperation {
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

        const auto& alter = Transaction.GetAlterColumnStore();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = alter.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterOlapStore Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        if (!alter.HasName()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "No store name in Alter");
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
                .IsOlapStore()
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        Y_ABORT_UNLESS(context.SS->OlapStores.contains(path->PathId));
        TOlapStoreInfo::TPtr storeInfo = context.SS->OlapStores.at(path->PathId);

        if (!storeInfo->ColumnTablesUnderOperation.empty()) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "Store has unfinished table operations");
            return result;
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (storeInfo->GetAlterVersion() == 0) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "Store is not created yet");
            return result;
        }
        if (storeInfo->AlterData) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "There's another Alter in flight");
            return result;
        }

        TProposeErrorCollector errors(*result);
        TOlapStoreInfo::TPtr alterData = ParseParams(storeInfo, alter, errors);
        if (!alterData) {
            return result;
        }
        storeInfo->AlterData = alterData;

        NIceDb::TNiceDb db(context.GetDB());

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterOlapStore, path->PathId);
        txState.State = TTxState::ConfigureParts;

        for (auto shardIdx : storeInfo->GetColumnShards()) {
            Y_VERIFY_S(context.SS->ShardInfos.contains(shardIdx), "Unknown shardIdx " << shardIdx);
            txState.Shards.emplace_back(shardIdx, context.SS->ShardInfos[shardIdx].TabletType, TTxState::ConfigureParts);

            context.SS->ShardInfos[shardIdx].CurrentTxId = OperationId.GetTxId();
            context.SS->PersistShardTx(db, shardIdx, OperationId.GetTxId());
        }

        path->LastTxId = OperationId.GetTxId();
        path->PathState = TPathElement::EPathState::EPathStateAlter;
        context.SS->PersistLastTxId(db, path.Base());

        context.SS->PersistOlapStoreAlter(db, path->PathId, *alterData);
        context.SS->PersistTxState(db, OperationId);

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TAlterOlapStore");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterOlapStore AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterOlapStore(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterOlapStore>(id, tx);
}

ISubOperation::TPtr CreateAlterOlapStore(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAlterOlapStore>(id, state);
}

}
