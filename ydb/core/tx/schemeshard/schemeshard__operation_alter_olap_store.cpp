#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

TOlapStoreInfo::TPtr ParseParams(
        const TPath& path, const TOlapStoreInfo::TPtr& storeInfo,
        const NKikimrSchemeOp::TAlterColumnStore& alter,
        NKikimrScheme::EStatus& status, TString& errStr, TOperationContext& context)
{
    Y_UNUSED(path);
    Y_UNUSED(context);

    // Make a copy of the current store and increment its version
    TOlapStoreInfo::TPtr alterData = new TOlapStoreInfo(*storeInfo);
    alterData->AlterVersion++;
    alterData->AlterBody.ConstructInPlace(alter);

    for (const auto& removeSchemaPreset : alter.GetRemoveSchemaPresets()) {
        Y_UNUSED(removeSchemaPreset);
        status = NKikimrScheme::StatusInvalidParameter;
        errStr = "Removing schema presets is not supported yet";
        return nullptr;
    }

    THashSet<TString> alteredSchemaPresets;
    for (const auto& alterSchemaPreset : alter.GetAlterSchemaPresets()) {
#if 0
        const TString& presetName = alterSchemaPreset.GetName();
        if (alteredSchemaPresets.contains(presetName)) {
            status = NKikimrScheme::StatusInvalidParameter;
            errStr = TStringBuilder() << "Cannot alter schema preset '" << presetName << "' multiple times";
            return nullptr;
        }
        if (!alterData->SchemaPresetByName.contains(presetName)) {
            status = NKikimrScheme::StatusInvalidParameter;
            errStr = TStringBuilder() << "Cannot alter unknown schema preset '" << presetName << "'";
            return nullptr;
        }

        const ui32 presetId = alterData->SchemaPresetByName.at(presetName);
        auto& preset = alterData->SchemaPresets.at(presetId);
        auto& presetProto = *alterData->Description.MutableSchemaPresets(preset.GetProtoIndex());
        auto& schemaProto = *presetProto.MutableSchema();

        const auto& alterSchema = alterSchemaPreset.GetAlterSchema();

        THashSet<ui32> droppedColumns;
        for (const auto& dropColumn : alterSchema.GetDropColumns()) {
            const TString& columnName = dropColumn.GetName();
            const auto* column = preset.GetColumnByName(columnName);
            if (!column) {
                status = NKikimrScheme::StatusInvalidParameter;
                errStr = TStringBuilder() << "Cannot drop non-existant column '" << columnName << "'";
                return nullptr;
            }
            const ui32 columnId = column->Id;
            if (column->IsKeyColumn()) {
                status = NKikimrScheme::StatusInvalidParameter;
                errStr = TStringBuilder() << "Cannot drop key column '" << columnName << "'";
                return nullptr;
            }
            preset.ColumnsByName.erase(columnName);
            preset.Columns.erase(columnId);
            droppedColumns.insert(columnId);
        }
        if (droppedColumns) {
            auto* columns = schemaProto.MutableColumns();
            for (int src = 0, dst = 0; src < columns->size(); ++src) {
                const auto& srcProto = columns->Get(src);
                const ui32 columnId = srcProto.GetId();
                if (droppedColumns.contains(columnId)) {
                    // skip dropped columns
                    continue;
                }
                if (src != dst) {
                    // auto& column = preset.Columns.at(columnId);
                    // column.ProtoIndex = dst;
                    columns->SwapElements(src, dst);
                }
                ++dst;
            }
        }

        for (const auto& alterColumn : alterSchema.GetAlterColumns()) {
            Y_UNUSED(alterColumn);
            status = NKikimrScheme::StatusInvalidParameter;
            errStr = "Altering existing columns is not supported yet";
            return nullptr;
        }

        const NScheme::TTypeRegistry* typeRegistry = AppData()->TypeRegistry;

        for (const auto& addColumn : alterSchema.GetAddColumns()) {
            auto& columnProto = *schemaProto.AddColumns();
            columnProto = addColumn;
            const TString& columnName = columnProto.GetName();
            if (columnName.empty()) {
                status = NKikimrScheme::StatusInvalidParameter;
                errStr = "Columns cannot have an empty name";
                return nullptr;
            }
            if (preset.ColumnsByName.contains(columnName)) {
                status = NKikimrScheme::StatusInvalidParameter;
                errStr = TStringBuilder() << "Cannot add duplicate column '" << columnName << "'";
                return nullptr;
            }
            if (columnProto.HasId()) {
                status = NKikimrScheme::StatusInvalidParameter;
                errStr = TStringBuilder() << "New column '" << columnName << "' cannot have an Id specified";
                return nullptr;
            }
            if (columnProto.HasTypeId()) {
                status = NKikimrScheme::StatusSchemeError;
                errStr = TStringBuilder() << "Cannot set TypeId for column '" << columnName << "', use Type";
                return nullptr;
            }
            if (!columnProto.HasType()) {
                status = NKikimrScheme::StatusSchemeError;
                errStr = TStringBuilder() << "Missing Type for column '" << columnName << "'";
                return nullptr;
            }

            const ui32 columnId = preset.NextColumnId++;
            Y_VERIFY(!preset.Columns.contains(columnId));
            columnProto.SetId(columnId);
            auto& column = preset.Columns[columnId];
            column.Id = columnId;
            column.Name = columnName;
            preset.ColumnsByName[columnName] = columnId;

            auto typeName = NMiniKQL::AdaptLegacyYqlType(columnProto.GetType());
            const NScheme::IType* type = typeRegistry->GetType(typeName);
            if (!type || !NScheme::NTypeIds::IsYqlType(type->GetTypeId())) {
                status = NKikimrScheme::StatusSchemeError;
                errStr = TStringBuilder() << "Type '" << columnProto.GetType() << "' specified for column '" << columnName << "' is not supported";
                return nullptr;
            }
            columnProto.SetTypeId(type->GetTypeId());
            column.TypeId = type->GetTypeId();
            schemaProto.SetNextColumnId(preset.NextColumnId);
        }

        for (const auto& addKeyColumnName : alterSchema.GetAddKeyColumnNames()) {
            Y_UNUSED(addKeyColumnName);
            status = NKikimrScheme::StatusInvalidParameter;
            errStr = "Adding key columns is not supported yet";
            return nullptr;
        }

        preset.Version++;
        schemaProto.SetVersion(preset.Version);

        alteredSchemaPresets.insert(presetName);
#else
        Y_UNUSED(alterSchemaPreset);
        Y_UNUSED(alteredSchemaPresets);
        status = NKikimrScheme::StatusInvalidParameter;
        errStr = "Altering schema presets is not supported yet";
        return nullptr;
#endif
    }

    for (const auto& addSchemaPreset : alter.GetAddSchemaPresets()) {
        Y_UNUSED(addSchemaPreset);
        status = NKikimrScheme::StatusInvalidParameter;
        errStr = "Adding schema presets is not supported yet";
        return nullptr;
    }

    for (const auto& removeTtlSettingsPreset : alter.GetRESERVED_RemoveTtlSettingsPresets()) {
        Y_UNUSED(removeTtlSettingsPreset);
        status = NKikimrScheme::StatusInvalidParameter;
        errStr = "TTL presets are not supported";
        return nullptr;
    }

    for (const auto& alterTtlSettingsPreset : alter.GetRESERVED_AlterTtlSettingsPresets()) {
        Y_UNUSED(alterTtlSettingsPreset);
        status = NKikimrScheme::StatusInvalidParameter;
        errStr = "TTL presets are not supported";
        return nullptr;
    }

    for (const auto& addTtlSettingsPreset : alter.GetRESERVED_AddTtlSettingsPresets()) {
        Y_UNUSED(addTtlSettingsPreset);
        status = NKikimrScheme::StatusInvalidParameter;
        errStr = "TTL presets are not supported";
        return nullptr;
    }

    storeInfo->AlterData = alterData;
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
        Y_VERIFY(storeInfo);
        TOlapStoreInfo::TPtr alterData = storeInfo->AlterData;
        Y_VERIFY(alterData);

        txState->ClearShardsInProgress();

        TVector<ui32> droppedSchemaPresets;
        for (const auto& presetProto : storeInfo->Description.GetSchemaPresets()) {
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
        {
            auto seqNo = context.SS->StartRound(*txState);
            NKikimrTxColumnShard::TSchemaTxBody tx;
            context.SS->FillSeqNo(tx, seqNo);

            auto* alter = tx.MutableAlterStore();
            alter->SetStorePathId(txState->TargetPathId.LocalPathId);

            for (ui32 id : droppedSchemaPresets) {
                alter->AddDroppedSchemaPresets(id);
            }
            for (const auto& presetProto : storeInfo->Description.GetSchemaPresets()) {
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
        Y_VERIFY(txState->TxType == TTxState::TxAlterOlapStore);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        TOlapStoreInfo::TPtr storeInfo = context.SS->OlapStores[pathId];
        Y_VERIFY(storeInfo);
        TOlapStoreInfo::TPtr alterData = storeInfo->AlterData;
        Y_VERIFY(alterData);

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
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxAlterOlapStore);

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
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxAlterOlapStore);

        auto shardId = TTabletId(ev->Get()->Record.GetOrigin());
        auto shardIdx = context.SS->MustGetShardIdx(shardId);
        Y_VERIFY(context.SS->ShardInfos.contains(shardIdx));

        txState->ShardsInProgress.erase(shardIdx);
        return MaybeFinish(context);
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " ProgressState"
                     << " at tablet: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxAlterOlapStore);

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
                        Y_FAIL("unexpected tablet type");
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
            Y_VERIFY(storeInfo);

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
        Y_VERIFY(txState->TxType == TTxState::TxAlterOlapStore);

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

        Y_VERIFY(context.SS->OlapStores.contains(path->PathId));
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

        if (storeInfo->AlterVersion == 0) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "Store is not created yet");
            return result;
        }
        if (storeInfo->AlterData) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "There's another Alter in flight");
            return result;
        }

        NKikimrScheme::EStatus status;
        TOlapStoreInfo::TPtr alterData = ParseParams(path, storeInfo, alter, status, errStr, context);
        if (!alterData) {
            result->SetError(status, errStr);
            return result;
        }

        NIceDb::TNiceDb db(context.GetDB());

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterOlapStore, path->PathId);
        txState.State = TTxState::ConfigureParts;

        for (auto shardIdx : storeInfo->ColumnShards) {
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
        Y_FAIL("no AbortPropose for TAlterOlapStore");
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
    Y_VERIFY(state != TTxState::Invalid);
    return MakeSubOperation<TAlterOlapStore>(id, state);
}

}
