#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/mind/hive/hive.h>

#include <util/random/shuffle.h>

namespace NKikimr {
namespace NSchemeShard {

namespace {

TColumnTableInfo::TPtr CreateColumnTable(
        const NKikimrSchemeOp::TColumnTableDescription& opSrc,
        TOlapStoreInfo::TPtr storeInfo, const TSubDomainInfo& subDomain,
        TEvSchemeShard::EStatus& status, TString& errStr,
        TSchemeShard* ss)
{
    Y_UNUSED(subDomain);

    TColumnTableInfo::TPtr tableInfo = new TColumnTableInfo;
    tableInfo->AlterVersion = 1;
    tableInfo->Description.CopyFrom(opSrc);

    auto& op = tableInfo->Description;

    if (op.HasRESERVED_TtlSettingsPresetName() || op.HasRESERVED_TtlSettingsPresetId()) {
        status = NKikimrScheme::StatusSchemeError;
        errStr = "TTL presets are not supported";
        return nullptr;
    }

    if (!op.HasSchemaPresetName() && !op.HasSchemaPresetId()) {
        op.SetSchemaPresetName("default");
    }

    const TOlapSchema* pSchema = nullptr;

    if (op.HasSchemaPresetName()) {
        const TString presetName = op.GetSchemaPresetName();
        if (!storeInfo->SchemaPresetByName.contains(presetName)) {
            status = NKikimrScheme::StatusSchemeError;
            errStr = Sprintf("Specified schema preset '%s' does not exist in olap store", presetName.c_str());
            return nullptr;
        }
        const ui32 presetId = storeInfo->SchemaPresetByName.at(presetName);
        if (!op.HasSchemaPresetId()) {
            op.SetSchemaPresetId(presetId);
        }
        if (op.GetSchemaPresetId() != presetId) {
            status = NKikimrScheme::StatusSchemeError;
            errStr = Sprintf("Specified schema preset '%s' and id %" PRIu32 " do not match in olap store", presetName.c_str(), presetId);
            return nullptr;
        }
        pSchema = &storeInfo->SchemaPresets.at(presetId);
    } else if (op.HasSchemaPresetId()) {
        const ui32 presetId = op.GetSchemaPresetId();
        if (!storeInfo->SchemaPresets.contains(presetId)) {
            status = NKikimrScheme::StatusSchemeError;
            errStr = Sprintf("Specified schema preset %" PRIu32 " does not exist in olap store", presetId);
            return nullptr;
        }
        const TString& presetName = storeInfo->SchemaPresets.at(presetId).Name;
        op.SetSchemaPresetName(presetName);
        pSchema = &storeInfo->SchemaPresets.at(presetId);
    }

    Y_VERIFY(pSchema, "Expected to find a preset schema");

    if (op.HasSchema()) {
        auto& opSchema = op.GetSchema();
        const NScheme::TTypeRegistry* typeRegistry = AppData()->TypeRegistry;

        ui32 lastColumnId = 0;
        THashSet<ui32> usedColumns;
        for (const auto& colProto : opSchema.GetColumns()) {
            if (colProto.GetName().empty()) {
                status = NKikimrScheme::StatusSchemeError;
                errStr = "Columns cannot have an empty name";
                return nullptr;
            }
            const TString& colName = colProto.GetName();
            auto* col = pSchema->FindColumnByName(colName);
            if (!col) {
                status = NKikimrScheme::StatusSchemeError;
                errStr = TStringBuilder()
                    << "Column '" << colName << "' does not match schema preset";
                return nullptr;
            }
            if (colProto.HasId() && colProto.GetId() != col->Id) {
                status = NKikimrScheme::StatusSchemeError;
                errStr = TStringBuilder()
                    << "Column '" << colName << "' has id " << colProto.GetId() << " that does not match schema preset";
                return nullptr;
            }

            if (!usedColumns.insert(col->Id).second) {
                status = NKikimrScheme::StatusSchemeError;
                errStr = TStringBuilder() << "Column '" << colName << "' is specified multiple times";
                return nullptr;
            }
            if (col->Id < lastColumnId) {
                status = NKikimrScheme::StatusSchemeError;
                errStr = "Column order does not match schema preset";
                return nullptr;
            }
            lastColumnId = col->Id;

            if (colProto.HasTypeId()) {
                status = NKikimrScheme::StatusSchemeError;
                errStr = TStringBuilder() << "Cannot set TypeId for column '" << colName << "', use Type";
                return nullptr;
            }
            if (!colProto.HasType()) {
                status = NKikimrScheme::StatusSchemeError;
                errStr = TStringBuilder() << "Missing Type for column '" << colName << "'";
                return nullptr;
            }

            auto typeName = NMiniKQL::AdaptLegacyYqlType(colProto.GetType());
            const NScheme::IType* type = typeRegistry->GetType(typeName);
            NScheme::TTypeInfo typeInfo;
            if (!type || !NScheme::NTypeIds::IsYqlType(type->GetTypeId())) {
                auto* typeDesc = NPg::TypeDescFromPgTypeName(typeName);
                if (!typeDesc) {
                    status = NKikimrScheme::StatusSchemeError;
                    errStr = TStringBuilder()
                        << "Type '" << colProto.GetType() << "' specified for column '" << colName << "' is not supported";
                    return nullptr;
                }
                typeInfo = NScheme::TTypeInfo(NScheme::NTypeIds::Pg, typeDesc);
            } else {
                typeInfo = NScheme::TTypeInfo(type->GetTypeId());
            }

            if (typeInfo != col->Type) {
                status = NKikimrScheme::StatusSchemeError;
                errStr = TStringBuilder()
                    << "Type '" << colProto.GetType() << "' specified for column '" << colName
                    << "' does not match schema preset";
                return nullptr;
            }
        }

        for (auto& pr : pSchema->Columns) {
            if (!usedColumns.contains(pr.second.Id)) {
                status = NKikimrScheme::StatusSchemeError;
                errStr = "Specified schema is missing some schema preset columns";
                return nullptr;
            }
        }

        TVector<ui32> keyColumnIds;
        for (const TString& keyName : opSchema.GetKeyColumnNames()) {
            auto* col = pSchema->FindColumnByName(keyName);
            if (!col) {
                status = NKikimrScheme::StatusSchemeError;
                errStr = TStringBuilder() << "Unknown key column '" << keyName << "'";
                return nullptr;
            }
            keyColumnIds.push_back(col->Id);
        }
        if (keyColumnIds != pSchema->KeyColumnIds) {
            status = NKikimrScheme::StatusSchemeError;
            errStr = "Specified schema key columns not matching schema preset";
            return nullptr;
        }

        if (opSchema.GetEngine() != pSchema->Engine) {
            status = NKikimrScheme::StatusSchemeError;
            errStr = "Specified schema engine does not match schema preset";
            return nullptr;
        }

        op.ClearSchema();
    }

    if (op.HasRESERVED_TtlSettingsPresetName() || op.HasRESERVED_TtlSettingsPresetId()) {
        status = NKikimrScheme::StatusSchemeError;
        errStr = "TTL presets are not supported";
        return nullptr;
    }

    if (op.HasTtlSettings()) {
        op.MutableTtlSettings()->SetVersion(1);
    }

    // Validate ttl settings and schema compatibility
    if (op.HasTtlSettings()) {
        if (!ValidateTtlSettings(op.GetTtlSettings(), pSchema->Columns, pSchema->ColumnsByName, errStr)) {
            status = NKikimrScheme::StatusInvalidParameter;
            return nullptr;
        }
    }

    if (op.HasSharding()) {
        tableInfo->Sharding = std::move(*op.MutableSharding());
        op.ClearSharding();
    } else {
        // Use default random sharding
        tableInfo->Sharding.MutableRandomSharding();
    }

    switch (tableInfo->Sharding.Method_case()) {
        case NKikimrSchemeOp::TColumnTableSharding::kRandomSharding: {
            // Random sharding implies non-unique primary key
            tableInfo->Sharding.SetUniquePrimaryKey(false);
            break;
        }
        case NKikimrSchemeOp::TColumnTableSharding::kHashSharding: {
            auto& sharding = *tableInfo->Sharding.MutableHashSharding();
            if (sharding.ColumnsSize() == 0) {
                status = NKikimrScheme::StatusSchemeError;
                errStr = Sprintf("Hash sharding requires a non-empty list of columns");
                return nullptr;
            }
            Y_VERIFY(pSchema);
            bool keysOnly = true;
            for (const TString& columnName : sharding.GetColumns()) {
                auto* pColumn = pSchema->FindColumnByName(columnName);
                if (!pColumn) {
                    status = NKikimrScheme::StatusSchemeError;
                    errStr = Sprintf("Hash sharding is using an unknown column '%s'", columnName.c_str());
                    return nullptr;
                }
                if (!pColumn->IsKeyColumn()) {
                    keysOnly = false;
                }
            }
            sharding.SetUniqueShardKey(true);
            tableInfo->Sharding.SetUniquePrimaryKey(keysOnly);
            break;
        }
        default: {
            status = NKikimrScheme::StatusSchemeError;
            errStr = "Unsupported sharding method";
            return nullptr;
        }
    }

    const ui32 columnShardCount = Max(ui32(1), op.GetColumnShardCount());
    if (columnShardCount > storeInfo->ColumnShards.size()) {
        status = NKikimrScheme::StatusSchemeError;
        errStr = Sprintf("Cannot create table with %" PRIu32 " column shards, only %" PRIu32 " are available",
            columnShardCount, ui32(storeInfo->ColumnShards.size()));
        return nullptr;
    }

    tableInfo->ColumnShards.reserve(storeInfo->ColumnShards.size());
    for (const auto& shardIdx : storeInfo->ColumnShards) {
        auto* shardInfo = ss->ShardInfos.FindPtr(shardIdx);
        Y_VERIFY(shardInfo, "ColumnShard not found");
        tableInfo->ColumnShards.push_back(shardInfo->TabletID.GetValue());
    }
    ShuffleRange(tableInfo->ColumnShards);
    tableInfo->ColumnShards.resize(columnShardCount);

    tableInfo->Sharding.SetVersion(1);

    tableInfo->Sharding.MutableColumnShards()->Clear();
    tableInfo->Sharding.MutableColumnShards()->Reserve(tableInfo->ColumnShards.size());
    for (ui64 columnShard : tableInfo->ColumnShards) {
        tableInfo->Sharding.AddColumnShards(columnShard);
    }

    tableInfo->Sharding.ClearAdditionalColumnShards();

    // Don't allow users to set these fields
    op.ClearSchemaPresetVersionAdj();
    op.ClearTtlSettingsPresetVersionAdj();

    return tableInfo;
}

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCreateColumnTable TConfigureParts"
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
        Y_VERIFY(txState->TxType == TTxState::TxCreateColumnTable);

        TPathId pathId = txState->TargetPathId;
        TPath path = TPath::Init(pathId, context.SS);
        TString pathString = path.PathString();

        TColumnTableInfo::TPtr pendingInfo = context.SS->ColumnTables[pathId];
        Y_VERIFY(pendingInfo);
        Y_VERIFY(pendingInfo->AlterData);
        TColumnTableInfo::TPtr tableInfo = pendingInfo->AlterData;

        auto olapStorePath = path.FindOlapStore();
        Y_VERIFY(olapStorePath, "Unexpected failure to find an olap store");
        auto storeInfo = context.SS->OlapStores.at(olapStorePath->PathId);

        txState->ClearShardsInProgress();

        auto seqNo = context.SS->StartRound(*txState);

        TString columnShardTxBody;
        {
            NKikimrTxColumnShard::TSchemaTxBody tx;
            context.SS->FillSeqNo(tx, seqNo);

            auto* create = tx.MutableEnsureTables()->AddTables();

            create->SetPathId(pathId.LocalPathId);
            if (tableInfo->Description.HasSchema()) {
                create->MutableSchema()->CopyFrom(tableInfo->Description.GetSchema());
            }
            if (tableInfo->Description.HasSchemaPresetId()) {
                const ui32 presetId = tableInfo->Description.GetSchemaPresetId();
                Y_VERIFY(storeInfo->SchemaPresets.contains(presetId),
                    "Failed to find schema preset %" PRIu32 " in an olap store", presetId);
                auto& preset = storeInfo->SchemaPresets.at(presetId);
                size_t presetIndex = preset.ProtoIndex;
                create->MutableSchemaPreset()->CopyFrom(storeInfo->Description.GetSchemaPresets(presetIndex));
            }
            if (tableInfo->Description.HasTtlSettings()) {
                create->MutableTtlSettings()->CopyFrom(tableInfo->Description.GetTtlSettings());
            }
            if (tableInfo->Description.HasSchemaPresetVersionAdj()) {
                create->SetSchemaPresetVersionAdj(tableInfo->Description.GetSchemaPresetVersionAdj());
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
                << "TCreateColumnTable TPropose"
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
        Y_VERIFY(txState->TxType == TTxState::TxCreateColumnTable);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        TColumnTableInfo::TPtr pending = context.SS->ColumnTables[pathId];
        Y_VERIFY(pending);
        TColumnTableInfo::TPtr table = pending->AlterData;
        Y_VERIFY(table);
        context.SS->ColumnTables[pathId] = table;

        context.SS->PersistColumnTableAlterRemove(db, pathId);
        context.SS->PersistColumnTable(db, pathId, *table);

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
        Y_VERIFY(txState->TxType == TTxState::TxCreateColumnTable);

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
                << "TCreateColumnTable TProposedWaitParts"
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
        Y_VERIFY(txState->TxType == TTxState::TxCreateColumnTable);

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
        Y_VERIFY(txState->TxType == TTxState::TxCreateColumnTable);

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

class TCreateColumnTable: public TSubOperation {
    const TOperationId OperationId;
    const TTxTransaction Transaction;
    TTxState::ETxState State = TTxState::Invalid;

    TTxState::ETxState NextState() {
        return TTxState::ConfigureParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) {
        switch(state) {
        case TTxState::Waiting:
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
        using TPtr = TSubOperationState::TPtr;

        switch(state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            return TPtr(new TConfigureParts(OperationId));
        case TTxState::Propose:
            return TPtr(new TPropose(OperationId));
        case TTxState::ProposedWaitParts:
            return TPtr(new TProposedWaitParts(OperationId));
        case TTxState::Done:
            return TPtr(new TDone(OperationId));
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
    TCreateColumnTable(TOperationId id, const TTxTransaction& tx)
        : OperationId(id)
        , Transaction(tx)
    {}

    TCreateColumnTable(TOperationId id, TTxState::ETxState state)
        : OperationId(id)
        , State(state)
    {
        SetState(SelectStateFunc(state));
    }

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto acceptExisted = !Transaction.GetFailOnExist();
        const TString& parentPathStr = Transaction.GetWorkingDir();
        auto& createDescription = Transaction.GetCreateColumnTable();
        const TString& name = createDescription.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateColumnTable Propose"
                        << ", path: " << parentPathStr << "/" << name
                        << ", opId: " << OperationId
                        << ", at schemeshard: " << ssId);

        TEvSchemeShard::EStatus status = NKikimrScheme::StatusAccepted;
        auto result = MakeHolder<TProposeResponse>(status, ui64(OperationId.GetTxId()), ui64(ssId));

        NSchemeShard::TPath parentPath = NSchemeShard::TPath::Resolve(parentPathStr, context.SS);
        {
            NSchemeShard::TPath::TChecker checks = parentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .HasOlapStore()
                .IsCommonSensePath()
                .IsLikeDirectory();

            if (!checks) {
                TString explain = TStringBuilder() << "parent path fail checks"
                                                   << ", path: " << parentPath.PathString();
                auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
                return result;
            }
        }

        const TString acl = Transaction.GetModifyACL().GetDiffACL();

        NSchemeShard::TPath dstPath = parentPath.Child(name);
        {
            NSchemeShard::TPath::TChecker checks = dstPath.Check();
            checks.IsAtLocalSchemeShard();
            if (dstPath.IsResolved()) {
                checks
                    .IsResolved()
                    .NotUnderDeleting()
                    .FailOnExist(TPathElement::EPathType::EPathTypeColumnTable, acceptExisted);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                checks
                    .IsValidLeafName()
                    .DepthLimit()
                    .PathsLimit()
                    .DirChildrenLimit()
                    .IsValidACL(acl);
            }

            if (!checks) {
                TString explain = TStringBuilder() << "dst path fail checks"
                                                   << ", path: " << dstPath.PathString();
                auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
                if (dstPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(dstPath.Base()->CreateTxId));
                    result->SetPathId(dstPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        TString errStr;

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        auto olapStorePath = dstPath.FindOlapStore();
        Y_VERIFY(olapStorePath, "Unexpected failure to find an olap store");
        auto storeInfo = context.SS->OlapStores.at(olapStorePath->PathId);
        {
            NSchemeShard::TPath::TChecker checks = olapStorePath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsOlapStore()
                .NotUnderOperation();

            if (!checks) {
                TString explain = TStringBuilder() << "olap store fail checks"
                                                   << ", path: " << olapStorePath.PathString();
                auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
                return result;
            }
        }

        if (!AppData()->FeatureFlags.GetEnableOlapSchemaOperations()) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed,
                "Olap schema operations are not supported");
            return result;
        }

        TColumnTableInfo::TPtr tableInfo = CreateColumnTable(createDescription, storeInfo, *parentPath.DomainInfo(), status, errStr, context.SS);
        if (!tableInfo) {
            result->SetError(status, errStr);
            return result;
        }
        if (!context.SS->CheckInFlightLimit(TTxState::TxCreateColumnTable, errStr)) {
            result->SetError(NKikimrScheme::StatusResourceExhausted, errStr);
            return result;
        }

        dstPath.MaterializeLeaf(owner);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        context.SS->TabletCounters->Simple()[COUNTER_COLUMN_TABLE_COUNT].Add(1);

        TPathId pathId = dstPath.Base()->PathId;
        dstPath.Base()->CreateTxId = OperationId.GetTxId();
        dstPath.Base()->LastTxId = OperationId.GetTxId();
        dstPath.Base()->PathState = TPathElement::EPathState::EPathStateCreate;
        dstPath.Base()->PathType = TPathElement::EPathType::EPathTypeColumnTable;

        NIceDb::TNiceDb db(context.GetDB());

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateColumnTable, pathId);
        txState.State = TTxState::ConfigureParts;

        txState.Shards.reserve(tableInfo->ColumnShards.size());
        for (ui64 columnShardId : tableInfo->ColumnShards) {
            auto tabletId = TTabletId(columnShardId);
            auto shardIdx = context.SS->TabletIdToShardIdx.at(tabletId);
            TShardInfo& shardInfo = context.SS->ShardInfos.at(shardIdx);
            txState.Shards.emplace_back(shardIdx, ETabletType::ColumnShard, TTxState::ConfigureParts);
            // N.B. we seem to only need CurrentTxId when creating/modifying tablets
            shardInfo.CurrentTxId = OperationId.GetTxId();
            context.SS->PersistShardTx(db, shardIdx, OperationId.GetTxId());
        }

        TColumnTableInfo::TPtr pending = new TColumnTableInfo;
        pending->AlterData = tableInfo;
        pending->SetOlapStorePathId(olapStorePath->PathId);
        tableInfo->SetOlapStorePathId(olapStorePath->PathId);
        context.SS->ColumnTables[pathId] = pending;
        storeInfo->ColumnTables.insert(pathId);
        storeInfo->ColumnTablesUnderOperation.insert(pathId);
        context.SS->PersistColumnTable(db, pathId, *pending);
        context.SS->PersistColumnTableAlter(db, pathId, *tableInfo);
        context.SS->IncrementPathDbRefCount(pathId);

        if (parentPath.Base()->HasActiveChanges()) {
            TTxId parentTxId = parentPath.Base()->PlannedToCreate() ? parentPath.Base()->CreateTxId : parentPath.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        // Sequentially chain operations in the same olap store
        if (context.SS->Operations.contains(olapStorePath.Base()->LastTxId)) {
            context.OnComplete.Dependence(olapStorePath.Base()->LastTxId, OperationId.GetTxId());
        }
        olapStorePath.Base()->LastTxId = OperationId.GetTxId();
        context.SS->PersistLastTxId(db, olapStorePath.Base());

        context.SS->PersistTxState(db, OperationId);
        context.SS->PersistPath(db, dstPath.Base()->PathId);

        context.OnComplete.ActivateTx(OperationId);

        if (!acl.empty()) {
            dstPath.Base()->ApplyACL(acl);
            context.SS->PersistACL(db, dstPath.Base());
        }

        context.SS->PersistUpdateNextPathId(db);
        context.SS->PersistUpdateNextShardIdx(db);

        ++parentPath.Base()->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentPath.Base());
        context.SS->ClearDescribePathCaches(parentPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, parentPath.Base()->PathId);

        context.SS->ClearDescribePathCaches(dstPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, dstPath.Base()->PathId);

        dstPath.DomainInfo()->IncPathsInside();
        parentPath.Base()->IncAliveChildren();

        State = NextState();
        SetState(SelectStateFunc(State));
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TCreateColumnTable");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateColumnTable AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};


} // namespace

ISubOperationBase::TPtr CreateNewColumnTable(TOperationId id, const TTxTransaction& tx) {
    return new TCreateColumnTable(id, tx);
}

ISubOperationBase::TPtr CreateNewColumnTable(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return new TCreateColumnTable(id, state);
}


} // namespace NSchemeShard
} // namespace NKikimr
