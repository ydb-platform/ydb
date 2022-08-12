#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/mind/hive/hive.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

bool PrepareTier(const NKikimrSchemeOp::TStorageTierConfig& proto, TString& errStr) {
    Y_UNUSED(proto);
    Y_UNUSED(errStr);
    // TODO
    return true;
}

// TODO: make it a part of TOlapSchema
bool PrepareSchema(NKikimrSchemeOp::TColumnTableSchema& proto, TOlapSchema& schema, TString& errStr) {
    schema.NextColumnId = proto.GetNextColumnId();

    const NScheme::TTypeRegistry* typeRegistry = AppData()->TypeRegistry;

    schema.Columns.clear();
    for (auto& colProto : *proto.MutableColumns()) {
        if (colProto.GetName().empty()) {
            errStr = Sprintf("Columns cannot have an empty name");
            return false;
        }
        if (!colProto.HasId()) {
            colProto.SetId(schema.NextColumnId++);
        } else if (colProto.GetId() <= 0 || colProto.GetId() >= schema.NextColumnId) {
            errStr = Sprintf("Column id is incorrect");
            return false;
        }
        ui32 colId = colProto.GetId();
        if (schema.Columns.contains(colId)) {
            errStr = Sprintf("Duplicate column id %" PRIu32 " for column '%s'", colId, colProto.GetName().c_str());
            return false;
        }
        auto& col = schema.Columns[colId];
        col.Id = colId;
        col.Name = colProto.GetName();

        if (colProto.HasTypeId()) {
            errStr = Sprintf("Cannot set TypeId for column '%s', use Type", col.Name.c_str());
            return false;
        }
        if (!colProto.HasType()) {
            errStr = Sprintf("Missing Type for column '%s'", col.Name.c_str());
            return false;
        }

        auto typeName = NMiniKQL::AdaptLegacyYqlType(colProto.GetType());
        const NScheme::IType* type = typeRegistry->GetType(typeName);
        if (!type || !NScheme::NTypeIds::IsYqlType(type->GetTypeId())) {
            errStr = Sprintf("Type '%s' specified for column '%s' is not supported", colProto.GetType().c_str(), col.Name.c_str());
            return false;
        }
        colProto.SetTypeId(type->GetTypeId());
        col.TypeId = type->GetTypeId();

        if (schema.ColumnsByName.contains(col.Name)) {
            errStr = Sprintf("Duplicate column '%s'", col.Name.c_str());
            return false;
        }
        schema.ColumnsByName[col.Name] = col.Id;
    }

    if (schema.Columns.empty()) {
        errStr = Sprintf("At least one column is required");
        return false;
    }

    schema.KeyColumnIds.clear();
    for (const TString& keyName : proto.GetKeyColumnNames()) {
        auto* col = schema.FindColumnByName(keyName);
        if (!col) {
            errStr = Sprintf("Unknown key column '%s'", keyName.c_str());
            return false;
        }
        if (col->IsKeyColumn()) {
            errStr = Sprintf("Duplicate key column '%s'", keyName.c_str());
            return false;
        }
        col->KeyOrder = schema.KeyColumnIds.size();
        schema.KeyColumnIds.push_back(col->Id);
    }

    if (schema.KeyColumnIds.empty()) {
        errStr = "At least one key column is required";
        return false;
    }

    for (auto& tierConfig : proto.GetStorageTiers()) {
        TString tierName = tierConfig.GetName();
        if (schema.Tiers.count(tierName)) {
            errStr = Sprintf("Same tier name in schema: '%s'", tierName.c_str());
            return false;
        }
        schema.Tiers.insert(tierName);

        if (!PrepareTier(tierConfig, errStr)) {
            return false;
        }
    }

    schema.Engine = proto.GetEngine();

    proto.SetNextColumnId(schema.NextColumnId);
    return true;
}

// TODO: make it a part of TOlapStoreInfo
bool PrepareSchemaPreset(NKikimrSchemeOp::TColumnTableSchemaPreset& proto, TOlapStoreInfo& store,
                         size_t protoIndex, TString& errStr)
{
    if (proto.GetName().empty()) {
        errStr = Sprintf("Schema preset name cannot be empty");
        return false;
    }
    if (!proto.HasId()) {
        proto.SetId(store.Description.GetNextSchemaPresetId());
        store.Description.SetNextSchemaPresetId(proto.GetId() + 1);
    } else if (proto.GetId() <= 0 || proto.GetId() >= store.Description.GetNextSchemaPresetId()) {
        errStr = Sprintf("Schema preset id is incorrect");
        return false;
    }
    if (store.SchemaPresets.contains(proto.GetId()) ||
        store.SchemaPresetByName.contains(proto.GetName()))
    {
        errStr = Sprintf("Duplicate schema preset %" PRIu32 " with name '%s'", proto.GetId(), proto.GetName().c_str());
        return false;
    }
    auto& preset = store.SchemaPresets[proto.GetId()];
    preset.Id = proto.GetId();
    preset.Name = proto.GetName();
    preset.ProtoIndex = protoIndex;
    store.SchemaPresetByName[preset.Name] = preset.Id;

    proto.MutableSchema()->SetNextColumnId(1);
    proto.MutableSchema()->SetVersion(1);

    if (!PrepareSchema(*proto.MutableSchema(), preset, errStr)) {
        return false;
    }
    return true;
}

TOlapStoreInfo::TPtr CreateOlapStore(const NKikimrSchemeOp::TColumnStoreDescription& opSrc,
                                    TEvSchemeShard::EStatus& status, TString& errStr)
{
    TOlapStoreInfo::TPtr storeInfo = new TOlapStoreInfo;
    storeInfo->AlterVersion = 1;
    storeInfo->Description = opSrc;
    auto& op = storeInfo->Description;

    if (op.GetRESERVED_MetaShardCount() != 0) {
        status = NKikimrScheme::StatusSchemeError;
        errStr = Sprintf("trying to create OLAP store with meta shards (not supported yet)");
        return nullptr;
    }

    if (!op.HasColumnShardCount()) {
        status = NKikimrScheme::StatusSchemeError;
        errStr = Sprintf("trying to create OLAP store without shards number specified");
        return nullptr;
    }

    op.SetNextSchemaPresetId(1);
    op.SetNextTtlSettingsPresetId(1);

    size_t protoIndex = 0;
    for (auto& presetProto : *op.MutableSchemaPresets()) {
        if (presetProto.HasId()) {
            status = NKikimrScheme::StatusSchemeError;
            errStr = Sprintf("Schema preset id cannot be specified explicitly");
            return nullptr;
        }
        if (!PrepareSchemaPreset(presetProto, *storeInfo, protoIndex++, errStr)) {
            status = NKikimrScheme::StatusSchemeError;
            return nullptr;
        }
    }

    protoIndex = 0;
    for (auto& presetProto : *op.MutableRESERVED_TtlSettingsPresets()) {
        Y_UNUSED(presetProto);
        status = NKikimrScheme::StatusSchemeError;
        errStr = "TTL presets are not supported";
        return nullptr;
    }

    if (!storeInfo->SchemaPresetByName.contains("default") || storeInfo->SchemaPresets.size() > 1) {
        status = NKikimrScheme::StatusSchemeError;
        errStr = "A single schema preset named 'default' is required";
        return nullptr;
    }

    storeInfo->ColumnShards.resize(op.GetColumnShardCount());
    return storeInfo;
}

void ApplySharding(TTxId txId, TPathId pathId, TOlapStoreInfo::TPtr storeInfo,
                   const TChannelsBindings& channelsBindings,
                   TTxState& txState, TSchemeShard* ss)
{
    ui32 numColumnShards = storeInfo->ColumnShards.size();
    ui32 numShards = numColumnShards;

    txState.Shards.reserve(numShards);

    TShardInfo columnShardInfo = TShardInfo::ColumnShardInfo(txId, pathId);
    columnShardInfo.BindedChannels = channelsBindings;

    storeInfo->Sharding.ClearColumnShards();
    for (ui64 i = 0; i < numColumnShards; ++i) {
        TShardIdx idx = ss->RegisterShardInfo(columnShardInfo);
        ss->TabletCounters->Simple()[COUNTER_COLUMN_SHARDS].Add(1);
        txState.Shards.emplace_back(idx, ETabletType::ColumnShard, TTxState::CreateParts);

        storeInfo->ColumnShards[i] = idx;
        auto* shardInfoProto = storeInfo->Sharding.AddColumnShards();
        shardInfoProto->SetOwnerId(idx.GetOwnerId());
        shardInfoProto->SetLocalId(idx.GetLocalId().GetValue());
    }

    ss->SetPartitioning(pathId, storeInfo);
}

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCreateOlapStore TConfigureParts"
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
        Y_VERIFY(txState->TxType == TTxState::TxCreateOlapStore);
        TOlapStoreInfo::TPtr pendingInfo = context.SS->OlapStores[txState->TargetPathId];
        Y_VERIFY(pendingInfo);
        Y_VERIFY(pendingInfo->AlterData);
        TOlapStoreInfo::TPtr storeInfo = pendingInfo->AlterData;

        txState->ClearShardsInProgress();

        auto seqNo = context.SS->StartRound(*txState);

        TString columnShardTxBody;
        {
            NKikimrTxColumnShard::TSchemaTxBody tx;
            context.SS->FillSeqNo(tx, seqNo);

            // TODO: we may need to specify a more complex data channel mapping
            auto* init = tx.MutableInitShard();
            init->SetDataChannelCount(storeInfo->Description.GetStorageConfig().GetDataChannelCount());
            init->SetStorePathId(txState->TargetPathId.LocalPathId);

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
                << "TCreateOlapStore TPropose"
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
        Y_VERIFY(txState->TxType == TTxState::TxCreateOlapStore);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        TOlapStoreInfo::TPtr pending = context.SS->OlapStores[pathId];
        Y_VERIFY(pending);
        TOlapStoreInfo::TPtr store = pending->AlterData;
        Y_VERIFY(store);
        context.SS->OlapStores[pathId] = store;

        context.SS->PersistOlapStoreAlterRemove(db, pathId);
        context.SS->PersistOlapStore(db, pathId, *store);

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
        Y_VERIFY(txState->TxType == TTxState::TxCreateOlapStore);

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
                << "TCreateOlapStore TProposedWaitParts"
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
        Y_VERIFY(txState->TxType == TTxState::TxCreateOlapStore);

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
        Y_VERIFY(txState->TxType == TTxState::TxCreateOlapStore);

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

class TCreateOlapStore: public TSubOperation {
    const TOperationId OperationId;
    const TTxTransaction Transaction;
    TTxState::ETxState State = TTxState::Invalid;

    TTxState::ETxState NextState() {
        return TTxState::CreateParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) {
        switch(state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return TTxState::ConfigureParts;
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
        case TTxState::CreateParts:
            return TPtr(new TCreateParts(OperationId));
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
    TCreateOlapStore(TOperationId id, const TTxTransaction& tx)
        : OperationId(id)
        , Transaction(tx)
    {}

    TCreateOlapStore(TOperationId id, TTxState::ETxState state)
        : OperationId(id)
        , State(state)
    {
        SetState(SelectStateFunc(state));
    }

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto acceptExisted = !Transaction.GetFailOnExist();
        const TString& parentPathStr = Transaction.GetWorkingDir();
        auto& createDescription = Transaction.GetCreateColumnStore();
        const TString& name = createDescription.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateOlapStore Propose"
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
                .NoOlapStore()
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
                    .FailOnExist(TPathElement::EPathType::EPathTypeColumnStore, acceptExisted);
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

        if (!AppData()->FeatureFlags.GetEnableOlapSchemaOperations()) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed,
                "Olap schema operations are not supported");
            return result;
        }

        TOlapStoreInfo::TPtr storeInfo = CreateOlapStore(createDescription, status, errStr);
        if (!storeInfo.Get()) {
            result->SetError(status, errStr);
            return result;
        }

        // Make it easier by having data channel count always specified internally
        if (!storeInfo->Description.GetStorageConfig().HasDataChannelCount()) {
            storeInfo->Description.MutableStorageConfig()->SetDataChannelCount(1);
        }

        // Construct channels bindings for columnshards
        TChannelsBindings channelsBindings;
        if (!context.SS->GetOlapChannelsBindings(dstPath.GetPathIdForDomain(), storeInfo->Description.GetStorageConfig(), channelsBindings, errStr)) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }
        if (!context.SS->CheckInFlightLimit(TTxState::TxCreateOlapStore, errStr)) {
            result->SetError(NKikimrScheme::StatusResourceExhausted, errStr);
            return result;
        }

        const ui64 shardsToCreate = storeInfo->ColumnShards.size();
        {
            NSchemeShard::TPath::TChecker checks = dstPath.Check();
            checks
                .ShardsLimit(shardsToCreate)
                .PathShardsLimit(shardsToCreate);

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

        dstPath.MaterializeLeaf(owner);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        context.SS->TabletCounters->Simple()[COUNTER_OLAP_STORE_COUNT].Add(1);

        TPathId pathId = dstPath.Base()->PathId;
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateOlapStore, pathId);

        ApplySharding(OperationId.GetTxId(), pathId, storeInfo, channelsBindings, txState, context.SS);

        NIceDb::TNiceDb db(context.GetDB());

        TOlapStoreInfo::TPtr pending = new TOlapStoreInfo;
        pending->AlterData = storeInfo;
        context.SS->OlapStores[pathId] = pending;
        context.SS->PersistOlapStore(db, pathId, *pending);
        context.SS->PersistOlapStoreAlter(db, pathId, *storeInfo);
        context.SS->IncrementPathDbRefCount(pathId);

        for (auto shard : txState.Shards) {
            Y_VERIFY(shard.Operation == TTxState::CreateParts);
            context.SS->PersistShardMapping(db, shard.Idx, InvalidTabletId, pathId, OperationId.GetTxId(), shard.TabletType);
            switch (shard.TabletType) {
                case ETabletType::ColumnShard: {
                    context.SS->PersistChannelsBinding(db, shard.Idx, channelsBindings);
                    break;
                }
                default: {
                    Y_FAIL("Unexpected tablet type");
                }
            }
        }
        Y_VERIFY(txState.Shards.size() == shardsToCreate);

        dstPath.Base()->CreateTxId = OperationId.GetTxId();
        dstPath.Base()->LastTxId = OperationId.GetTxId();
        dstPath.Base()->PathState = TPathElement::EPathState::EPathStateCreate;
        dstPath.Base()->PathType = TPathElement::EPathType::EPathTypeColumnStore;

        if (parentPath.Base()->HasActiveChanges()) {
            TTxId parentTxId = parentPath.Base()->PlannedToCreate() ? parentPath.Base()->CreateTxId : parentPath.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::CreateParts);
        context.OnComplete.ActivateTx(OperationId);

        context.SS->PersistTxState(db, OperationId);
        context.SS->PersistPath(db, dstPath.Base()->PathId);

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
        dstPath.DomainInfo()->AddInternalShards(txState);
        dstPath.Base()->IncShardsInside(shardsToCreate);
        parentPath.Base()->IncAliveChildren();

        State = NextState();
        SetState(SelectStateFunc(State));
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TCreateOlapStore");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateOlapStore AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr {
namespace NSchemeShard {

ISubOperationBase::TPtr CreateNewOlapStore(TOperationId id, const TTxTransaction& tx) {
    return new TCreateOlapStore(id, tx);
}

ISubOperationBase::TPtr CreateNewOlapStore(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return new TCreateOlapStore(id, state);
}

}
}
