#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"
#include "schemeshard_olap_types.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/mind/hive/hive.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;


// TODO: make it a part of TOlapStoreInfo
bool PrepareSchemaPreset(NKikimrSchemeOp::TColumnTableSchemaPreset& proto, TOlapStoreInfo& store,
                         size_t protoIndex, IErrorCollector& errors)
{
    TOlapStoreSchemaPreset preset;
    if (!preset.ParseFromRequest(proto, errors)) {
        return false;
    }
    const auto presetId = store.Description.GetNextSchemaPresetId();
    if (store.SchemaPresets.contains(presetId) || store.SchemaPresetByName.contains(preset.GetName())) {
        errors.AddError(Sprintf("Duplicate schema preset %" PRIu32 " with name '%s'", presetId, proto.GetName().c_str()));
        return false;
    }
    preset.SetId(presetId);
    preset.SetProtoIndex(protoIndex);

    TOlapSchemaUpdate schemaDiff;
    if (!schemaDiff.Parse(proto.GetSchema(), errors, true)) {
        return false;
    }

    if (!preset.Update(schemaDiff, errors)) {
        return false;
    }
    proto.Clear();
    preset.Serialize(proto); 
    
    store.Description.SetNextSchemaPresetId(presetId + 1);
    store.SchemaPresetByName[preset.GetName()] = preset.GetId();
    store.SchemaPresets[preset.GetId()] = std::move(preset);
    return true;
}

TOlapStoreInfo::TPtr CreateOlapStore(const NKikimrSchemeOp::TColumnStoreDescription& opSrc, IErrorCollector& errors)
{
    TOlapStoreInfo::TPtr storeInfo = new TOlapStoreInfo;
    storeInfo->AlterVersion = 1;
    storeInfo->Description = opSrc;
    auto& op = storeInfo->Description;

    if (op.GetRESERVED_MetaShardCount() != 0) {
        errors.AddError("trying to create OLAP store with meta shards (not supported yet)");
        return nullptr;
    }

    if (!op.HasColumnShardCount()) {
        errors.AddError("trying to create OLAP store without shards number specified");
        return nullptr;
    }

    if (op.GetColumnShardCount() == 0) {
        errors.AddError("trying to create OLAP store without zero shards");
        return nullptr;
    }

    for (auto& presetProto : *op.MutableRESERVED_TtlSettingsPresets()) {
        Y_UNUSED(presetProto);
        errors.AddError("TTL presets are not supported");
        return nullptr;
    }

    op.SetNextSchemaPresetId(1);
    op.SetNextTtlSettingsPresetId(1);

    size_t protoIndex = 0;
    for (auto& presetProto : *op.MutableSchemaPresets()) {
        if (presetProto.HasId()) {
            errors.AddError("Schema preset id cannot be specified explicitly");
            return nullptr;
        }
        if (!PrepareSchemaPreset(presetProto, *storeInfo, protoIndex++, errors)) {
            return nullptr;
        }
    }

    if (!storeInfo->SchemaPresetByName.contains("default") || storeInfo->SchemaPresets.size() > 1) {
        errors.AddError("A single schema preset named 'default' is required");
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

        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxCreateOlapStore); 
        TOlapStoreInfo::TPtr pendingInfo = context.SS->OlapStores[txState->TargetPathId];
        Y_VERIFY(pendingInfo);
        Y_VERIFY(pendingInfo->AlterData);
        TOlapStoreInfo::TPtr storeInfo = pendingInfo->AlterData;

        txState->ClearShardsInProgress();
        TString columnShardTxBody;
        {
            auto seqNo = context.SS->StartRound(*txState);
            NKikimrTxColumnShard::TSchemaTxBody tx;
            context.SS->FillSeqNo(tx, seqNo);
            
            NSchemeShard::TPath path = NSchemeShard::TPath::Init(txState->TargetPathId, context.SS);
            Y_VERIFY(path.IsResolved());

            // TODO: we may need to specify a more complex data channel mapping
            auto* init = tx.MutableInitShard();
            init->SetDataChannelCount(storeInfo->Description.GetStorageConfig().GetDataChannelCount());
            init->SetOwnerPathId(txState->TargetPathId.LocalPathId);
            init->SetOwnerPath(path.PathString());

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

        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxCreateOlapStore); 

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

        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxCreateOlapStore); 

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
        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxCreateOlapStore); 

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

        TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxCreateOlapStore); 
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
    static TTxState::ETxState NextState() {
        return TTxState::CreateParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
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
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        using TPtr = TSubOperationState::TPtr;

        switch (state) {
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

public:
    using TSubOperation::TSubOperation;

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
                result->SetError(checks.GetStatus(), checks.GetError());
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
                result->SetError(checks.GetStatus(), checks.GetError());
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

        TProposeErrorCollector errors(*result);
        TOlapStoreInfo::TPtr storeInfo = CreateOlapStore(createDescription, errors);
        if (!storeInfo.Get()) {
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

        const ui64 shardsToCreate = storeInfo->ColumnShards.size();
        {
            NSchemeShard::TPath::TChecker checks = dstPath.Check();
            checks
                .ShardsLimit(shardsToCreate)
                .PathShardsLimit(shardsToCreate);

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
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

        SetState(NextState());
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

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateNewOlapStore(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateOlapStore>(id, tx);
}

ISubOperation::TPtr CreateNewOlapStore(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return MakeSubOperation<TCreateOlapStore>(id, state);
}

}
