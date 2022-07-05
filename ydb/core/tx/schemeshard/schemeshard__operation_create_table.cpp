#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/core/base/subdomain.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

void PrepareScheme(NKikimrSchemeOp::TTableDescription& schema) {
    // Ignore column ids if they were passed by user!
    for (auto& col : *schema.MutableColumns()) {
        col.ClearId();
    }
    schema.ClearKeyColumnIds();
}

bool InitPartitioning(const NKikimrSchemeOp::TTableDescription& op,
                      const NScheme::TTypeRegistry* typeRegistry,
                      const TVector<ui32>& keyColIds,
                      const TVector<NScheme::TTypeId>& keyColTypeIds,
                      TString& errStr,
                      TVector<TTableShardInfo>& partitions,
                      const TSchemeLimits& limits) {
    ui32 partitionCount = 1;
    if (op.HasUniformPartitionsCount()) {
        partitionCount = op.GetUniformPartitionsCount();
    } else {
        partitionCount = op.SplitBoundarySize() + 1;
    }

    if (partitionCount == 0 || partitionCount > limits.MaxShardsInPath) {
        errStr = Sprintf("Invalid partition count specified: %u", partitionCount);
        return false;
    }

    TVector<TString> rangeEnds;
    if (op.HasUniformPartitionsCount()) {
        Y_VERIFY(!keyColIds.empty());
        NScheme::TTypeId firstKeyColType = keyColTypeIds[0];
        if (!TSchemeShard::FillUniformPartitioning(rangeEnds, keyColIds.size(), firstKeyColType, partitionCount, typeRegistry, errStr)) {
            return false;
        }
    } else {
        if (!TSchemeShard::FillSplitPartitioning(rangeEnds, keyColTypeIds, op.GetSplitBoundary(), errStr)) {
            return false;
        }
    }

    for (ui32 i = 0; i < rangeEnds.size(); ++i) {
        partitions.emplace_back(InvalidShardIdx, rangeEnds[i]);
    }
    // Add end of last range
    partitions.emplace_back(InvalidShardIdx, TString());

    // Check that range ends are sorted in ascending order
    TVector<TCell> prevKey(keyColTypeIds.size()); // Start from (NULL, NULL, .., NULL)
    for (ui32 i = 0; i < partitions.size(); ++i) {
        TSerializedCellVec key(partitions[i].EndOfRange);
        if (CompareBorders<true, true>(prevKey, key.GetCells(), true, true, keyColTypeIds) >= 0) {
            errStr = Sprintf("Partition ranges are not sorted at index %u", i);
            return false;
        }
        prevKey.assign(key.GetCells().begin(), key.GetCells().end());
    }

    return true;
}


bool DoInitPartitioning(TTableInfo::TPtr tableInfo,
                        const NKikimrSchemeOp::TTableDescription& op,
                        const NScheme::TTypeRegistry* typeRegistry,
                        TString& errStr,
                        TVector<TTableShardInfo>& partitions,
                        const TSchemeLimits& limits) {
    const TVector<ui32>& keyColIds = tableInfo->KeyColumnIds;
    if (keyColIds.size() == 0) {
        errStr = Sprintf("No key columns specified");
        return false;
    }

    TVector<NScheme::TTypeId> keyColTypeIds;
    for (ui32 ki : keyColIds) {
        NScheme::TTypeId typeId = tableInfo->Columns[ki].PType;

        if (!IsAllowedKeyType(typeId)) {
            errStr = Sprintf("Column %s has wrong key type %s",
                tableInfo->Columns[ki].Name.c_str(), NScheme::GetTypeName(typeId).c_str());
            return false;
        }

        keyColTypeIds.push_back(typeId);
    }

    if (!InitPartitioning(op, typeRegistry, keyColIds, keyColTypeIds, errStr, partitions, limits)) {
        return false;
    }

    return true;
}

void ApplyPartitioning(TTxId txId,
                       const TPathId& pathId,
                       TTableInfo::TPtr tableInfo,
                       TTxState& txState,
                       const TChannelsBindings& bindedChannels,
                       TSchemeShard* ss,
                       TVector<TTableShardInfo>& partitions) {
    TShardInfo datashardInfo = TShardInfo::DataShardInfo(txId, pathId);
    datashardInfo.BindedChannels = bindedChannels;

    ui64 count = partitions.size();
    txState.Shards.reserve(count);
    for (ui64 i = 0; i < count; ++i) {
        auto idx = ss->RegisterShardInfo(datashardInfo);
        txState.Shards.emplace_back(idx, ETabletType::DataShard, TTxState::CreateParts);
        partitions[i].ShardIdx = idx;
    }

    ss->SetPartitioning(pathId, tableInfo, std::move(partitions));
}


class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCreateTable TConfigureParts"
                << " operationId#" << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvProposeTransactionResult"
                               << " at tabletId# " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvProposeTransactionResult"
                                << " message: " << ev->Get()->Record.ShortDebugString());

         return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                   << " at tabletId# " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState->TxType == TTxState::TxCreateTable);

        NKikimrTxDataShard::TFlatSchemeTransaction txTemplate;
        context.SS->FillAsyncIndexInfo(txState->TargetPathId, txTemplate);

        txState->ClearShardsInProgress();

        const ui64 subDomainPathId = context.SS->ResolvePathIdForDomain(txState->TargetPathId).LocalPathId;

        for (ui32 i = 0; i < txState->Shards.size(); ++i) {
            TShardIdx shardIdx = txState->Shards[i].Idx;
            TTabletId datashardId = context.SS->ShardInfos[shardIdx].TabletID;

            auto seqNo = context.SS->StartRound(*txState);

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                        << " Propose modify scheme on datashard"
                        << " datashardId: " << datashardId
                        << " seqNo: " << seqNo);

            NKikimrTxDataShard::TFlatSchemeTransaction tx(txTemplate);
            auto tableDesc = tx.MutableCreateTable();
            context.SS->FillSeqNo(tx, seqNo);
            context.SS->FillTableDescription(txState->TargetPathId, i, NEW_TABLE_ALTER_VERSION, tableDesc);

            TString txBody;
            Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&txBody);

            THolder<TEvDataShard::TEvProposeTransaction> event =
                THolder(new TEvDataShard::TEvProposeTransaction(NKikimrTxDataShard::TX_KIND_SCHEME,
                                                        context.SS->TabletID(),
                                                        subDomainPathId,
                                                        context.Ctx.SelfID,
                                                        ui64(OperationId.GetTxId()),
                                                        txBody,
                                                        context.SS->SelectProcessingPrarams(txState->TargetPathId)));

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << " Propose modify scheme on datashard"
                                    << " datashardId: " << datashardId
                                    << " message: " << event->Record.ShortDebugString());

            context.OnComplete.BindMsgToPipe(OperationId, datashardId, shardIdx,  event.Release());
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
                << "TCreateTable TPropose"
                << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, TEvDataShard::TEvProposeTransactionResult::EventType});
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
                     << " at tablet: " << ssId
                     << ", stepId: " << step);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState->TxType == TTxState::TxCreateTable);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        TTableInfo::TPtr table = context.SS->Tables[pathId];
        Y_VERIFY(table);
        table->AlterVersion = NEW_TABLE_ALTER_VERSION;

        if (table->IsTTLEnabled() && !context.SS->TTLEnabledTables.contains(pathId)) {
            context.SS->TTLEnabledTables[pathId] = table;
            context.SS->TabletCounters->Simple()[COUNTER_TTL_ENABLED_TABLE_COUNT].Add(1);

            const auto now = context.Ctx.Now();
            for (auto& shard : table->GetPartitions()) {
                auto& lag = shard.LastCondEraseLag;
                Y_VERIFY_DEBUG(!lag.Defined());

                lag = now - shard.LastCondErase;
                context.SS->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].IncrementFor(lag->Seconds());
            }
        }
        context.SS->PersistTableCreated(db, pathId);

        auto parentDir = context.SS->PathsById.at(path->ParentPathId);
        if (parentDir->IsDirectory() || parentDir->IsDomainRoot()) {
            ++parentDir->DirAlterVersion;
            context.SS->PersistPathDirAlterVersion(db, parentDir);
        }
        context.SS->ClearDescribePathCaches(parentDir);
        context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);

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
        Y_VERIFY(txState->TxType == TTxState::TxCreateTable);

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

class TCreateTable: public TSubOperation {
    const TOperationId OperationId;
    const TTxTransaction Transaction;
    TTxState::ETxState State = TTxState::Invalid;

    bool AllowShadowData = false;
    THashSet<TString> LocalSequences;

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
        switch(state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return THolder(new TCreateParts(OperationId));
        case TTxState::ConfigureParts:
            return THolder(new TConfigureParts(OperationId));
        case TTxState::Propose:
            return THolder(new TPropose(OperationId));
        case TTxState::ProposedWaitParts:
            return THolder(new NTableState::TProposedWaitParts(OperationId));
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
    TCreateTable(TOperationId id, const TTxTransaction& tx)
        : OperationId(id)
        , Transaction(tx)
    {
    }

    TCreateTable(TOperationId id, TTxState::ETxState state)
        : OperationId(id)
        , State(state)
    {
        SetState(SelectStateFunc(state));
    }

    void SetAllowShadowDataForBuildIndex() {
        AllowShadowData = true;
    }

    void SetLocalSequences(const THashSet<TString>& localSequences) {
        LocalSequences = localSequences;
    }

    bool IsShadowDataAllowed() const {
        return AllowShadowData || AppData()->AllowShadowDataInSchemeShardForTests;
    }

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto acceptExisted = !Transaction.GetFailOnExist();
        auto schema = Transaction.GetCreateTable();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = schema.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateTable Propose"
                        << ", path: " << parentPathStr << "/" << name
                        << ", opId: " << OperationId
                        << ", at schemeshard: " << ssId);

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TCreateTable Propose"
                        << ", path: " << parentPathStr << "/" << name
                        << ", opId: " << OperationId
                        << ", schema: " << schema.ShortDebugString()
                        << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        NSchemeShard::TPath parentPath = NSchemeShard::TPath::Resolve(parentPathStr, context.SS);
        {
            NSchemeShard::TPath::TChecker checks = parentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting();

            if (checks) {
                if (parentPath.Base()->IsTableIndex()) {
                    checks.IsInsideTableIndexPath()
                          .IsUnderCreating(NKikimrScheme::StatusNameConflict)
                          .IsUnderTheSameOperation(OperationId.GetTxId()); //allow only as part of creating base table
                } else {
                    checks.IsCommonSensePath()
                          .IsLikeDirectory();
                }
            }

            if (!checks) {
                TString explain = TStringBuilder() << "parent path fail checks"
                                                   << ", path: " << parentPath.PathString();
                auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
                return result;
            }
        }

        ui32 shardsToCreate = TTableInfo::ShardsToCreate(schema);
        const TString acl = Transaction.GetModifyACL().GetDiffACL();

        NSchemeShard::TPath dstPath = parentPath.Child(name);
        {
            NSchemeShard::TPath::TChecker checks = dstPath.Check();
            checks.IsAtLocalSchemeShard();
            if (dstPath.IsResolved()) {
                checks
                    .IsResolved()
                    .NotUnderDeleting()
                    .FailOnExist(TPathElement::EPathType::EPathTypeTable, acceptExisted);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                if (!parentPath.Base()->IsTableIndex()) {
                    checks.DepthLimit();
                }

                checks
                    .IsValidLeafName()
                    .PathsLimit()
                    .DirChildrenLimit()
                    .ShardsLimit(shardsToCreate)
                    .PathShardsLimit(shardsToCreate)
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

        if (schema.GetIsBackup()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Cannot create table with explicit 'IsBackup' property");
            return result;
        }

        if (parentPath.Base()->IsTableIndex()) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "Creating private table for table index"
                         << ", opId: " << OperationId);

            if (schema.HasTTLSettings()) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, "TTL on index table is not supported");
                return result;
            }
        }

        auto domainInfo = parentPath.DomainInfo();
        bool transactionSupport = domainInfo->IsSupportTransactions();
        if (domainInfo->GetAlter()) {
            TPathId domainPathId = dstPath.GetPathIdForDomain();
            Y_VERIFY(context.SS->PathsById.contains(domainPathId));
            TPathElement::TPtr domain = context.SS->PathsById.at(domainPathId);
            Y_VERIFY(domain->PlannedToCreate() || domain->HasActiveChanges());

            transactionSupport |= domainInfo->GetAlter()->IsSupportTransactions();
        }
        if (!transactionSupport) {
            result->SetError(NKikimrScheme::StatusNameConflict, "Inclusive subDomian do not support shared transactions");
            return result;
        }

        PrepareScheme(schema);

        TString errStr;

        NKikimrSchemeOp::TPartitionConfig compilationPartitionConfig;
        if (!TPartitionConfigMerger::ApplyChanges(compilationPartitionConfig, TPartitionConfigMerger::DefaultConfig(AppData()), schema.GetPartitionConfig(), AppData(), errStr)
            || !TPartitionConfigMerger::VerifyCreateParams(compilationPartitionConfig, AppData(), IsShadowDataAllowed(), errStr)) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }
        schema.MutablePartitionConfig()->CopyFrom(compilationPartitionConfig);

        if (!schema.GetPartitionConfig().GetPartitioningPolicy().HasMinPartitionsCount()) {
            // This is the expected partitions count, see below
            schema.MutablePartitionConfig()->MutablePartitioningPolicy()->SetMinPartitionsCount(shardsToCreate);
        }

        const NScheme::TTypeRegistry* typeRegistry = AppData()->TypeRegistry;
        const TSchemeLimits& limits = domainInfo->GetSchemeLimits();
        TTableInfo::TAlterDataPtr alterData = TTableInfo::CreateAlterData(nullptr, schema, *typeRegistry, limits, *domainInfo, errStr, LocalSequences);
        if (!alterData.Get()) {
            result->SetError(NKikimrScheme::StatusSchemeError, errStr);
            return result;
        }

        TTableInfo::TPtr tableInfo = new TTableInfo(std::move(*alterData));
        alterData.Reset();

        TVector<TTableShardInfo> partitions;

        if (!DoInitPartitioning(tableInfo, schema, typeRegistry, errStr, partitions, domainInfo->GetSchemeLimits())) {
            result->SetError(NKikimrScheme::StatusSchemeError, errStr);
            return result;
        }
        Y_VERIFY(shardsToCreate == partitions.size());

        TChannelsBindings channelsBinding;

        bool storePerShardConfig = false;
        NKikimrSchemeOp::TPartitionConfig perShardConfig;

        if (context.SS->IsStorageConfigLogic(tableInfo)) {
            TVector<TStorageRoom> storageRooms;
            THashMap<ui32, ui32> familyRooms;
            storageRooms.emplace_back(0);
            if (!context.SS->GetBindingsRooms(dstPath.GetPathIdForDomain(), tableInfo->PartitionConfig(), storageRooms, familyRooms, channelsBinding, errStr)) {
                errStr = TString("database doesn't have required storage pools to create tablet with storage config, details: ") + errStr;
                result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                return result;
            }
            tableInfo->SetRoom(storageRooms[0]);

            storePerShardConfig = true;
            for (const auto& room : storageRooms) {
                perShardConfig.AddStorageRooms()->CopyFrom(room);
            }
            for (const auto& familyRoom : familyRooms) {
                auto* protoFamily = perShardConfig.AddColumnFamilies();
                protoFamily->SetId(familyRoom.first);
                protoFamily->SetRoom(familyRoom.second);
            }
        } else if (context.SS->IsCompatibleChannelProfileLogic(dstPath.GetPathIdForDomain(), tableInfo)) {
            if (!context.SS->GetChannelsBindings(dstPath.GetPathIdForDomain(), tableInfo, channelsBinding, errStr)) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                return result;
            }
        }

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        TUserAttributes::TPtr userAttrs = new TUserAttributes(1);
        const auto& userAttrsDetails = Transaction.GetAlterUserAttributes();
        if (!userAttrs->ApplyPatch(EUserAttributesOp::CreateTable, userAttrsDetails, errStr) ||
            !userAttrs->CheckLimits(errStr))
        {
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        dstPath.MaterializeLeaf(owner);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        TPathElement::TPtr newTable = dstPath.Base();
        newTable->CreateTxId = OperationId.GetTxId();
        newTable->LastTxId = OperationId.GetTxId();
        newTable->PathState = TPathElement::EPathState::EPathStateCreate;
        newTable->PathType = TPathElement::EPathType::EPathTypeTable;
        newTable->UserAttrs->AlterData = userAttrs;

        NIceDb::TNiceDb db(context.GetDB());

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateTable, newTable->PathId);

        ApplyPartitioning(OperationId.GetTxId(), newTable->PathId, tableInfo, txState, channelsBinding, context.SS, partitions);

        Y_VERIFY(tableInfo->GetPartitions().back().EndOfRange.empty(), "End of last range must be +INF");

        context.SS->Tables[newTable->PathId] = tableInfo;
        context.SS->TabletCounters->Simple()[COUNTER_TABLE_COUNT].Add(1);
        context.SS->IncrementPathDbRefCount(newTable->PathId, "new path created");

        if ((parentPath.Base()->IsDirectory() || parentPath.Base()->IsDomainRoot()) && parentPath.Base()->HasActiveChanges()) {
            TTxId parentTxId = parentPath.Base()->PlannedToCreate() ? parentPath.Base()->CreateTxId : parentPath.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::CreateParts);
        context.OnComplete.ActivateTx(OperationId);

        context.SS->PersistPath(db, newTable->PathId);
        context.SS->ApplyAndPersistUserAttrs(db, newTable->PathId);

        if (!acl.empty()) {
            newTable->ApplyACL(acl);
            context.SS->PersistACL(db, newTable);
        }
        context.SS->PersistTable(db, newTable->PathId);
        context.SS->PersistTxState(db, OperationId);

        context.SS->PersistUpdateNextPathId(db);
        context.SS->PersistUpdateNextShardIdx(db);
        // Persist new shards info
        for (const auto& shard : tableInfo->GetPartitions()) {
            Y_VERIFY(context.SS->ShardInfos.contains(shard.ShardIdx), "shard info is set before");
            auto tabletType = context.SS->ShardInfos[shard.ShardIdx].TabletType;
            const auto& bindedChannels = context.SS->ShardInfos[shard.ShardIdx].BindedChannels;
            context.SS->PersistShardMapping(db, shard.ShardIdx, InvalidTabletId, newTable->PathId, OperationId.GetTxId(), tabletType);
            context.SS->PersistChannelsBinding(db, shard.ShardIdx, bindedChannels);

            if (storePerShardConfig) {
                tableInfo->PerShardPartitionConfig[shard.ShardIdx].CopyFrom(perShardConfig);
                context.SS->PersistAddTableShardPartitionConfig(db, shard.ShardIdx, perShardConfig);
            }
        }

        if (parentPath.Base()->IsDirectory() || parentPath.Base()->IsDomainRoot()) {
            ++parentPath.Base()->DirAlterVersion;
            context.SS->PersistPathDirAlterVersion(db, parentPath.Base());
        }
        context.SS->ClearDescribePathCaches(parentPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, parentPath.Base()->PathId);

        context.SS->ClearDescribePathCaches(dstPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, dstPath.Base()->PathId);

        Y_VERIFY(shardsToCreate == txState.Shards.size());
        dstPath.DomainInfo()->IncPathsInside();
        dstPath.DomainInfo()->AddInternalShards(txState);

        dstPath.Base()->IncShardsInside(shardsToCreate);
        parentPath.Base()->IncAliveChildren();

        State = NextState();
        SetState(SelectStateFunc(State));
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TCreateTable");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateTable AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr {
namespace NSchemeShard {

ISubOperationBase::TPtr CreateNewTable(TOperationId id, const TTxTransaction& tx, const THashSet<TString>& localSequences) {
    auto obj = MakeHolder<TCreateTable>(id, tx);
    obj->SetLocalSequences(localSequences);
    return obj.Release();
}

ISubOperationBase::TPtr CreateNewTable(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return new TCreateTable(id, state);
}

ISubOperationBase::TPtr CreateInitializeBuildIndexImplTable(TOperationId id, const TTxTransaction& tx) {
    auto obj = MakeHolder<TCreateTable>(id, tx);
    obj->SetAllowShadowDataForBuildIndex();
    return obj.Release();
}

ISubOperationBase::TPtr CreateInitializeBuildIndexImplTable(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    auto obj = MakeHolder<TCreateTable>(id, state);
    obj->SetAllowShadowDataForBuildIndex();
    return obj.Release();
}

}
}
