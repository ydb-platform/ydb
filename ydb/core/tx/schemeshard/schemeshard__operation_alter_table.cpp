#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

bool CheckFreezeStateAlreadySet(const TTableInfo::TPtr table, const NKikimrSchemeOp::TTableDescription& alter) {
    if (!alter.HasPartitionConfig())
        return false;
    if (alter.GetPartitionConfig().HasFreezeState()) {
        if (table->PartitionConfig().HasFreezeState()) {
            auto tableFreezeState = table->PartitionConfig().GetFreezeState();
            if (tableFreezeState == alter.GetPartitionConfig().GetFreezeState()) {
                return true;
            }
        } else {
            if (alter.GetPartitionConfig().GetFreezeState() == NKikimrSchemeOp::EFreezeState::Unfreeze) {
                return true;
            }
        }
    }

    return false;
}

bool IsSuperUser(const NACLib::TUserToken* userToken) {
    if (!userToken)
        return false;

    const auto& adminSids = AppData()->AdministrationAllowedSIDs;
    auto hasSid = [userToken](const TString& sid) -> bool {
        return userToken->IsExist(sid);
    };
    auto it = std::find_if(adminSids.begin(), adminSids.end(), hasSid);
    return (it != adminSids.end());
}

template <typename TMessage>
bool CheckAllowedFields(const TMessage& message, THashSet<TString>&& allowedFields) {
    std::vector<const google::protobuf::FieldDescriptor*> fields;
    message.GetReflection()->ListFields(message, &fields);
    for (const auto* field : fields) {
        if (!allowedFields.contains(field->name())) {
            return false;
        }
    }
    return true;
}

TTableInfo::TAlterDataPtr ParseParams(const TPath& path, TTableInfo::TPtr table, const NKikimrSchemeOp::TTableDescription& alter,
                                      const bool shadowDataAllowed, const THashSet<TString>& localSequences,
                                      TString& errStr, NKikimrScheme::EStatus& status, TOperationContext& context) {
    const TAppData* appData = AppData(context.Ctx);

    if (!path.IsCommonSensePath()) {
        if (alter.ColumnsSize() != 0 || alter.DropColumnsSize() != 0) {
            errStr = "Adding or dropping columns in index table is not supported";
            status = NKikimrScheme::StatusInvalidParameter;
            return nullptr;
        }

        if (alter.HasTTLSettings()) {
            errStr = "TTL on index table is not supported";
            status = NKikimrScheme::StatusInvalidParameter;
            return nullptr;
        }
    }

    auto copyAlter = alter;

    const bool hasSchemaChanges = (
            copyAlter.ColumnsSize() != 0 ||
            copyAlter.DropColumnsSize() != 0);

    if (copyAlter.HasIsBackup() && copyAlter.GetIsBackup() !=  table->IsBackup) {
        errStr = Sprintf("Cannot add/remove 'IsBackup' property");
        status = NKikimrScheme::StatusInvalidParameter;
        return nullptr;
    }

    if (!hasSchemaChanges
        && !copyAlter.HasPartitionConfig()
        && !copyAlter.HasTTLSettings()
        && !copyAlter.HasReplicationConfig())
    {
        errStr = Sprintf("No changes specified");
        status = NKikimrScheme::StatusInvalidParameter;
        return nullptr;
    }

    if (copyAlter.HasPartitionConfig() && copyAlter.GetPartitionConfig().HasFreezeState()) {
        if (hasSchemaChanges) {
            errStr = Sprintf("Mix freeze cmd with other options is forbidden");
            status = NKikimrScheme::StatusInvalidParameter;
            return nullptr;
        }
    }

    // Ignore column ids if they were passed by user!
    for (auto& col : *copyAlter.MutableColumns()) {
        bool hasDefault = col.HasDefaultFromLiteral();
        if (hasDefault && !context.SS->EnableAddColumsWithDefaults) {
            errStr = Sprintf("Column addition with default value is not supported now.");
            status = NKikimrScheme::StatusInvalidParameter;
            return nullptr;
        }

        if (col.GetNotNull() && !hasDefault) {
            errStr = Sprintf("Not null columns without defaults are not supported.");
            status = NKikimrScheme::StatusInvalidParameter;
            return nullptr;
        }

        col.ClearId();
    }

    for (auto& col : *copyAlter.MutableDropColumns()) {
        if (col.GetName().empty()) {
            errStr = Sprintf("Must specify name for the column to drop");
            status = NKikimrScheme::StatusInvalidParameter;
            return nullptr;
        }
        col.ClearId();
    }

    if (CheckFreezeStateAlreadySet(table, copyAlter)) {
        errStr = Sprintf("Requested freeze state already set");
        status = NKikimrScheme::StatusAlreadyExists;
        return nullptr;
    }

    NKikimrSchemeOp::TPartitionConfig compilationPartitionConfig;
    if (!TPartitionConfigMerger::ApplyChanges(compilationPartitionConfig, table->PartitionConfig(), copyAlter.GetPartitionConfig(), appData, errStr)
        || !TPartitionConfigMerger::VerifyAlterParams(table->PartitionConfig(), compilationPartitionConfig, appData, shadowDataAllowed, errStr)) {
        status = NKikimrScheme::StatusInvalidParameter;
        return nullptr;
    }
    copyAlter.MutablePartitionConfig()->CopyFrom(compilationPartitionConfig);

    const TSubDomainInfo& subDomain = *path.DomainInfo();
    const TSchemeLimits& limits = subDomain.GetSchemeLimits();


    TTableInfo::TAlterDataPtr alterData = TTableInfo::CreateAlterData(
        table, copyAlter, *appData->TypeRegistry, limits, subDomain,
        context.SS->EnableTablePgTypes, context.SS->EnableTableDatetime64, errStr, localSequences);
    if (!alterData) {
        status = NKikimrScheme::StatusInvalidParameter;
        return nullptr;
    }

    return alterData;
}

void PrepareChanges(TOperationId opId, TPathElement::TPtr path, TTableInfo::TPtr table, const TBindingsRoomsChanges& bindingChanges, TOperationContext& context) {

    path->LastTxId = opId.GetTxId();
    path->PathState = TPathElement::EPathState::EPathStateAlter;

    TTxState& txState = context.SS->CreateTx(opId, TTxState::TxAlterTable, path->PathId);
    txState.State = TTxState::CreateParts;

    NIceDb::TNiceDb db(context.GetDB());

    TTxState::ETxState commonShardOp = table->NeedRecreateParts()
            ? TTxState::CreateParts
            : TTxState::ConfigureParts;

    txState.Shards.reserve(table->GetPartitions().size());
    for (const auto& shard : table->GetPartitions()) {
        auto shardIdx = shard.ShardIdx;
        TShardInfo& shardInfo = context.SS->ShardInfos[shardIdx];

        auto shardOp = commonShardOp;

        auto it = bindingChanges.find(GetPoolsMapping(shardInfo.BindedChannels));
        if (it != bindingChanges.end()) {
            if (it->second.ChannelsBindingsUpdated) {
                // We must recreate this shard to apply new channel bindings
                shardOp = TTxState::CreateParts;
                shardInfo.BindedChannels = it->second.ChannelsBindings;
                context.SS->PersistChannelsBinding(db, shardIdx, shardInfo.BindedChannels);
            }

            table->PerShardPartitionConfig[shardIdx].CopyFrom(it->second.PerShardConfig);
            context.SS->PersistAddTableShardPartitionConfig(db, shardIdx, it->second.PerShardConfig);
        }

        txState.Shards.emplace_back(shardIdx, ETabletType::DataShard, shardOp);

        shardInfo.CurrentTxId = opId.GetTxId();
        context.SS->PersistShardTx(db, shardIdx, opId.GetTxId());
    }

    context.SS->PersistAddAlterTable(db, path->PathId, table->AlterData);
    context.SS->PersistTxState(db, opId);


    for (auto splitTx: table->GetSplitOpsInFlight()) {
        context.OnComplete.Dependence(splitTx.GetTxId(), opId.GetTxId());
    }

    context.OnComplete.ActivateTx(opId);
}

bool CheckDroppingColumns(const TSchemeShard* ss, const NKikimrSchemeOp::TTableDescription& alter, const TPath& tablePath, TString& errStr) {
    TSet<TString> deletedColumns;

    for (const auto& colDescr: alter.GetDropColumns()) {
        if (colDescr.GetName()) {
            deletedColumns.insert(colDescr.GetName());
        }
    }

    for (const auto& child : tablePath.Base()->GetChildren()) {
        const auto& childName = child.first;
        const auto& childPathId = child.second;

        auto childPath = ss->PathsById.at(childPathId);
        if (!childPath->IsTableIndex() || childPath->Dropped()) {
            continue;
        }

        const TTableIndexInfo::TPtr indexInfo = ss->Indexes.at(childPathId);
        for (const auto& indexKey: indexInfo->IndexKeys) {
            if (deletedColumns.contains(indexKey)) {
                errStr = TStringBuilder ()
                    << "Impossible drop column because table has an index with that column"
                    << ", column name: " << indexKey
                    << ", table name: " << tablePath.PathString()
                    << ", index name: " << childName;
                return false;
            }
        }

        for (const auto& col: indexInfo->IndexDataColumns) {
            if (deletedColumns.contains(col)) {
                errStr = TStringBuilder ()
                    << "Impossible drop column because table index covers that column"
                    << ", column name: " << col
                    << ", table name: " << tablePath.PathString()
                    << ", index name: " << childName;
                return false;
            }
        }
    }

    return true;
}

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TAlterTable TConfigureParts"
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
                               << ", at schemeshard: " << ssId
                               << " message# " << ev->Get()->Record.ShortDebugString());

        return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterTable);

        txState->ClearShardsInProgress();

        for (ui32 i = 0; i < txState->Shards.size(); ++i) {
            auto idx = txState->Shards[i].Idx;
            auto datashardId = context.SS->ShardInfos[idx].TabletID;

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Propose modify scheme on datashard " << datashardId << " txid: " << OperationId << " at schemeshard" << ssId);

            const auto seqNo = context.SS->StartRound(*txState);
            const auto txBody = context.SS->FillAlterTableTxBody(txState->TargetPathId, idx, seqNo);
            auto event = context.SS->MakeDataShardProposal(txState->TargetPathId, OperationId, txBody, context.Ctx);
            context.OnComplete.BindMsgToPipe(OperationId, datashardId, idx, event.Release());
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
                << "TAlterTable TPropose"
                << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvDataShard::TEvProposeTransactionResult::EventType});
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        const auto& evRecord = ev->Get()->Record;

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvSchemaChanged"
                               << " triggers early"
                               << ", at schemeshard: " << ssId
                               << " message# " << evRecord.ShortDebugString());

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", operationId: " << OperationId
                               << ", stepId: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterTable);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        TTableInfo::TPtr table = context.SS->Tables.at(pathId);
        table->FinishAlter();

        auto ttlIt = context.SS->TTLEnabledTables.find(pathId);
        if (table->IsTTLEnabled() && ttlIt == context.SS->TTLEnabledTables.end()) {
            context.SS->TTLEnabledTables[pathId] = table;
            context.SS->TabletCounters->Simple()[COUNTER_TTL_ENABLED_TABLE_COUNT].Add(1);

            const auto now = context.Ctx.Now();
            for (auto& shard : table->GetPartitions()) {
                auto& lag = shard.LastCondEraseLag;
                Y_DEBUG_ABORT_UNLESS(!lag.Defined());

                lag = now - shard.LastCondErase;
                context.SS->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].IncrementFor(lag->Seconds());
            }
        } else if (!table->IsTTLEnabled() && ttlIt != context.SS->TTLEnabledTables.end()) {
            context.SS->TTLEnabledTables.erase(ttlIt);
            context.SS->TabletCounters->Simple()[COUNTER_TTL_ENABLED_TABLE_COUNT].Sub(1);

            for (auto& shard : table->GetPartitions()) {
                if (auto& lag = shard.LastCondEraseLag) {
                    context.SS->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].DecrementFor(lag->Seconds());
                    lag.Clear();
                } else {
                    Y_DEBUG_ABORT_UNLESS(false);
                }
            }
        }

        context.SS->PersistTableAltered(db, pathId, table);

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterTable);

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

class TAlterTable: public TSubOperation {
    bool AllowShadowData = false;

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
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return MakeHolder<TCreateParts>(OperationId);
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigureParts>(OperationId);
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
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

    void SetAllowShadowDataForBuildIndex() {
        AllowShadowData = true;
    }

    bool IsShadowDataAllowed() const {
        return AllowShadowData || AppData()->AllowShadowDataInSchemeShardForTests;
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& alter = Transaction.GetAlterTable();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = alter.GetName();

        TPathId pathId;
        if (alter.HasId_Deprecated() || alter.HasPathId()) {
            pathId = alter.HasPathId()
                ? PathIdFromPathId(alter.GetPathId())
                : context.SS->MakeLocalId(alter.GetId_Deprecated());
        }

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterTable Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", pathId: " << pathId
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        if (!alter.HasName() && !pathId) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "No table name or pathId in Alter");
            return result;
        }

        TPath path = pathId
            ? TPath::Init(pathId, context.SS)
            : TPath::Resolve(parentPathStr, context.SS).Dive(name);
        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsTable()
                .NotUnderOperation();

            if (!Transaction.GetInternal()) {
                checks.NotAsyncReplicaTable();
            }

            if (!context.IsAllowedPrivateTables) {
                checks.IsCommonSensePath(); //forbid alter impl index tables outside consistent operation
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        THashSet<TString> localSequences;

        for (const auto& column: alter.GetColumns()) {
            if (column.HasDefaultFromSequence()) {
                TString defaultFromSequence = column.GetDefaultFromSequence();

                const auto sequencePath = TPath::Resolve(defaultFromSequence, context.SS);
                {
                    const auto checks = sequencePath.Check();
                    checks
                        .NotEmpty()
                        .NotUnderDomainUpgrade()
                        .IsAtLocalSchemeShard()
                        .IsResolved()
                        .NotDeleted()
                        .IsSequence()
                        .NotUnderDeleting()
                        .NotUnderOperation();

                    if (!checks) {
                        result->SetError(checks.GetStatus(), checks.GetError());
                        return result;
                    }
                }

                localSequences.insert(sequencePath.PathString());
            }
        }

        TString errStr;

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (!context.SS->CheckLocks(path.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        Y_ABORT_UNLESS(context.SS->Tables.contains(path.Base()->PathId));
        TTableInfo::TPtr table = context.SS->Tables.at(path.Base()->PathId);

        if (table->AlterVersion == 0) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "Table is not created yet");
            return result;
        }
        if (table->AlterData) {
            auto lastOpId = TOperationId(path.Base()->LastTxId, 0);
            Y_ABORT_UNLESS(context.SS->TxInFlight.contains(lastOpId), "AlterData without Alter tx");
            result->SetError(NKikimrScheme::StatusMultipleModifications, "There's another Alter in flight");
            return result;
        }

        bool isReplicated = false;
        if (path.Base()->GetAliveChildren()) {
            for (const auto& [_, childPathId] : path.Base()->GetChildren()) {
                Y_ABORT_UNLESS(context.SS->PathsById.contains(childPathId));
                auto childPath = context.SS->PathsById.at(childPathId);

                if (!childPath->IsCdcStream() || childPath->Dropped()) {
                    continue;
                }

                if (isReplicated = childPath->AsyncReplication.IsDefined()) {
                    break;
                }
            }
        }

        NKikimrScheme::EStatus status;
        TTableInfo::TAlterDataPtr alterData = ParseParams(
            path, table, alter, IsShadowDataAllowed(), localSequences, errStr, status, context);
        if (!alterData) {
            result->SetError(status, errStr);
            return result;
        }

        Y_ABORT_UNLESS(alterData->AlterVersion == table->AlterVersion + 1);

        if (!CheckDroppingColumns(context.SS, alter, path, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (isReplicated) {
            for (const auto& [id, column] : alterData->Columns) {
                if (column.CreateVersion == alterData->AlterVersion) {
                    result->SetError(NKikimrScheme::StatusPreconditionFailed, "Cannot add columns to replicated table");
                    return result;
                }
                if (column.DeleteVersion == alterData->AlterVersion) {
                    result->SetError(NKikimrScheme::StatusPreconditionFailed, "Cannot drop columns of replicated table");
                    return result;
                }
            }
        }

        TBindingsRoomsChanges bindingChanges;

        if (context.SS->IsStorageConfigLogic(table)) {
            if (!context.SS->GetBindingsRoomsChanges(path.GetPathIdForDomain(), table->GetPartitions(), alterData->PartitionConfigFull(), bindingChanges, errStr)) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                return result;
            }
        }

        table->PrepareAlter(alterData);
        PrepareChanges(OperationId, path.Base(), table, bindingChanges, context);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TAlterTable");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterTable AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterTable>(id, tx);
}

ISubOperation::TPtr CreateAlterTable(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAlterTable>(id, state);
}

ISubOperation::TPtr CreateFinalizeBuildIndexImplTable(TOperationId id, const TTxTransaction& tx) {
    auto obj = MakeHolder<TAlterTable>(id, tx);
    obj->SetAllowShadowDataForBuildIndex();
    return obj.Release();
}

ISubOperation::TPtr CreateFinalizeBuildIndexImplTable(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    auto obj = MakeHolder<TAlterTable>(id, state);
    obj->SetAllowShadowDataForBuildIndex();
    return obj.Release();
}

TVector<ISubOperation::TPtr> CreateConsistentAlterTable(TOperationId id, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable);

    const auto& alter = tx.GetAlterTable();

    const TString& parentPathStr = tx.GetWorkingDir();
    const TString& name = alter.GetName();

    TPathId pathId = alter.HasPathId() ? PathIdFromPathId(alter.GetPathId()) : InvalidPathId;

    if (!alter.HasName() && !pathId) {
        return {CreateAlterTable(id, tx)};
    }

    TPath path = pathId
        ? TPath::Init(pathId, context.SS)
        : TPath::Resolve(parentPathStr, context.SS).Dive(name);

    if (!path.IsResolved()) {
        return {CreateAlterTable(id, tx)};
    }

    if (!path->IsTable()) {
        if (path->IsColumnTable()) {
            return {CreateAlterColumnTable(id, tx)};
        }
        return {CreateAlterTable(id, tx)};
    }

    if (path.IsCommonSensePath()) {
        return {CreateAlterTable(id, tx)};
    }

    TPath parent = path.Parent();

    if (!parent.IsTableIndex()) {
        return {CreateAlterTable(id, tx)};
    }

    // Admins can alter indexImplTable unconditionally.
    // Regular users can only alter allowed fields.
    if (!IsSuperUser(context.UserToken.Get())
        && (!CheckAllowedFields(alter, {"Name", "PathId", "PartitionConfig", "ReplicationConfig"})
            || (alter.HasPartitionConfig()
                && !CheckAllowedFields(alter.GetPartitionConfig(), {"PartitioningPolicy"})
            )
        )
    ) {
        return {CreateAlterTable(id, tx)};
    }

    TVector<ISubOperation::TPtr> result;

    {
        auto tableIndexAltering = TransactionTemplate(parent.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpAlterTableIndex);
        tableIndexAltering.SetInternal(tx.GetInternal());
        auto alterIndex = tableIndexAltering.MutableAlterTableIndex();
        alterIndex->SetName(parent.LeafName());
        alterIndex->SetState(NKikimrSchemeOp::EIndexState::EIndexStateReady);

        result.push_back(CreateAlterTableIndex(NextPartId(id, result), tableIndexAltering));
    }

    result.push_back(CreateAlterTable(NextPartId(id, result), tx));


    return result;
}

}
