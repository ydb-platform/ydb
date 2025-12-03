#include "schemeshard__shred_manager.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard__operation_states.h"
#include "schemeshard_cdc_stream_common.h"
#include "schemeshard_impl.h"
#include "schemeshard_tx_infly.h"
#include "schemeshard_utils.h"  // for TransactionTemplate

#include <ydb/core/base/subdomain.h>
#include <ydb/core/mind/hive/hive.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

void PrepareScheme(NKikimrSchemeOp::TTableDescription* schema, const TString& name, const TTableInfo::TPtr srcTableInfo, TOperationContext &context) {
    const NScheme::TTypeRegistry* typeRegistry = AppData(context.Ctx)->TypeRegistry;

    NKikimrSchemeOp::TTableDescription completedSchema;
    context.SS->DescribeTable(*srcTableInfo, typeRegistry, true, &completedSchema);
    completedSchema.SetName(name);

    //inherit all from Src except PartitionConfig, PartitionConfig could be altered
    completedSchema.MutablePartitionConfig()->CopyFrom(schema->GetPartitionConfig());
    schema->Swap(&completedSchema);
    schema->SetSystemColumnNamesAllowed(true);
}

void FillSrcSnapshot(const TTxState* const txState, ui64 dstDatashardId, NKikimrTxDataShard::TSendSnapshot& snapshot) {
    snapshot.SetTableId_Deprecated(txState->SourcePathId.LocalPathId);
    snapshot.MutableTableId()->SetOwnerId(txState->SourcePathId.OwnerId);
    snapshot.MutableTableId()->SetTableId(txState->SourcePathId.LocalPathId);
    snapshot.AddSendTo()->SetShard(dstDatashardId);
}

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCopyTable TConfigureParts"
                << " operationId# " << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, });
    }

    bool HandleReply(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 DebugHint() << " HandleReply TEvProposeTransactionResult"
                 << " at tablet# " << ssId
                 << " message# " << ev->Get()->Record.ShortDebugString());

        return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " ProgressState"
                     << " at tablet# " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        txState->ClearShardsInProgress();

        Y_ABORT_UNLESS(txState->SourcePathId != InvalidPathId);
        Y_ABORT_UNLESS(txState->TargetPathId != InvalidPathId);
        const TTableInfo::TPtr srcTableInfo = *context.SS->Tables.FindPtr(txState->SourcePathId);
        const TTableInfo::TPtr dstTableInfo = *context.SS->Tables.FindPtr(txState->TargetPathId);

        Y_ABORT_UNLESS(srcTableInfo->GetPartitions().size() == dstTableInfo->GetPartitions().size(),
                 "CopyTable partition counts don't match");
        const ui64 dstSchemaVersion = NEW_TABLE_ALTER_VERSION;

        for (ui32 i = 0; i < dstTableInfo->GetPartitions().size(); ++i) {
            TShardIdx srcShardIdx = srcTableInfo->GetPartitions()[i].ShardIdx;
            TTabletId srcDatashardId = context.SS->ShardInfos[srcShardIdx].TabletID;

            TShardIdx dstShardIdx = dstTableInfo->GetPartitions()[i].ShardIdx;
            TTabletId dstDatashardId = context.SS->ShardInfos[dstShardIdx].TabletID;

            auto seqNo = context.SS->StartRound(*txState);

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                      DebugHint() << " Propose modify scheme on dstDatashard# " << dstDatashardId
                        << " idx# " << dstShardIdx
                        << " srcDatashard# " << srcDatashardId
                        << " idx# " <<  srcShardIdx
                        << " operationId# " << OperationId
                        << " seqNo# " << seqNo
                        << " at tablet# " << ssId);

            // Send "CreateTable + ReceiveParts" transaction to destination datashard
            NKikimrTxDataShard::TFlatSchemeTransaction newShardTx;
            context.SS->FillSeqNo(newShardTx, seqNo);
            context.SS->FillTableDescription(txState->TargetPathId, i, dstSchemaVersion, newShardTx.MutableCreateTable());
            newShardTx.MutableReceiveSnapshot()->SetTableId_Deprecated(txState->TargetPathId.LocalPathId);
            newShardTx.MutableReceiveSnapshot()->MutableTableId()->SetOwnerId(txState->TargetPathId.OwnerId);
            newShardTx.MutableReceiveSnapshot()->MutableTableId()->SetTableId(txState->TargetPathId.LocalPathId);
            newShardTx.MutableReceiveSnapshot()->AddReceiveFrom()->SetShard(ui64(srcDatashardId));

            auto dstEvent = context.SS->MakeDataShardProposal(txState->TargetPathId, OperationId, newShardTx.SerializeAsString(), context.Ctx);
            if (const ui64 subDomainPathId = context.SS->ResolvePathIdForDomain(txState->TargetPathId).LocalPathId) {
                dstEvent->Record.SetSubDomainPathId(subDomainPathId);
            }
            context.OnComplete.BindMsgToPipe(OperationId, dstDatashardId, dstShardIdx, dstEvent.Release());

            // Send "SendParts" transaction to source datashard
            NKikimrTxDataShard::TFlatSchemeTransaction oldShardTx;
            context.SS->FillSeqNo(oldShardTx, seqNo);
            if (txState->CdcPathId != InvalidPathId) {
                auto& combined = *oldShardTx.MutableCreateIncrementalBackupSrc();
                FillSrcSnapshot(txState, ui64(dstDatashardId), *combined.MutableSendSnapshot());
                NCdcStreamAtTable::FillNotice(txState->CdcPathId, context, *combined.MutableCreateCdcStreamNotice());
            } else {
                FillSrcSnapshot(txState, ui64(dstDatashardId), *oldShardTx.MutableSendSnapshot());
                oldShardTx.SetReadOnly(true);
            }
            auto srcEvent = context.SS->MakeDataShardProposal(txState->TargetPathId, OperationId, oldShardTx.SerializeAsString(), context.Ctx);
            context.OnComplete.BindMsgToPipe(OperationId, srcDatashardId, srcShardIdx, srcEvent.Release());
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
                << "TCopyTable TPropose"
                << " operationId# " << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
            {TEvHive::TEvCreateTabletReply::EventType, TEvDataShard::TEvProposeTransactionResult::EventType});
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvDataShard::TEvSchemaChanged"
                               << " triggers early, save it"
                               << ", at schemeshard: " << ssId);

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", stepId: " << step
                               << ", at schemeshard" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        TTableInfo::TPtr table = context.SS->Tables[pathId];
        Y_ABORT_UNLESS(table);
        table->AlterVersion = NEW_TABLE_ALTER_VERSION;
        context.SS->PersistTableCreated(db, pathId);

        context.SS->TabletCounters->Simple()[COUNTER_TABLE_COUNT].Add(1);

        if (table->IsTTLEnabled() && !context.SS->TTLEnabledTables.contains(pathId)) {
            context.SS->TTLEnabledTables[pathId] = table;
            context.SS->TabletCounters->Simple()[COUNTER_TTL_ENABLED_TABLE_COUNT].Add(1);

            const auto now = context.Ctx.Now();
            for (auto& shard : table->GetPartitions()) {
                auto& lag = shard.LastCondEraseLag;
                Y_DEBUG_ABORT_UNLESS(!lag.Defined());

                lag = now - shard.LastCondErase;
                context.SS->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].IncrementFor(lag->Seconds());
            }
        }

        auto parentDir = context.SS->PathsById.at(path->ParentPathId); // TargetPathId has been created
        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
        context.SS->ClearDescribePathCaches(parentDir);
        context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);

        if (txState->CdcPathId != InvalidPathId) {
            TPathId srcPathId = txState->SourcePathId;

            Y_ABORT_UNLESS(context.SS->PathsById.contains(srcPathId));
            auto srcPath = context.SS->PathsById.at(srcPathId);

            Y_ABORT_UNLESS(context.SS->Tables.contains(srcPathId));
            auto srcTable = context.SS->Tables.at(srcPathId);

            srcTable->AlterVersion += 1;

            context.SS->PersistTableAlterVersion(db, srcPathId, srcTable);

            // Sync child indexes to match the new version
            NCdcStreamState::SyncChildIndexes(srcPath, srcTable->AlterVersion, OperationId, context, db);

            context.SS->ClearDescribePathCaches(srcPath);
            context.OnComplete.PublishToSchemeBoard(OperationId, srcPathId);
        }

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

class TCopyTable: public TSubOperation {

    THashSet<TString> LocalSequences;
    TMaybe<TPathElement::EPathState> TargetState;

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
            return TTxState::CopyTableBarrier;
        case TTxState::CopyTableBarrier:
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
            return MakeHolder<NTableState::TProposedWaitParts>(OperationId, TTxState::ETxState::CopyTableBarrier);
        case TTxState::CopyTableBarrier:
            return MakeHolder<TWaitCopyTableBarrier>(OperationId, "TCopyTable");
        case TTxState::Done:
            if (!TargetState) {
                return MakeHolder<TDone>(OperationId);
            } else {
                return MakeHolder<TDone>(OperationId, *TargetState);
            }
        default:
            return nullptr;
        }
    }

public:
    explicit TCopyTable(const TOperationId& id, TTxState::ETxState txState, TTxState* state)
        : TSubOperation(id, txState)
    {
        Y_ENSURE(state);
        TargetState = state->TargetPathTargetState;
    }

    explicit TCopyTable(const TOperationId& id, const TTxTransaction& tx, const THashSet<TString>& localSequences, TMaybe<TPathElement::EPathState> targetState)
        : TSubOperation(id, tx)
        , LocalSequences(localSequences)
        , TargetState(targetState)
    {
    }

    bool IsShadowDataAllowed() const {
        return AppData()->AllowShadowDataInSchemeShardForTests;
    }

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const TString& parentPath = Transaction.GetWorkingDir();
        const TString& name = Transaction.GetCreateTable().GetName();
        const auto acceptExisted = !Transaction.GetFailOnExist();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCopyTable Propose"
                         << ", path: " << parentPath << "/" << name
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        TPath parent = TPath::Resolve(parentPath, context.SS);
        {
            TPath::TChecker checks = parent.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .FailOnRestrictedCreateInTempZone(Transaction.GetAllowCreateInTempDir());

            if (checks) {
                if (parent.Base()->IsTableIndex()) {
                    checks
                        .IsInsideTableIndexPath()
                        .IsUnderCreating(NKikimrScheme::StatusNameConflict)
                        .IsUnderTheSameOperation(OperationId.GetTxId()); //allow only as part of copying base table
                } else {
                    checks
                        .IsCommonSensePath()
                        .IsLikeDirectory();

                    if (!Transaction.GetCreateTable().GetAllowUnderSameOperation()) {
                        checks.NotUnderOperation();
                    }
                }
            }

            if (checks && Transaction.HasCreateCdcStream()) {
                NCdcStreamAtTable::CheckWorkingDirOnPropose(
                    checks,
                    parent.IsTableIndex(Nothing(), false));
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TPath srcPath = TPath::Resolve(Transaction.GetCreateTable().GetCopyFromTable(), context.SS);

        {
            TPath::TChecker checks = srcPath.Check();
            checks
                .NotEmpty()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsTable();

            if (!Transaction.GetCreateTable().GetAllowUnderSameOperation()) {
                checks
                    .NotUnderTheSameOperation(OperationId.GetTxId())
                    .NotUnderOperation();
            }

            if (checks) {
                if (parent.Base()->IsTableIndex()) {
                    checks.IsInsideTableIndexPath(); //copy imp index table as index index table, not a separate one
                } else {
                    // Allow copying index impl tables when feature flag is enabled
                    if (!srcPath.ShouldSkipCommonPathCheckForIndexImplTable()) {
                        checks.IsCommonSensePath();
                    }
                }
            }

            if (checks && Transaction.HasCreateCdcStream()) {
                NCdcStreamAtTable::CheckSrcDirOnPropose(
                    checks,
                    srcPath.IsInsideTableIndexPath(false));
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const ui32 maxShardsToCreate = srcPath.Shards();
        const TString acl = Transaction.GetModifyACL().GetDiffACL();

        auto schema = Transaction.GetCreateTable();
        const bool isBackup = schema.GetIsBackup();

        TPath dstPath = parent.Child(name);
        {
            TPath::TChecker checks = dstPath.Check();
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
                if (!parent.Base()->IsTableIndex() && !isBackup) {
                    checks.DepthLimit();
                }

                checks
                    .IsValidLeafName(context.UserToken.Get())
                    .IsTheSameDomain(srcPath)
                    .PathShardsLimit(maxShardsToCreate)
                    .IsValidACL(acl);
            }

            if (checks && !isBackup) {
                checks
                    .PathsLimit()
                    .DirChildrenLimit()
                    .ShardsLimit(maxShardsToCreate);
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        // TODO: cdc checks

        auto domainInfo = parent.DomainInfo();
        bool transactionSupport = domainInfo->IsSupportTransactions();
        if (domainInfo->GetAlter()) {
            TPathId domainPathId = parent.GetPathIdForDomain();
            Y_ABORT_UNLESS(context.SS->PathsById.contains(domainPathId));
            TPathElement::TPtr domainPath = context.SS->PathsById.at(domainPathId);
            Y_ABORT_UNLESS(domainPath->PlannedToCreate() || domainPath->HasActiveChanges());

            transactionSupport |= domainInfo->GetAlter()->IsSupportTransactions();
        }
        if (!transactionSupport) {
            result->SetError(NKikimrScheme::StatusNameConflict, "Inclusive subDomain do not support shared transactions");
            return result;
        }

        TString errStr;

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (!isBackup && !context.SS->CheckLocks(srcPath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        Y_ABORT_UNLESS(context.SS->Tables.contains(srcPath.Base()->PathId));
        TTableInfo::TPtr srcTableInfo = context.SS->Tables.at(srcPath.Base()->PathId);

        // do not allow copy from table with enabled external blobs
        {
            const NKikimrSchemeOp::TPartitionConfig &srcPartitionConfig = srcTableInfo->PartitionConfig();
            if (PartitionConfigHasExternalBlobsEnabled(srcPartitionConfig)) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed, "source table contains external blobs, copy operation is not safe so prohibited");
                return result;
            }
            if (srcPartitionConfig.GetShadowData()) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed, "Cannot copy tables with enabled ShadowData");
                return result;
            }
        }

        const bool omitFollowers = schema.GetOmitFollowers();

        PrepareScheme(&schema, name, srcTableInfo, context);
        schema.SetIsBackup(isBackup);

        if (omitFollowers) {
            schema.MutablePartitionConfig()->AddFollowerGroups()->Clear();
        }

        if (isBackup) {
            schema.ClearTTLSettings();
        }

        // replication config is not copied
        schema.ClearReplicationConfig();

        // incr backup config is not copied
        schema.ClearIncrementalBackupConfig();

        const bool isServerless = context.SS->IsServerlessDomain(TPath::Init(context.SS->RootPathId(), context.SS));

        NKikimrSchemeOp::TPartitionConfig compilationPartitionConfig;
        if (!TPartitionConfigMerger::ApplyChanges(compilationPartitionConfig, srcTableInfo->PartitionConfig(), schema.GetPartitionConfig(), schema.GetColumns(), AppData(), isServerless, errStr)
            || !TPartitionConfigMerger::VerifyCreateParams(compilationPartitionConfig, AppData(), IsShadowDataAllowed(), errStr)) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }
        schema.MutablePartitionConfig()->CopyFrom(compilationPartitionConfig);

        const NScheme::TTypeRegistry* typeRegistry = AppData()->TypeRegistry;
        const TSchemeLimits& limits = domainInfo->GetSchemeLimits();
        // Copy table should not check feature flags for columns types.
        // If the types in original table are created then they should be allowed in destination table.
        const TTableInfo::TCreateAlterDataFeatureFlags featureFlags = {
            .EnableTablePgTypes = true,
            .EnableTableDatetime64 = true,
            .EnableParameterizedDecimal = true,
        };
        TTableInfo::TAlterDataPtr alterData = TTableInfo::CreateAlterData(nullptr, schema, *typeRegistry,
            limits, *domainInfo, featureFlags, errStr, LocalSequences);
        if (!alterData.Get()) {
            result->SetError(NKikimrScheme::StatusSchemeError, errStr);
            return result;
        }

        TTableInfo::TPtr tableInfo = new TTableInfo(std::move(*alterData));
        alterData.Reset();

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

        auto guard = context.DbGuard();
        TPathId allocatedPathId = context.SS->AllocatePathId();
        context.MemChanges.GrabNewPath(context.SS, allocatedPathId);
        context.MemChanges.GrabPath(context.SS, parent.Base()->PathId);
        context.MemChanges.GrabPath(context.SS, srcPath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabDomain(context.SS, parent.GetPathIdForDomain());
        context.MemChanges.GrabNewTable(context.SS, allocatedPathId);

        context.DbChanges.PersistPath(allocatedPathId);
        context.DbChanges.PersistPath(parent.Base()->PathId);
        context.DbChanges.PersistPath(srcPath.Base()->PathId);
        context.DbChanges.PersistApplyUserAttrs(allocatedPathId);
        context.DbChanges.PersistTable(allocatedPathId);
        context.DbChanges.PersistTxState(OperationId);

        // copy attrs without any checks
        TUserAttributes::TPtr userAttrs = new TUserAttributes(1);
        userAttrs->Attrs = srcPath.Base()->UserAttrs->Attrs;

        dstPath.MaterializeLeaf(owner, allocatedPathId);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        TPathElement::TPtr newTable = dstPath.Base();
        newTable->CreateTxId = OperationId.GetTxId();
        newTable->LastTxId = OperationId.GetTxId();
        newTable->PathState = TPathElement::EPathState::EPathStateCreate;
        newTable->PathType = TPathElement::EPathType::EPathTypeTable;
        newTable->UserAttrs->AlterData = userAttrs;

        srcPath.Base()->PathState = TPathElement::EPathState::EPathStateCopying;
        srcPath.Base()->LastTxId = OperationId.GetTxId();

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCopyTable, newTable->PathId, srcPath.Base()->PathId);
        txState.State = TTxState::CreateParts;
        if (Transaction.HasCreateCdcStream()) {
            txState.CdcPathId = srcPath.Base()->PathId;
        }
        if (Transaction.GetCreateTable().HasPathState()) {
            txState.TargetPathTargetState = Transaction.GetCreateTable().GetPathState();
        }

        TShardInfo datashardInfo = TShardInfo::DataShardInfo(OperationId.GetTxId(), newTable->PathId);
        datashardInfo.BindedChannels = channelsBinding;
        auto newPartition = NTableState::ApplyPartitioningCopyTable(datashardInfo, srcTableInfo, txState, context.SS);
        TVector<TShardIdx> newShardsIdx;
        newShardsIdx.reserve(newPartition.size());
        for (const auto& part: newPartition) {
            context.MemChanges.GrabNewShard(context.SS, part.ShardIdx);
            context.DbChanges.PersistShard(part.ShardIdx);
            newShardsIdx.push_back(part.ShardIdx);
        }
        context.SS->SetPartitioning(newTable->PathId, tableInfo, std::move(newPartition));
        if (context.SS->EnableShred && context.SS->ShredManager->GetStatus() == EShredStatus::IN_PROGRESS) {
            context.OnComplete.Send(context.SS->SelfId(), new TEvPrivate::TEvAddNewShardToShred(std::move(newShardsIdx)));
        }
        for (const auto& shard : tableInfo->GetPartitions()) {
            Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shard.ShardIdx), "shard info is set before");
            if (storePerShardConfig) {
                tableInfo->PerShardPartitionConfig[shard.ShardIdx].CopyFrom(perShardConfig);
            }
        }

        Y_ABORT_UNLESS(tableInfo->GetPartitions().back().EndOfRange.empty(), "End of last range must be +INF");

        context.SS->Tables[newTable->PathId] = tableInfo;
        context.SS->IncrementPathDbRefCount(newTable->PathId);

        if (parent.Base()->HasActiveChanges()) {
            TTxId parentTxId = parent.Base()->PlannedToCreate() ? parent.Base()->CreateTxId : parent.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        // Add dependencies on in-flight split operations for source table in case of CopyTable
        Y_ABORT_UNLESS(txState.SourcePathId != InvalidPathId);
        for (auto splitTx: context.SS->Tables.at(srcPath.Base()->PathId)->GetSplitOpsInFlight()) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TCopyTable Propose "
                            << " opId: " << OperationId
                            << " wait split ops in flight on src table " << splitTx);
            context.OnComplete.Dependence(splitTx.GetTxId(), OperationId.GetTxId());
        }

        context.OnComplete.ActivateTx(OperationId);

        ++parent.Base()->DirAlterVersion;
        context.SS->ClearDescribePathCaches(parent.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, parent.Base()->PathId);

        context.SS->ClearDescribePathCaches(newTable);
        context.OnComplete.PublishToSchemeBoard(OperationId, newTable->PathId);

        const ui32 shardsToCreate = tableInfo->GetPartitions().size();
        Y_VERIFY_S(shardsToCreate <= maxShardsToCreate, "shardsToCreate: " << shardsToCreate << " maxShardsToCreate: " << maxShardsToCreate);

        dstPath.DomainInfo()->IncPathsInside(context.SS, 1, isBackup);
        dstPath.DomainInfo()->AddInternalShards(txState, context.SS, isBackup);
        dstPath.Base()->IncShardsInside(shardsToCreate);
        IncAliveChildrenSafeWithUndo(OperationId, parent, context, isBackup);

        LOG_TRACE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TCopyTable Propose creating new table"
                << " opId# " << OperationId
                << " srcPath# " << srcPath.PathString()
                << " srcPathId# " << srcPath.Base()->PathId
                << " path# " << dstPath.PathString()
                << " pathId# " << newTable->PathId
                << " withNewCdc# " << (Transaction.HasCreateCdcStream() ? "true" : "false")
                << " schemeshard# " << ssId
                << " tx# " << Transaction.DebugString()
                );

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCopyTable AbortPropose"
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << context.SS->TabletID());
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCopyTable AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        //txState->TargetPathId should be marked as drop by forceDropTxId

        TPathId srcPathId = txState->SourcePathId;
        Y_ABORT_UNLESS(srcPathId != InvalidPathId);
        Y_ABORT_UNLESS(context.SS->PathsById.contains(srcPathId));
        TPathElement::TPtr srcPath = context.SS->PathsById.at(srcPathId);
        srcPath->PathState = TPathElement::EPathState::EPathStateDrop;

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateCopyTable(TOperationId id, const TTxTransaction& tx, const THashSet<TString>& localSequences, TMaybe<TPathElement::EPathState> targetState)
{
    return MakeSubOperation<TCopyTable>(id, tx, localSequences, targetState);
}

ISubOperation::TPtr CreateCopyTable(TOperationId id, TTxState::ETxState txState, TTxState* state) {
    Y_ABORT_UNLESS(txState != TTxState::Invalid);
    return MakeSubOperation<TCopyTable>(id, txState, state);
}

TVector<ISubOperation::TPtr> CreateCopyTable(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);

    auto copying = tx.GetCreateTable();
    Y_ABORT_UNLESS(copying.HasCopyFromTable());
    auto cdcPeerOp = tx.HasCreateCdcStream() ? &tx.GetCreateCdcStream() : nullptr;

    TPath srcPath = TPath::Resolve(copying.GetCopyFromTable(), context.SS);

    {
        TPath::TChecker checks = srcPath.Check();
        checks.NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .NotUnderDeleting()
            .IsTable();

        // Allow copying index impl tables when feature flag is enabled
        if (checks && !srcPath.ShouldSkipCommonPathCheckForIndexImplTable()) {
            checks.IsCommonSensePath();
        }

        if (!copying.GetAllowUnderSameOperation()) {
            checks.NotUnderOperation();
        }

        if (!checks) {
            return {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
        }
    }

    THashSet<TString> sequences = GetLocalSequences(context, srcPath);

    TPath workDir = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    TPath dstPath = workDir.Child(copying.GetName());

    TVector<ISubOperation::TPtr> result;
    {
        auto schema = TransactionTemplate(tx.GetWorkingDir(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
        schema.SetFailOnExist(tx.GetFailOnExist());

        auto operation = schema.MutableCreateTable();
        operation->SetName(copying.GetName());
        operation->SetCopyFromTable(copying.GetCopyFromTable());
        operation->SetOmitFollowers(copying.GetOmitFollowers());
        operation->SetIsBackup(copying.GetIsBackup());
        operation->MutablePartitionConfig()->CopyFrom(copying.GetPartitionConfig());
        if (cdcPeerOp) {
            schema.MutableCreateCdcStream()->CopyFrom(*cdcPeerOp);
        }

        result.push_back(CreateCopyTable(NextPartId(nextId, result), schema, sequences));
    }

    // Process indexes: always create index structure, but skip impl table copies if OmitIndexes is set
    // (impl tables are handled separately by CreateConsistentCopyTables for incremental backups with CDC)
    for (auto& child: srcPath.Base()->GetChildren()) {
        auto name = child.first;
        auto pathId = child.second;

        TPath childPath = srcPath.Child(name);
        if (childPath.IsDeleted()) {
            continue;
        }

        if (childPath.IsSequence()) {
            continue;
        }

        if (!childPath.IsTableIndex()) {
            continue;
        }

        Y_ABORT_UNLESS(childPath.Base()->PathId == pathId);

        TTableIndexInfo::TPtr indexInfo = context.SS->Indexes.at(pathId);
        {
            auto schema = TransactionTemplate(dstPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex);
            schema.SetFailOnExist(tx.GetFailOnExist());

            auto operation = schema.MutableCreateTableIndex();
            operation->SetName(name);
            operation->SetType(indexInfo->Type);
            for (const auto& keyName: indexInfo->IndexKeys) {
                *operation->MutableKeyColumnNames()->Add() = keyName;
            }
            for (const auto& dataColumn: indexInfo->IndexDataColumns) {
                *operation->MutableDataColumnNames()->Add() = dataColumn;
            }
            if (indexInfo->Type == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree) {
                *operation->MutableVectorIndexKmeansTreeDescription() =
                    std::get<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>(indexInfo->SpecializedIndexDescription);
            } else if (!std::holds_alternative<std::monostate>(indexInfo->SpecializedIndexDescription)) {
                return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter,
                                     TStringBuilder{} << "Copy table doesn't support table with index type " << indexInfo->Type)};
            }

            result.push_back(CreateNewTableIndex(NextPartId(nextId, result), schema));
        }

        // Skip impl table copies if OmitIndexes is set (handled by CreateConsistentCopyTables for incremental backups)
        if (copying.GetOmitIndexes()) {
            continue;
        }

        for (const auto& [implTableName, implTablePathId] : childPath.Base()->GetChildren()) {
            TPath implTable = childPath.Child(implTableName);
            Y_ABORT_UNLESS(implTable.Base()->PathId == implTablePathId);

            NKikimrSchemeOp::TModifyScheme schema;
            schema.SetFailOnExist(tx.GetFailOnExist());
            schema.SetWorkingDir(JoinPath({dstPath.PathString(), name}));
            schema.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);

            auto operation = schema.MutableCreateTable();
            operation->SetName(implTableName);
            operation->SetCopyFromTable(implTable.PathString());
            operation->SetOmitFollowers(copying.GetOmitFollowers());
            operation->SetIsBackup(copying.GetIsBackup());

            result.push_back(CreateCopyTable(NextPartId(nextId, result), schema, GetLocalSequences(context, implTable)));
            AddCopySequences(nextId, tx, context, result, implTable, JoinPath({dstPath.PathString(), name, implTableName}));
        }
    }

    AddCopySequences(nextId, tx, context, result, srcPath, dstPath.PathString());
    return result;
}

}
