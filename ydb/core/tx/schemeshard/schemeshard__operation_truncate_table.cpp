#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"

#include "schemeshard_impl.h"

#include <ydb/core/base/auth.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/subdomain.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TTruncateTable TConfigureParts"
            << " operationId# " << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    template<typename TEvent>
    bool HandleReplyImpl(TEvent& ev, TOperationContext& context) {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvProposeTransactionResult"
                               << " at tabletId# " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvProposeTransactionResult"
                                << " message# " << ev->Get()->Record.ShortDebugString());

        return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
    }

    bool HandleReply(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        return HandleReplyImpl(ev, context);
    }

    bool HandleReply(TEvColumnShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        return HandleReplyImpl(ev, context);
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxTruncateTable ||
                       txState->TxType == TTxState::TxTruncateColumnTable);

        if (txState->TxType == TTxState::TxTruncateTable) {
            if (NTableState::CheckPartitioningChangedForTableModification(*txState, context)) {
                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            DebugHint() << " UpdatePartitioningForTableModification");
                NTableState::UpdatePartitioningForTableModification(OperationId, *txState, context);
            }
        }

        txState->ClearShardsInProgress();

        TPath tablePath = TPath::Init(txState->TargetPathId, context.SS);
        Y_ABORT_UNLESS(tablePath.IsResolved());

        Y_ABORT_UNLESS(txState->Shards.size());

        const auto seqNo = context.SS->StartRound(*txState);

        TString txBody;
        if (tablePath->IsTable()) {
            Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
            auto table = context.SS->Tables.at(tablePath.Base()->PathId);

            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            context.SS->FillSeqNo(tx, seqNo);
            auto truncateTable = tx.MutableTruncateTable();
            truncateTable->SetTableSchemaVersion(table->AlterVersion + 1);
            txState->TargetPathId.ToProto(truncateTable->MutablePathId());
            Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&txBody);
        } else if (tablePath->IsColumnTable()) {
            NKikimrTxColumnShard::TSchemaTxBody tx;
            context.SS->FillSeqNo(tx, seqNo);
            auto* truncate = tx.MutableTruncateTable();
            truncate->SetPathId(txState->TargetPathId.LocalPathId);
            Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&txBody);
        } else {
            Y_ABORT();
        }

        for (const auto& shard : txState->Shards) {
            auto idx = shard.Idx;
            auto tabletId = context.SS->ShardInfos[idx].TabletID;
            auto event = context.SS->MakeShardProposal(tablePath, OperationId, seqNo, txBody, context.Ctx);
            context.OnComplete.BindMsgToPipe(OperationId, tabletId, idx, event.Release());
        }

        txState->UpdateShardsInProgress(TTxState::ConfigureParts);
        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TTruncateTable TPropose"
            << " operationId# " << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
            {TEvDataShard::TEvProposeTransactionResult::EventType,
             TEvColumnShard::TEvProposeTransactionResult::EventType});
    }

    template<typename TEvent>
    bool HandleReplyImpl(TEvent& ev, TOperationContext& context) {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply " << TEvSchemaChangedTraits<TEvent>::GetName()
                                << " at tablet: " << ssId);

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        return HandleReplyImpl(ev, context);
    }

    bool HandleReply(TEvColumnShard::TEvNotifyTxCompletionResult::TPtr& ev, TOperationContext& context) override {
        return HandleReplyImpl(ev, context);
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", stepId: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxTruncateTable ||
                       txState->TxType == TTxState::TxTruncateColumnTable);

        NIceDb::TNiceDb db(context.GetDB());

        txState->PlanStep = step;
        context.SS->PersistTxPlanStep(db, OperationId, step);

        const auto path = TPath::Init(txState->TargetPathId, context.SS);

        if (path->IsTable()) {
            auto table = context.SS->Tables.at(path.Base()->PathId);
            table->AlterVersion += 1;
            context.SS->PersistTableAlterVersion(db, path.Base()->PathId, table);

            // This check means that the table being processed is the main one.
            if (!path.Parent()->IsTableIndex()) {
                NTableIndexVersion::SyncChildIndexVersions(
                    path.Base(), table, table->AlterVersion,
                    OperationId, context, db
                );
            }
        } else if (path->IsColumnTable()) {
            Y_ABORT_UNLESS(context.SS->ColumnTables.contains(txState->TargetPathId));
            auto tableInfo = context.SS->ColumnTables.GetVerifiedPtr(txState->TargetPathId);
            tableInfo->AlterVersion += 1;
            context.SS->PersistColumnTable(db, txState->TargetPathId, *tableInfo);
        } else {
            Y_ABORT();
        }

        context.SS->ClearDescribePathCaches(path.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, path.Base()->PathId);

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxTruncateTable ||
                       txState->TxType == TTxState::TxTruncateColumnTable);

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

class TTruncateTable: public TSubOperation {
public:
    using TSubOperation::TSubOperation;
    static TTxState::ETxState NextState() {
        return TTxState::ConfigureParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
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
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
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

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        THolder<TProposeResponse> result;
        result.Reset(new TEvSchemeShard::TEvModifySchemeTransactionResult(
            NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId)));

        const auto& truncateTableOp = Transaction.GetTruncateTable();
        const auto stringTablePath = NKikimr::JoinPath({Transaction.GetWorkingDir(), truncateTableOp.GetTableName()});
        TPath tablePath = TPath::Resolve(stringTablePath, context.SS);
        {
            TPath::TChecker checks = tablePath.Check();
            checks
                .IsResolved()
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .NotDeleted()
                .NotUnderTheSameOperation(OperationId.GetTxId())
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const bool isColumnTable = tablePath.Base()->IsColumnTable();
        const bool isTable = tablePath.Base()->IsTable();

        if (!isColumnTable && !isTable) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, "TRUNCATE TABLE is only supported for tables and column tables");
            return result;
        }

        if (isColumnTable) {
            if (!AppData()->FeatureFlags.GetEnableTruncateColumnTable()) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed, "TRUNCATE TABLE is not supported for column tables");
                return result;
            }

            if (!AppData()->ColumnShardConfig.GetGenerateInternalPathId()) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed,
                    "TRUNCATE TABLE for column tables requires GenerateInternalPathId to be enabled");
                return result;
            }

            TPath::TChecker colChecks = tablePath.Check();
            colChecks
                .IsColumnTable()
                .NotReadOnlyColumnTable()
                .NotUnderDeleting()
                .NotBackupTable();

            if (!colChecks) {
                result->SetError(colChecks.GetStatus(), colChecks.GetError());
                return result;
            }
        } else {
            TPath::TChecker tableChecks = tablePath.Check();
            tableChecks
                .IsTable()
                .NotBackupTable();

            if (!tableChecks) {
                result->SetError(tableChecks.GetStatus(), tableChecks.GetError());
                return result;
            }
        }

        {
            TString errStr;
            if (!context.SS->CheckApplyIf(Transaction, errStr)) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
                return result;
            }
        }

        {
            TString errStr;
            if (!context.SS->CheckLocks(tablePath.Base()->PathId, Transaction, errStr)) {
                result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
                return result;
            }
        }

        // Begin create local transaction

        TTxState::ETxType txType = isColumnTable ? TTxState::TxTruncateColumnTable : TTxState::TxTruncateTable;

        if (isColumnTable) {
            Y_ABORT_UNLESS(context.SS->ColumnTables.contains(tablePath.Base()->PathId));
            auto tableInfo = context.SS->ColumnTables.GetVerified(tablePath.Base()->PathId);

            // TRUNCATE is currently supported only for standalone column tables. Tables that belong to a
            // column store are rejected here (the same restriction is enforced on the column shard side).
            if (!tableInfo->IsStandalone()) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed,
                    "TRUNCATE TABLE is not supported for column tables in a column store");
                return result;
            }

            // Tiering (TTL eviction to external storage) is bound to the table's InternalPathId and
            // external-storage activation on the column shard. Truncating would allocate a new
            // InternalPathId; until tiering is fully re-wired across that swap, reject early.
            if (!tableInfo->GetUsedTiers().empty()) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed,
                    "Cannot truncate column table with tiering");
                return result;
            }

            TTxState& txState = context.SS->CreateTx(OperationId, txType, tablePath.Base()->PathId);
            txState.State = TTxState::ConfigureParts;

            {
                NIceDb::TNiceDb db(context.GetDB());
                for (auto shardIdx : tableInfo->BuildOwnedColumnShardsVerified()) {
                    Y_VERIFY_S(context.SS->ShardInfos.contains(shardIdx), "Unknown shardIdx " << shardIdx);
                    txState.Shards.emplace_back(shardIdx, context.SS->ShardInfos[shardIdx].TabletType, TTxState::ConfigureParts);

                    context.SS->ShardInfos[shardIdx].CurrentTxId = OperationId.GetTxId();
                    context.SS->PersistShardTx(db, shardIdx, OperationId.GetTxId());
                }
            }

            tablePath.Base()->PathState = TPathElement::EPathState::EPathStateAlter;
            tablePath.Base()->LastTxId = OperationId.GetTxId();

            NIceDb::TNiceDb db(context.GetDB());
            context.SS->PersistLastTxId(db, tablePath.Base());
            context.SS->PersistTxState(db, OperationId);

            context.OnComplete.ActivateTx(OperationId);

            context.SS->ClearDescribePathCaches(tablePath.Base());
            context.OnComplete.PublishToSchemeBoard(OperationId, tablePath.Base()->PathId);

            SetState(NextState());
        } else {
            Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
            TTableInfo::TPtr table = context.SS->Tables.at(tablePath.Base()->PathId);
            Y_ABORT_UNLESS(table->GetPartitions().size());

            {
                context.MemChanges.GrabPath(context.SS, tablePath.Base()->PathId);
                context.MemChanges.GrabNewTxState(context.SS, OperationId);

                context.DbChanges.PersistPath(tablePath.Base()->PathId);
                context.DbChanges.PersistTxState(OperationId);

                tablePath.Base()->PathState = TPathElement::EPathState::EPathStateAlter;
                tablePath.Base()->LastTxId = OperationId.GetTxId();

                TTxState& txState = context.SS->CreateTx(OperationId, txType, tablePath.Base()->PathId);
                txState.State = TTxState::ConfigureParts;

                for (const auto* shard : table->GetPartitions()) {
                    auto shardIdx = shard->ShardIdx;
                    context.MemChanges.GrabShard(context.SS, shardIdx);
                    context.DbChanges.PersistShard(shardIdx);

                    TShardInfo& shardInfo = context.SS->ShardInfos[shardIdx];
                    txState.Shards.emplace_back(shardIdx, ETabletType::DataShard, TTxState::ConfigureParts);
                    shardInfo.CurrentTxId = OperationId.GetTxId();
                }

                for (auto splitTx : table->GetSplitOpsInFlight()) {
                    context.OnComplete.Dependence(splitTx.GetTxId(), OperationId.GetTxId());
                }

                context.OnComplete.ActivateTx(OperationId);
                SetState(NextState());
                Y_ABORT_UNLESS(txState.Shards.size());
            }
        }

        // End create local transaction
        // (but this does not mean that the local transaction has been executed or started)

        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTruncateTable AbortPropose"
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << context.SS->TabletID());
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        AbortUnsafeDropOperation(OperationId, forceDropTxId, context);
    }
};

enum ESchemeObjectType {
// First tree's level
    MainTable,

// Second tree's level
    GenericIndex,                // used for secondary, unique and fulltext indexes
    GlobalVectorIndex,           // global vector index requires special processing
    PrefixVectorIndex,           // prefixed vector index requires special processing
    FulltextCompactIndex,        // compact fulltext index has a sequence under indexImplTable

// Third tree's level
    GenericIndexImplTable,       // simple index impl table
    IndexImplWithSequence,       // index impl table with a sequence
};

// About DfsOnTableChildrenTree.
//
// Traverses the table's child tree in DFS order (main table -> index objects -> index implementation tables)
// and builds a list of TRUNCATE TABLE sub-operations, so all related tables are truncated as one consistent action.
//
// For each node that is a table, the function adds a TRUNCATE TABLE sub-operation to `result`.
//
// During the traversal it also checks that the tree structure matches what we expect for the supported index types:
//  - the main table may have only certain kinds of children (table indexes / sequences);
//  - for each supported index type, its subtree is checked (which implementation tables may exist, their types,
//    and any special-case exceptions).
// If the structure does not match, the traversal fails and the operation is rejected.
//
// These checks are intentionally strict. If the expected tree structure changes in the future, truncating only some of
// the related tables could leave the database in an inconsistent state. In that case we return an error instead of
// running an unsafe TRUNCATE.
bool DfsOnTableChildrenTree(
    TOperationId opId,
    const TTxTransaction& tx,
    TOperationContext& context,
    const TPathId& currentPathId,
    TVector<ISubOperation::TPtr>& result,
    ESchemeObjectType objectType
) {
    TPath currentPath = TPath::Init(currentPathId, context.SS);

    if (currentPath->IsTable()) {
        TPath::TChecker checks = currentPath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .NotDeleted()
            .NotBackupTable()
            .NotUnderOperation();

        if (!checks) {
            result = {CreateReject(opId, checks.GetStatus(), checks.GetError())};
            return false;
        }

        const auto workingDir = TString(NKikimr::ExtractParent(currentPath.PathString()));
        const auto tableName = TString(NKikimr::ExtractBase(currentPath.PathString()));

        auto modifycheme = TransactionTemplate(workingDir, NKikimrSchemeOp::EOperationType::ESchemeOpTruncateTable);
        modifycheme.SetInternal(tx.GetInternal());
        auto truncateTable = modifycheme.MutableTruncateTable();
        truncateTable->SetTableName(tableName);
        result.push_back(CreateTruncateTable(NextPartId(opId, result), modifycheme));
    }

    switch (objectType) {
        case ESchemeObjectType::MainTable: {
            for (const auto& [childName, childPathId] : currentPath.Base()->GetChildren()) {
                Y_ABORT_UNLESS(context.SS->PathsById.contains(childPathId));
                TPath srcChildPath = currentPath.Child(childName);

                if (srcChildPath.IsDeleted()) {
                    continue;
                }

                switch (srcChildPath.Base()->PathType) {
                    case NKikimrSchemeOp::EPathType::EPathTypeTableIndex: {
                        auto indexType = context.SS->Indexes.at(srcChildPath.Base()->PathId)->Type;

                        switch (indexType) {
                            case NKikimrSchemeOp::EIndexTypeInvalid: {
                                result = {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, "Cannot truncate table with invalid indexes")};
                                return false;
                            }
                            case NKikimrSchemeOp::EIndexTypeGlobalAsync: {
                                result = {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, "Cannot truncate table with async indexes")};
                                return false;
                            }
                            case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree: {
                                const auto& index = context.SS->Indexes.at(childPathId);
                                bool isGlobalVectorIndex = index->IndexKeys.size() == 1;
                                bool isPrefixVectorIndex = index->IndexKeys.size() > 1;

                                if (isGlobalVectorIndex) {
                                    if (!DfsOnTableChildrenTree(opId, tx, context, childPathId, result, ESchemeObjectType::GlobalVectorIndex)) {
                                        return false;
                                    }
                                } else if (isPrefixVectorIndex) {
                                    if (!DfsOnTableChildrenTree(opId, tx, context, childPathId, result, ESchemeObjectType::PrefixVectorIndex)) {
                                        return false;
                                    }
                                }

                                break;
                            }
                            case NKikimrSchemeOp::EIndexTypeGlobalJsonCompact:
                            case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact:
                            case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance: {
                                if (!DfsOnTableChildrenTree(opId, tx, context, childPathId, result, ESchemeObjectType::FulltextCompactIndex)) {
                                    return false;
                                }

                                break;
                            }
                            case NKikimrSchemeOp::EIndexTypeGlobalJson:
                            case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
                            case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
                            case NKikimrSchemeOp::EIndexTypeGlobal:
                            case NKikimrSchemeOp::EIndexTypeGlobalUnique: {
                                if (!DfsOnTableChildrenTree(opId, tx, context, childPathId, result, ESchemeObjectType::GenericIndex)) {
                                    return false;
                                }

                                break;
                            }
                            case NKikimrSchemeOp::EIndexTypeLocalBloomFilter:
                            case NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter:
                            case NKikimrSchemeOp::EIndexTypeLocalMinMax:
                            case NKikimrSchemeOp::EIndexTypeLocalCountMinSketch:
                                // Local index scheme objects are not supported yet in row tables
                                break;
                        }

                        break;
                    }

                    case NKikimrSchemeOp::EPathType::EPathTypeCdcStream: {
                        result = {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, "Cannot truncate table with CDC streams")};
                        return false;
                    }

                    case NKikimrSchemeOp::EPathType::EPathTypeSequence: {
                        break;
                    }

                    default: {
                        result = {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, "Cannot truncate table with unknown children")};
                        return false;
                    }
                }
            }

            break;
        }

        case ESchemeObjectType::GlobalVectorIndex:
        case ESchemeObjectType::PrefixVectorIndex:
        case ESchemeObjectType::FulltextCompactIndex:
        case ESchemeObjectType::GenericIndex: {
            for (const auto& [childName, childPathId] : currentPath.Base()->GetChildren()) {
                Y_ABORT_UNLESS(context.SS->PathsById.contains(childPathId));
                TPath srcChildPath = currentPath.Child(childName);

                if (srcChildPath.IsDeleted()) {
                    continue;
                }

                constexpr TStringBuf excludedFromTruncateTableName = "indexImplLevelTable";
                if (objectType == ESchemeObjectType::GlobalVectorIndex && srcChildPath.PathString().EndsWith(excludedFromTruncateTableName)) {
                    continue;
                }

                switch (srcChildPath.Base()->PathType) {
                    case NKikimrSchemeOp::EPathType::EPathTypeTable: {
                        if (objectType == ESchemeObjectType::PrefixVectorIndex && srcChildPath.PathString().EndsWith("indexImplPrefixTable") ||
                            objectType == ESchemeObjectType::FulltextCompactIndex && srcChildPath.PathString().EndsWith("indexImplTable")) {
                            if (!DfsOnTableChildrenTree(opId, tx, context, childPathId, result, ESchemeObjectType::IndexImplWithSequence)) {
                                return false;
                            }
                        } else {
                            if (!DfsOnTableChildrenTree(opId, tx, context, childPathId, result, ESchemeObjectType::GenericIndexImplTable)) {
                                return false;
                            }
                        }

                        break;
                    }

                    default: {
                        result = {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, "Cannot truncate table with unknown children")};
                        return false;
                    }
                }
            }

            break;
        }

        case ESchemeObjectType::IndexImplWithSequence: {
            if (currentPath.Base()->GetChildren().size() != 1 ||
                currentPath.Child(currentPath.Base()->GetChildren().begin()->first).Base()->PathType != NKikimrSchemeOp::EPathType::EPathTypeSequence
            ) {
                result = {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, TStringBuilder() << currentPath.PathString() << " should contain only 1 sequence")};
                return false;
            }

            break;
        }

        case ESchemeObjectType::GenericIndexImplTable: {
            if (!currentPath.Base()->GetChildren().empty()) {
                result = {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, "Index impl tables cannot contain children")};
                return false;
            }

            break;
        }
    }

    return true;
}

} // anonymous namespace

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateTruncateTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TTruncateTable>(id, tx);
}

ISubOperation::TPtr CreateTruncateTable(TOperationId opId, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TTruncateTable>(opId, state);
}

TVector<ISubOperation::TPtr> CreateConsistentTruncateTable(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    const auto& op = tx.GetTruncateTable();
    const auto stringMainTablePath = NKikimr::JoinPath({tx.GetWorkingDir(), op.GetTableName()});
    TPath mainTablePath = TPath::Resolve(stringMainTablePath, context.SS);

    TVector<ISubOperation::TPtr> result = {};

    // 'IsResolved' check is necessary because the dfs implementation expects that the vertex coming into the function exists.
    // If we do not do this check, then the Y_VERIFY may fire if the path does not exist.

    // Since one check was added, it makes sense to add all the checks at once,
    // so as not to generate a large set of sub-operations in case even the main table is "wrong".
    TPath::TChecker checks = mainTablePath.Check();
    checks
        .IsResolved();

    if (!checks) {
        result = {CreateReject(opId, checks.GetStatus(), checks.GetError())};
        return result;
    }

    if (mainTablePath.Base()->IsColumnTable()) {
        // Column tables: use the unified TruncateTable sub-operation
        result.push_back(CreateTruncateTable(opId, tx));
        return result;
    }

    // Row tables: check that it's a table
    TPath::TChecker tableChecks = mainTablePath.Check();
    tableChecks.IsTable();

    if (!tableChecks) {
        result = {CreateReject(opId, tableChecks.GetStatus(), tableChecks.GetError())};
        return result;
    }

    if (mainTablePath.Parent()->IsTableIndex()) {
        result = {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, "Cannot truncate index")};
        return result;
    }

    DfsOnTableChildrenTree(opId, tx, context, mainTablePath.Base()->PathId, result, ESchemeObjectType::MainTable);

    return result;
}

}
