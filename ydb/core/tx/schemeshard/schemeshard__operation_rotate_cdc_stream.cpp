#include "schemeshard__operation_rotate_cdc_stream.h"

#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_cdc_stream_common.h"
#include "schemeshard_utils.h"  // for TransactionTemplate

#define LOG_D(stream) LOG_DEBUG_S (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

namespace NCdc {

namespace {

class TPropose: public TSubOperationState {
    TString DebugHint() const override {
        return TStringBuilder()
            << "RotateCdcStream TPropose"
            << " opId# " << OperationId << " ";
    }

public:
    explicit TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxRotateCdcStream);

        // TODO(KIKIMR-12278): shards

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const auto step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << "HandleReply TEvOperationPlan"
            << ": step# " << step);

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxRotateCdcStream);
        const auto& oldStreamPathId = txState->SourcePathId;
        const auto& newStreamPathId = txState->TargetPathId;

        Y_ABORT_UNLESS(context.SS->PathsById.contains(oldStreamPathId));
        auto oldStreamPath = context.SS->PathsById.at(oldStreamPathId);
        Y_ABORT_UNLESS(context.SS->PathsById.contains(newStreamPathId));
        auto newStreamPath = context.SS->PathsById.at(newStreamPathId);

        Y_ABORT_UNLESS(context.SS->CdcStreams.contains(oldStreamPathId));
        auto oldStream = context.SS->CdcStreams.at(oldStreamPathId);
        Y_ABORT_UNLESS(context.SS->CdcStreams.contains(newStreamPathId));
        auto newStream = context.SS->CdcStreams.at(newStreamPathId);

        NIceDb::TNiceDb db(context.GetDB());

        newStreamPath->StepCreated = step;
        context.SS->PersistCreateStep(db, newStreamPathId, step);

        context.SS->PersistCdcStream(db, newStreamPathId);
        context.SS->CdcStreams[newStreamPathId] = newStream->AlterData;
        context.SS->TabletCounters->Simple()[COUNTER_CDC_STREAMS_COUNT].Add(1);

        context.SS->PersistCdcStream(db, oldStreamPathId);
        context.SS->CdcStreams[oldStreamPathId]->FinishAlter();

        context.SS->ClearDescribePathCaches(oldStreamPath);
        context.SS->ClearDescribePathCaches(newStreamPath);
        context.OnComplete.PublishToSchemeBoard(OperationId, oldStreamPathId);
        context.OnComplete.PublishToSchemeBoard(OperationId, newStreamPathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

private:
    const TOperationId OperationId;

}; // TPropose

class TRotateCdcStream: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& op = Transaction.GetRotateCdcStream();
        const auto& oldStreamName = op.GetOldStreamName();
        const auto& newStreamOp = op.GetNewStream();
        const auto& newStreamDesc = newStreamOp.GetStreamDescription();
        const auto& newStreamName = newStreamDesc.GetName();
        const auto acceptExisted = !Transaction.GetFailOnExist();

        LOG_N("TRotateCdcStream Propose"
            << ": opId# " << OperationId
            << ", oldStream# " << workingDir << "/" << oldStreamName
            << ", newStream# " << workingDir << "/" << newStreamName);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), context.SS->TabletID());

        if (op.GetTableName() != newStreamOp.GetTableName()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "New stream should be created on the same table");
            return result;
        }

        const auto oldStreamPath = TPath::Resolve(workingDir, context.SS).Dive(oldStreamName);
        {
            const auto checks = oldStreamPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsCdcStream()
                .NotUnderDeleting();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const auto tablePath = oldStreamPath.Parent();
        {
            const auto checks = tablePath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsTable()
                .NotAsyncReplicaTable()
                .NotUnderDeleting();

            // Allow CDC operations on tables that are under incremental backup/restore
            if (checks && tablePath.IsUnderOperation() &&
                !tablePath.IsUnderOutgoingIncrementalRestore()) {
                checks.NotUnderOperation();
            }

            if (checks && !tablePath.IsInsideTableIndexPath()) {
                checks.IsCommonSensePath();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        auto newStreamPath = tablePath.Child(newStreamName);
        {
            const auto checks = newStreamPath.Check();
            checks
                .IsAtLocalSchemeShard();

            if (newStreamPath.IsResolved()) {
                checks
                    .IsResolved()
                    .NotUnderDeleting()
                    .FailOnExist(TPathElement::EPathType::EPathTypeCdcStream, acceptExisted);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                checks
                    .IsValidLeafName(context.UserToken.Get())
                    .PathsLimit()
                    .DirChildrenLimit();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (newStreamPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(newStreamPath.Base()->CreateTxId));
                    result->SetPathId(newStreamPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }


        Y_ABORT_UNLESS(context.SS->CdcStreams.contains(oldStreamPath.Base()->PathId));
        auto oldStream = context.SS->CdcStreams.at(oldStreamPath.Base()->PathId);

        TCdcStreamInfo::EState requiredState = TCdcStreamInfo::EState::ECdcStreamStateDisabled;
        TCdcStreamInfo::EState newState = TCdcStreamInfo::EState::ECdcStreamStateInvalid;

        if (oldStream->State == TCdcStreamInfo::EState::ECdcStreamStateReady) {
            newState = requiredState;
        }

        if (newState == TCdcStreamInfo::EState::ECdcStreamStateInvalid) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, TStringBuilder()
                << "Cannot switch state"
                << ": from# " << oldStream->State
                << ", to# " << requiredState);
            return result;
        }

        switch (newStreamDesc.GetMode()) {
        case NKikimrSchemeOp::ECdcStreamModeKeysOnly:
        case NKikimrSchemeOp::ECdcStreamModeNewImage:
        case NKikimrSchemeOp::ECdcStreamModeOldImage:
        case NKikimrSchemeOp::ECdcStreamModeNewAndOldImages:
        case NKikimrSchemeOp::ECdcStreamModeRestoreIncrBackup:
            break;
        case NKikimrSchemeOp::ECdcStreamModeUpdate:
            if (newStreamDesc.GetFormat() == NKikimrSchemeOp::ECdcStreamFormatDynamoDBStreamsJson) {
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                    "DYNAMODB_STREAMS_JSON format incompatible with specified stream mode");
                return result;
            }
            if (newStreamDesc.GetFormat() == NKikimrSchemeOp::ECdcStreamFormatDebeziumJson) {
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                    "DEBEZIUM_JSON format incompatible with specified stream mode");
                return result;
            }
            break;
        default:
            result->SetError(NKikimrScheme::StatusInvalidParameter, TStringBuilder()
                << "Invalid stream mode: " << static_cast<ui32>(newStreamDesc.GetMode()));
            return result;
        }

        switch (newStreamDesc.GetFormat()) {
        case NKikimrSchemeOp::ECdcStreamFormatProto:
        case NKikimrSchemeOp::ECdcStreamFormatJson:
            break;
        case NKikimrSchemeOp::ECdcStreamFormatDynamoDBStreamsJson:
            if (!AppData()->FeatureFlags.GetEnableChangefeedDynamoDBStreamsFormat()) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed,
                    "DYNAMODB_STREAMS_JSON format is not supported yet");
                return result;
            }
            if (tablePath.Base()->DocumentApiVersion < 1) {
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                    "DYNAMODB_STREAMS_JSON format incompatible with non-document table");
                return result;
            }
            break;
        case NKikimrSchemeOp::ECdcStreamFormatDebeziumJson:
            if (!AppData()->FeatureFlags.GetEnableChangefeedDebeziumJsonFormat()) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed,
                    "DEBEZIUM_JSON format is not supported yet");
                return result;
            }
            break;
        default:
            result->SetError(NKikimrScheme::StatusInvalidParameter, TStringBuilder()
                << "Invalid stream format: " << static_cast<ui32>(newStreamDesc.GetFormat()));
            return result;
        }

        if (!newStreamDesc.GetAwsRegion().empty()) {
            switch (newStreamDesc.GetFormat()) {
            case NKikimrSchemeOp::ECdcStreamFormatDynamoDBStreamsJson:
                break;
            default:
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                    "AWS_REGION option incompatible with specified stream format");
                return result;
            }
        }

        if (newStreamDesc.GetVirtualTimestamps()) {
            switch (newStreamDesc.GetFormat()) {
            case NKikimrSchemeOp::ECdcStreamFormatProto:
            case NKikimrSchemeOp::ECdcStreamFormatJson:
                break;
            default:
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                    "VIRTUAL_TIMESTAMPS incompatible with specified stream format");
                return result;
            }
        }

        if (newStreamDesc.GetResolvedTimestampsIntervalMs()) {
            switch (newStreamDesc.GetFormat()) {
            case NKikimrSchemeOp::ECdcStreamFormatProto:
            case NKikimrSchemeOp::ECdcStreamFormatJson:
                break;
            default:
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                    "RESOLVED_TIMESTAMPS incompatible with specified stream format");
                return result;
            }
        }

        if (newStreamDesc.GetSchemaChanges()) {
            switch (newStreamDesc.GetFormat()) {
            case NKikimrSchemeOp::ECdcStreamFormatJson:
                break;
            default:
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                    "SCHEMA_CHANGES incompatible with specified stream format");
                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckLocks(tablePath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        TUserAttributes::TPtr userAttrs = new TUserAttributes(1);
        if (!userAttrs->ApplyPatch(EUserAttributesOp::CreateChangefeed, newStreamDesc.GetUserAttributes(), errStr) ||
            !userAttrs->CheckLimits(errStr))
        {
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        if (!AppData()->FeatureFlags.GetEnableTopicAutopartitioningForCDC() && newStreamOp.GetTopicAutoPartitioning()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Topic autopartitioning for CDC is disabled");
            return result;
        }

        auto guard = context.DbGuard();
        context.MemChanges.GrabPath(context.SS, oldStreamPath.Base()->PathId);
        context.MemChanges.GrabCdcStream(context.SS, oldStreamPath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        const auto pathId = context.SS->AllocatePathId();
        context.MemChanges.GrabNewPath(context.SS, pathId);
        context.MemChanges.GrabPath(context.SS, tablePath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabDomain(context.SS, newStreamPath.GetPathIdForDomain());
        context.MemChanges.GrabNewCdcStream(context.SS, pathId);

        context.DbChanges.PersistPath(oldStreamPath.Base()->PathId);
        context.DbChanges.PersistAlterCdcStream(oldStreamPath.Base()->PathId);
        context.DbChanges.PersistTxState(OperationId);

        context.DbChanges.PersistPath(pathId);
        context.DbChanges.PersistPath(tablePath.Base()->PathId);
        context.DbChanges.PersistApplyUserAttrs(pathId);
        context.DbChanges.PersistAlterCdcStream(pathId);
        context.DbChanges.PersistTxState(OperationId);

        auto streamAlter = oldStream->CreateNextVersion();
        Y_ABORT_UNLESS(streamAlter);
        streamAlter->State = newState;

        auto newStream = TCdcStreamInfo::Create(newStreamDesc);
        Y_ABORT_UNLESS(newStream);
        context.SS->CdcStreams[pathId] = newStream;

        newStreamPath.MaterializeLeaf(owner, pathId);
        result->SetPathId(pathId.LocalPathId);

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, TTxState::TxRotateCdcStream,
            newStreamPath.Base()->PathId, oldStreamPath.Base()->PathId);
        txState.State = TTxState::Propose;
        txState.MinStep = TStepId(1);

        oldStreamPath.Base()->PathState = TPathElement::EPathState::EPathStateAlter;
        oldStreamPath.Base()->LastTxId = OperationId.GetTxId();

        newStreamPath.Base()->PathState = NKikimrSchemeOp::EPathStateCreate;
        newStreamPath.Base()->CreateTxId = OperationId.GetTxId();
        newStreamPath.Base()->LastTxId = OperationId.GetTxId();
        newStreamPath.Base()->PathType = TPathElement::EPathType::EPathTypeCdcStream;
        newStreamPath.Base()->UserAttrs->AlterData = userAttrs;

        context.SS->IncrementPathDbRefCount(pathId);

        newStreamPath.DomainInfo()->IncPathsInside(context.SS);
        IncAliveChildrenSafeWithUndo(OperationId, tablePath, context); // for correct discard of ChildrenExist prop

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TRotateCdcStream AbortPropose"
            << ": opId# " << OperationId);
    }

    void AbortUnsafe(TTxId txId, TOperationContext& context) override {
        LOG_N("TRotateCdcStream AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << txId);
        context.OnComplete.DoneOperation(OperationId);
    }

}; // TRotateCdcStream

class TConfigurePartsAtTable: public NCdcStreamState::TConfigurePartsAtTable {
protected:
    void FillNotice(const TPathId& pathId, NKikimrTxDataShard::TFlatSchemeTransaction& tx, TOperationContext& context) const override {
        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
        auto table = context.SS->Tables.at(pathId);

        auto& notice = *tx.MutableRotateCdcStreamNotice();
        pathId.ToProto(notice.MutablePathId());
        notice.SetTableSchemaVersion(table->AlterVersion + 1);

        auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxRotateCdcStreamAtTable);
        auto newStreamPathId = txState->CdcPathId;
        auto oldStreamPathId = txState->SourcePathId;
        oldStreamPathId.ToProto(notice.MutableOldStreamPathId());

        Y_ABORT_UNLESS(context.SS->PathsById.contains(newStreamPathId));
        Y_ABORT_UNLESS(context.SS->CdcStreams.contains(newStreamPathId));
        auto newStreamPath = context.SS->PathsById.at(newStreamPathId);
        auto newStream = context.SS->CdcStreams.at(newStreamPathId);

        Y_ABORT_UNLESS(newStream->AlterData);
        context.SS->DescribeCdcStream(newStreamPathId, newStreamPath->Name, newStream->AlterData, *notice.MutableNewStreamDescription());
    }

public:
    using NCdcStreamState::TConfigurePartsAtTable::TConfigurePartsAtTable;

}; // TConfigurePartsAtTable

class TRotateCdcStreamAtTable: public TSubOperation {
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

        return TTxState::Invalid;
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigurePartsAtTable>(OperationId);
        case TTxState::Propose:
            return MakeHolder<NCdcStreamState::TProposeAtTable>(OperationId);
        case TTxState::ProposedWaitParts:
            return MakeHolder<NTableState::TProposedWaitParts>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    explicit TRotateCdcStreamAtTable(TOperationId id, const TTxTransaction& tx)
        : TSubOperation(id, tx)
    {
    }

    explicit TRotateCdcStreamAtTable(TOperationId id, TTxState::ETxState state)
        : TSubOperation(id, state)
    {
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& op = Transaction.GetRotateCdcStream();
        const auto& tableName = op.GetTableName();
        const auto& oldStreamName = op.GetOldStreamName();
        const auto& newStreamName = op.GetNewStream().GetStreamDescription().GetName();

        LOG_N("TRotateCdcStreamAtTable Propose"
            << ": opId# " << OperationId
            << ", oldStream# " << workingDir << "/" << tableName << "/" << oldStreamName
            << ", newStream# " << workingDir << "/" << tableName << "/" << newStreamName);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), context.SS->TabletID());

        if (op.GetTableName() != op.GetNewStream().GetTableName()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "New stream should be created on the same table");
            return result;
        }

        const auto workingDirPath = TPath::Resolve(workingDir, context.SS);
        {
            const auto checks = workingDirPath.Check();
            NCdcStreamAtTable::CheckWorkingDirOnPropose(
                checks,
                workingDirPath.IsTableIndex(Nothing(), false));
        }

        const auto tablePath = TPath::Resolve(workingDir, context.SS).Child(tableName, TPath::TSplitChildTag{});
        {
            const auto checks = tablePath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsTable()
                .NotAsyncReplicaTable()
                .NotUnderDeleting();

            // Allow CDC operations on tables that are under incremental backup/restore
            if (checks && tablePath.IsUnderOperation() &&
                !tablePath.IsUnderOutgoingIncrementalRestore()) {
                checks.NotUnderOperation();
            }

            if (checks && !tablePath.IsInsideTableIndexPath()) {
                checks.IsCommonSensePath();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const auto oldStreamPath = tablePath.Child(oldStreamName);
        {
            const auto checks = oldStreamPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsCdcStream()
                .IsUnderOperation()
                .IsUnderTheSameOperation(OperationId.GetTxId());

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const auto newStreamPath = tablePath.Child(newStreamName);
        {
            const auto checks = newStreamPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsCdcStream()
                .IsUnderOperation()
                .IsUnderTheSameOperation(OperationId.GetTxId());

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (!context.SS->CheckLocks(tablePath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        NKikimrScheme::EStatus status;
        if (!context.SS->CanCreateSnapshot(tablePath.Base()->PathId, OperationId.GetTxId(), status, errStr)) {
            result->SetError(status, errStr);
            return result;
        }

        auto guard = context.DbGuard();
        context.MemChanges.GrabPath(context.SS, tablePath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(tablePath.Base()->PathId);
        context.DbChanges.PersistTxState(OperationId);

        Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
        auto table = context.SS->Tables.at(tablePath.Base()->PathId);

        Y_ABORT_UNLESS(table->AlterVersion != 0);
        Y_ABORT_UNLESS(!table->AlterData);

        Y_ABORT_UNLESS(context.SS->CdcStreams.contains(oldStreamPath.Base()->PathId));
        auto stream = context.SS->CdcStreams.at(oldStreamPath.Base()->PathId);

        Y_ABORT_UNLESS(stream->AlterVersion != 0);
        Y_ABORT_UNLESS(stream->AlterData);

        const auto txType = TTxState::TxRotateCdcStreamAtTable;

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, txType, tablePath.Base()->PathId, oldStreamPath.Base()->PathId);
        txState.CdcPathId = newStreamPath.Base()->PathId;
        txState.State = TTxState::ConfigureParts;

        tablePath.Base()->PathState = NKikimrSchemeOp::EPathStateAlter;
        tablePath.Base()->LastTxId = OperationId.GetTxId();

        for (const auto& splitOpId : table->GetSplitOpsInFlight()) {
            context.OnComplete.Dependence(splitOpId.GetTxId(), OperationId.GetTxId());
        }

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TRotateCdcStreamAtTable AbortPropose"
            << ": opId# " << OperationId);
    }

    void AbortUnsafe(TTxId txId, TOperationContext& context) override {
        LOG_N("TRotateCdcStreamAtTable AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << txId);
        context.OnComplete.DoneOperation(OperationId);
    }
}; // TRotateCdcStreamAtTable

} // namespace anonymous

void DoRotateStream(
        TVector<ISubOperation::TPtr>& result,
        const NKikimrSchemeOp::TRotateCdcStream& op,
        const TOperationId& opId,
        const TPath& workingDirPath,
        const TPath& tablePath)
{
    {
        auto outTx = TransactionTemplate(tablePath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpRotateCdcStreamImpl);
        outTx.MutableRotateCdcStream()->CopyFrom(op);
        result.push_back(CreateRotateCdcStreamImpl(NextPartId(opId, result), outTx));
    }

    {
        auto outTx = TransactionTemplate(workingDirPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpRotateCdcStreamAtTable);
        outTx.MutableRotateCdcStream()->CopyFrom(op);
        result.push_back(CreateRotateCdcStreamAtTable(NextPartId(opId, result), outTx));
    }
}

} // namespace NCdc

using namespace NCdc;

ISubOperation::TPtr CreateRotateCdcStreamImpl(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TRotateCdcStream>(id, tx);
}

ISubOperation::TPtr CreateRotateCdcStreamImpl(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TRotateCdcStream>(id, state);
}

ISubOperation::TPtr CreateRotateCdcStreamAtTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TRotateCdcStreamAtTable>(id, tx);
}

ISubOperation::TPtr CreateRotateCdcStreamAtTable(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TRotateCdcStreamAtTable>(id, state);
}

TVector<ISubOperation::TPtr> CreateRotateCdcStream(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpRotateCdcStream);

    LOG_D("CreateRotateCdcStream"
        << ": opId# " << opId
        << ", tx# " << tx.ShortDebugString());

    const auto acceptExisted = !tx.GetFailOnExist();
    const auto& op = tx.GetRotateCdcStream();
    const auto& tableName = op.GetTableName();
    const auto& oldStreamName = op.GetOldStreamName();
    const auto& newStreamName = op.GetNewStream().GetStreamDescription().GetName();

    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);

    if (op.GetTableName() != op.GetNewStream().GetTableName()) {
        return {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, "New stream should be created on the same table")};
    }

    const auto tablePath = workingDirPath.Child(tableName);
    {
        const auto checks = tablePath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .IsTable()
            .NotAsyncReplicaTable()
            .NotUnderDeleting();

        // Allow CDC operations on tables that are under incremental backup/restore
        if (checks && tablePath.IsUnderOperation() &&
            !tablePath.IsUnderOutgoingIncrementalRestore()) {
            checks.NotUnderOperation();
        }

        if (checks && !tablePath.IsInsideTableIndexPath()) {
            checks.IsCommonSensePath();
        }

        if (!checks) {
            return {CreateReject(opId, checks.GetStatus(), checks.GetError())};
        }
    }

    const auto oldStreamPath = tablePath.Child(oldStreamName);
    {
        const auto checks = oldStreamPath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .IsCdcStream()
            .NotUnderDeleting();

        if (!checks) {
            return {CreateReject(opId, checks.GetStatus(), checks.GetError())};
        }
    }

    const auto newStreamPath = tablePath.Child(newStreamName);
    {
        const auto checks = newStreamPath.Check();
        checks
            .IsAtLocalSchemeShard();

        if (newStreamPath.IsResolved()) {
            checks
                .IsResolved()
                .NotUnderDeleting()
                .FailOnExist(TPathElement::EPathType::EPathTypeCdcStream, acceptExisted);
        } else {
            checks
                .NotEmpty()
                .NotResolved();
        }

        if (checks) {
            checks
                .IsValidLeafName(context.UserToken.Get())
                .PathsLimit()
                .DirChildrenLimit();
        }

        if (!checks) {
            return {CreateReject(opId, checks.GetStatus(), checks.GetError())};
        }
    }

    TString errStr;
    if (!context.SS->CheckApplyIf(tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, errStr)};
    }

    if (!context.SS->CheckLocks(tablePath.Base()->PathId, tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusMultipleModifications, errStr)};
    }

    TVector<ISubOperation::TPtr> result;

    DoRotateStream(result, op, opId, workingDirPath, tablePath);

    return result;
}

} // namespace NKikimr::NSchemeShard
