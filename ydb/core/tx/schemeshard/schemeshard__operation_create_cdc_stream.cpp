#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#define LOG_D(stream) LOG_DEBUG_S (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

namespace {

class TPropose: public TSubOperationState {
    TString DebugHint() const override {
        return TStringBuilder()
            << "NewCdcStream TPropose"
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateCdcStream);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const auto step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << "HandleReply TEvOperationPlan"
            << ": step# " << step);

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateCdcStream);
        const auto& pathId = txState->TargetPathId;

        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(context.SS->CdcStreams.contains(pathId));
        auto stream = context.SS->CdcStreams.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        context.SS->PersistCdcStream(db, pathId);
        context.SS->CdcStreams[pathId] = stream->AlterData;

        context.SS->TabletCounters->Simple()[COUNTER_CDC_STREAMS_COUNT].Add(1);
        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

private:
    const TOperationId OperationId;

}; // TPropose

class TNewCdcStream: public TSubOperation {
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
        const auto& op = Transaction.GetCreateCdcStream();
        const auto& streamDesc = op.GetStreamDescription();
        const auto& streamName = streamDesc.GetName();
        const auto acceptExisted = !Transaction.GetFailOnExist();

        LOG_N("TNewCdcStream Propose"
            << ": opId# " << OperationId
            << ", stream# " << workingDir << "/" << streamName);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), context.SS->TabletID());

        const auto tablePath = TPath::Resolve(workingDir, context.SS);
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

            if (checks && !tablePath.IsInsideTableIndexPath()) {
                checks.IsCommonSensePath();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        auto streamPath = tablePath.Child(streamName);
        {
            const auto checks = streamPath.Check();
            checks
                .IsAtLocalSchemeShard();

            if (streamPath.IsResolved()) {
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
                    .IsValidLeafName()
                    .PathsLimit()
                    .DirChildrenLimit();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (streamPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(streamPath.Base()->CreateTxId));
                    result->SetPathId(streamPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        switch (streamDesc.GetMode()) {
        case NKikimrSchemeOp::ECdcStreamModeKeysOnly:
        case NKikimrSchemeOp::ECdcStreamModeNewImage:
        case NKikimrSchemeOp::ECdcStreamModeOldImage:
        case NKikimrSchemeOp::ECdcStreamModeNewAndOldImages:
            break;
        case NKikimrSchemeOp::ECdcStreamModeUpdate:
            if (streamDesc.GetFormat() == NKikimrSchemeOp::ECdcStreamFormatDynamoDBStreamsJson) {
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                    "DYNAMODB_STREAMS_JSON format incompatible with specified stream mode");
                return result;
            }
            if (streamDesc.GetFormat() == NKikimrSchemeOp::ECdcStreamFormatDebeziumJson) {
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                    "DEBEZIUM_JSON format incompatible with specified stream mode");
                return result;
            }
            break;
        default:
            result->SetError(NKikimrScheme::StatusInvalidParameter, TStringBuilder()
                << "Invalid stream mode: " << static_cast<ui32>(streamDesc.GetMode()));
            return result;
        }

        switch (streamDesc.GetFormat()) {
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
                << "Invalid stream format: " << static_cast<ui32>(streamDesc.GetFormat()));
            return result;
        }

        if (!streamDesc.GetAwsRegion().empty()) {
            switch (streamDesc.GetFormat()) {
            case NKikimrSchemeOp::ECdcStreamFormatDynamoDBStreamsJson:
                break;
            default:
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                    "AWS_REGION option incompatible with specified stream format");
                return result;
            }
        }

        if (streamDesc.GetVirtualTimestamps()) {
            switch (streamDesc.GetFormat()) {
            case NKikimrSchemeOp::ECdcStreamFormatProto:
            case NKikimrSchemeOp::ECdcStreamFormatJson:
                break;
            default:
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                    "VIRTUAL_TIMESTAMPS incompatible with specified stream format");
                return result;
            }
        }

        if (streamDesc.GetResolvedTimestampsIntervalMs()) {
            switch (streamDesc.GetFormat()) {
            case NKikimrSchemeOp::ECdcStreamFormatProto:
            case NKikimrSchemeOp::ECdcStreamFormatJson:
                break;
            default:
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                    "RESOLVED_TIMESTAMPS incompatible with specified stream format");
                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckLocks(tablePath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        TUserAttributes::TPtr userAttrs = new TUserAttributes(1);
        if (!userAttrs->ApplyPatch(EUserAttributesOp::CreateChangefeed, streamDesc.GetUserAttributes(), errStr) ||
            !userAttrs->CheckLimits(errStr))
        {
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        auto stream = TCdcStreamInfo::Create(streamDesc);
        Y_ABORT_UNLESS(stream);

        auto guard = context.DbGuard();
        const auto pathId = context.SS->AllocatePathId();
        context.MemChanges.GrabNewPath(context.SS, pathId);
        context.MemChanges.GrabPath(context.SS, tablePath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabDomain(context.SS, streamPath.GetPathIdForDomain());
        context.MemChanges.GrabNewCdcStream(context.SS, pathId);

        context.DbChanges.PersistPath(pathId);
        context.DbChanges.PersistPath(tablePath.Base()->PathId);
        context.DbChanges.PersistApplyUserAttrs(pathId);
        context.DbChanges.PersistAlterCdcStream(pathId);
        context.DbChanges.PersistTxState(OperationId);

        streamPath.MaterializeLeaf(owner, pathId);
        result->SetPathId(pathId.LocalPathId);

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateCdcStream, streamPath.Base()->PathId);
        txState.State = TTxState::Propose;

        streamPath.Base()->PathState = NKikimrSchemeOp::EPathStateCreate;
        streamPath.Base()->CreateTxId = OperationId.GetTxId();
        streamPath.Base()->LastTxId = OperationId.GetTxId();
        streamPath.Base()->PathType = TPathElement::EPathType::EPathTypeCdcStream;
        streamPath.Base()->UserAttrs->AlterData = userAttrs;

        context.SS->CdcStreams[pathId] = stream;
        context.SS->IncrementPathDbRefCount(pathId);

        streamPath.DomainInfo()->IncPathsInside();
        tablePath.Base()->IncAliveChildren();

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TNewCdcStream AbortPropose"
            << ": opId# " << OperationId);
    }

    void AbortUnsafe(TTxId txId, TOperationContext& context) override {
        LOG_N("TNewCdcStream AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << txId);
        context.OnComplete.DoneOperation(OperationId);
    }

}; // TNewCdcStream

class TConfigurePartsAtTable: public NCdcStreamState::TConfigurePartsAtTable {
protected:
    void FillNotice(const TPathId& pathId, NKikimrTxDataShard::TFlatSchemeTransaction& tx, TOperationContext& context) const override {
        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
        auto table = context.SS->Tables.at(pathId);

        auto& notice = *tx.MutableCreateCdcStreamNotice();
        PathIdFromPathId(pathId, notice.MutablePathId());
        notice.SetTableSchemaVersion(table->AlterVersion + 1);

        bool found = false;
        for (const auto& [childName, childPathId] : path->GetChildren()) {
            Y_ABORT_UNLESS(context.SS->PathsById.contains(childPathId));
            auto childPath = context.SS->PathsById.at(childPathId);

            if (!childPath->IsCdcStream() || childPath->Dropped()) {
                continue;
            }

            Y_ABORT_UNLESS(context.SS->CdcStreams.contains(childPathId));
            auto stream = context.SS->CdcStreams.at(childPathId);

            if (stream->State != TCdcStreamInfo::EState::ECdcStreamStateInvalid) {
                continue;
            }

            Y_VERIFY_S(!found, "Too many cdc streams are planned to create"
                << ": found# " << PathIdFromPathId(notice.GetStreamDescription().GetPathId())
                << ", another# " << childPathId);
            found = true;

            Y_ABORT_UNLESS(stream->AlterData);
            context.SS->DescribeCdcStream(childPathId, childName, stream->AlterData, *notice.MutableStreamDescription());

            if (stream->AlterData->State == TCdcStreamInfo::EState::ECdcStreamStateScan) {
                notice.SetSnapshotName("ChangefeedInitialScan");
            }
        }
    }

public:
    using NCdcStreamState::TConfigurePartsAtTable::TConfigurePartsAtTable;

}; // TConfigurePartsAtTable

class TProposeAtTableWithInitialScan: public NCdcStreamState::TProposeAtTable {
public:
    using NCdcStreamState::TProposeAtTable::TProposeAtTable;

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        if (!NCdcStreamState::TProposeAtTable::HandleReply(ev, context)) {
            return false;
        }

        const auto step = TStepId(ev->Get()->StepId);
        NIceDb::TNiceDb db(context.GetDB());
        context.SS->SnapshotsStepIds[OperationId.GetTxId()] = step;
        context.SS->PersistSnapshotStepId(db, OperationId.GetTxId(), step);

        context.SS->TabletCounters->Simple()[COUNTER_SNAPSHOTS_COUNT].Add(1);
        return true;
    }

}; // TProposeAtTableWithInitialScan

class TDoneWithInitialScan: public TDone {
public:
    using TDone::TDone;

    bool ProgressState(TOperationContext& context) override {
        if (!TDone::ProgressState(context)) {
            return false;
        }

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateCdcStreamAtTableWithInitialScan);
        const auto& pathId = txState->TargetPathId;

        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        TMaybe<TPathId> streamPathId;
        for (const auto& [_, childPathId] : path->GetChildren()) {
            Y_ABORT_UNLESS(context.SS->PathsById.contains(childPathId));
            auto childPath = context.SS->PathsById.at(childPathId);

            if (childPath->CreateTxId != OperationId.GetTxId()) {
                continue;
            }

            Y_ABORT_UNLESS(childPath->IsCdcStream() && !childPath->Dropped());
            Y_ABORT_UNLESS(context.SS->CdcStreams.contains(childPathId));
            auto stream = context.SS->CdcStreams.at(childPathId);

            Y_ABORT_UNLESS(stream->State == TCdcStreamInfo::EState::ECdcStreamStateScan);
            Y_VERIFY_S(!streamPathId, "Too many cdc streams are planned to fill with initial scan"
                << ": found# " << *streamPathId
                << ", another# " << childPathId);
            streamPathId = childPathId;
        }

        if (AppData()->DisableCdcAutoSwitchingToReadyStateForTests) {
            return true;
        }

        Y_ABORT_UNLESS(streamPathId);
        context.OnComplete.Send(context.SS->SelfId(), new TEvPrivate::TEvRunCdcStreamScan(*streamPathId));

        return true;
    }

}; // TDoneWithInitialScan

class TNewCdcStreamAtTable: public TSubOperation {
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
            return MakeHolder<TConfigurePartsAtTable>(OperationId);
        case TTxState::Propose:
            if (InitialScan) {
                return MakeHolder<TProposeAtTableWithInitialScan>(OperationId);
            } else {
                return MakeHolder<NCdcStreamState::TProposeAtTable>(OperationId);
            }
        case TTxState::ProposedWaitParts:
            return MakeHolder<NTableState::TProposedWaitParts>(OperationId);
        case TTxState::Done:
            if (InitialScan) {
                return MakeHolder<TDoneWithInitialScan>(OperationId);
            } else {
                return MakeHolder<TDone>(OperationId);
            }
        default:
            return nullptr;
        }
    }

public:
    explicit TNewCdcStreamAtTable(TOperationId id, const TTxTransaction& tx, bool initialScan)
        : TSubOperation(id, tx)
        , InitialScan(initialScan)
    {
    }

    explicit TNewCdcStreamAtTable(TOperationId id, TTxState::ETxState state, bool initialScan)
        : TSubOperation(id, state)
        , InitialScan(initialScan)
    {
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& op = Transaction.GetCreateCdcStream();
        const auto& tableName = op.GetTableName();
        const auto& streamName = op.GetStreamDescription().GetName();

        LOG_N("TNewCdcStreamAtTable Propose"
            << ": opId# " << OperationId
            << ", stream# " << workingDir << "/" << tableName << "/" << streamName);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), context.SS->TabletID());

        const auto workingDirPath = TPath::Resolve(workingDir, context.SS);
        {
            const auto checks = workingDirPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsLikeDirectory()
                .NotUnderDeleting();

            if (checks && !workingDirPath.IsTableIndex()) {
                checks.IsCommonSensePath();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
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

            if (checks) {
                if (!tablePath.IsInsideTableIndexPath()) {
                    checks.IsCommonSensePath();
                }
                if (InitialScan) {
                    checks.IsUnderTheSameOperation(OperationId.GetTxId()); // lock op
                } else {
                    checks.NotUnderOperation();
                }
            }

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

        if (InitialScan) {
            context.MemChanges.GrabNewTableSnapshot(context.SS, tablePath.Base()->PathId, OperationId.GetTxId());
            context.DbChanges.PersistTableSnapshot(tablePath.Base()->PathId, OperationId.GetTxId());

            context.SS->TablesWithSnapshots.emplace(tablePath.Base()->PathId, OperationId.GetTxId());
            context.SS->SnapshotTables[OperationId.GetTxId()].insert(tablePath.Base()->PathId);
        }

        Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
        auto table = context.SS->Tables.at(tablePath.Base()->PathId);

        Y_ABORT_UNLESS(table->AlterVersion != 0);
        Y_ABORT_UNLESS(!table->AlterData);

        const auto txType = InitialScan
            ? TTxState::TxCreateCdcStreamAtTableWithInitialScan
            : TTxState::TxCreateCdcStreamAtTable;

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, txType, tablePath.Base()->PathId);
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
        LOG_N("TNewCdcStreamAtTable AbortPropose"
            << ": opId# " << OperationId);
    }

    void AbortUnsafe(TTxId txId, TOperationContext& context) override {
        LOG_N("TNewCdcStreamAtTable AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << txId);
        context.OnComplete.DoneOperation(OperationId);
    }

private:
    const bool InitialScan;

}; // TNewCdcStreamAtTable

} // anonymous

ISubOperation::TPtr CreateNewCdcStreamImpl(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TNewCdcStream>(id, tx);
}

ISubOperation::TPtr CreateNewCdcStreamImpl(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TNewCdcStream>(id, state);
}

ISubOperation::TPtr CreateNewCdcStreamAtTable(TOperationId id, const TTxTransaction& tx, bool initialScan) {
    return MakeSubOperation<TNewCdcStreamAtTable>(id, tx, initialScan);
}

ISubOperation::TPtr CreateNewCdcStreamAtTable(TOperationId id, TTxState::ETxState state, bool initialScan) {
    return MakeSubOperation<TNewCdcStreamAtTable>(id, state, initialScan);
}

TVector<ISubOperation::TPtr> CreateNewCdcStream(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStream);

    LOG_D("CreateNewCdcStream"
        << ": opId# " << opId
        << ", tx# " << tx.ShortDebugString());

    const auto acceptExisted = !tx.GetFailOnExist();
    const auto& op = tx.GetCreateCdcStream();
    const auto& tableName = op.GetTableName();
    const auto& streamDesc = op.GetStreamDescription();
    const auto& streamName = streamDesc.GetName();

    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);

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
            .NotUnderDeleting()
            .NotUnderOperation();

        if (checks) {
            if (!tablePath.IsInsideTableIndexPath()) {
                checks.IsCommonSensePath();
            } else {
                if (!tablePath.Parent().IsTableIndex(NKikimrSchemeOp::EIndexTypeGlobal)) {
                    return {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed,
                        "Cannot add changefeed to index table")};
                }
                if (!AppData()->FeatureFlags.GetEnableChangefeedsOnIndexTables()) {
                    return {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed,
                        "Changefeed on index table is not supported yet")};
                }
            }
        }

        if (!checks) {
            return {CreateReject(opId, checks.GetStatus(), checks.GetError())};
        }
    }

    const auto streamPath = tablePath.Child(streamName);
    {
        const auto checks = streamPath.Check();
        checks
            .IsAtLocalSchemeShard();

        if (streamPath.IsResolved()) {
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
                .IsValidLeafName()
                .PathsLimit()
                .DirChildrenLimit();
        }

        if (!checks) {
            return {CreateReject(opId, checks.GetStatus(), checks.GetError())};
        }
    }

    switch (streamDesc.GetMode()) {
    case NKikimrSchemeOp::ECdcStreamModeKeysOnly:
    case NKikimrSchemeOp::ECdcStreamModeUpdate:
    case NKikimrSchemeOp::ECdcStreamModeNewImage:
    case NKikimrSchemeOp::ECdcStreamModeOldImage:
    case NKikimrSchemeOp::ECdcStreamModeNewAndOldImages:
        break;
    default:
        return {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, TStringBuilder()
            << "Invalid stream mode: " << static_cast<ui32>(streamDesc.GetMode()))};
    }

    const ui64 aliveStreams = context.SS->GetAliveChildren(tablePath.Base(), NKikimrSchemeOp::EPathTypeCdcStream);
    if (aliveStreams + 1 > tablePath.DomainInfo()->GetSchemeLimits().MaxTableCdcStreams) {
        return {CreateReject(opId, NKikimrScheme::EStatus::StatusResourceExhausted, TStringBuilder()
            << "cdc streams count has reached maximum value in the table"
            << ", children limit for dir in domain: " << tablePath.DomainInfo()->GetSchemeLimits().MaxTableCdcStreams
            << ", intention to create new children: " << aliveStreams + 1)};
    }

    if (!AppData()->PQConfig.GetEnableProtoSourceIdInfo()) {
        return {CreateReject(opId, NKikimrScheme::EStatus::StatusPreconditionFailed, TStringBuilder()
            << "Changefeeds require proto source id info to be enabled")};
    }

    TString errStr;
    if (!context.SS->CheckApplyIf(tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, errStr)};
    }

    if (!context.SS->CheckLocks(tablePath.Base()->PathId, tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusMultipleModifications, errStr)};
    }

    const bool initialScan = op.GetStreamDescription().GetState() == NKikimrSchemeOp::ECdcStreamStateScan;
    if (initialScan && !AppData()->FeatureFlags.GetEnableChangefeedInitialScan()) {
        return {CreateReject(opId, NKikimrScheme::EStatus::StatusPreconditionFailed, TStringBuilder()
            << "Initial scan is not supported yet")};
    }

    Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
    auto table = context.SS->Tables.at(tablePath.Base()->PathId);

    TVector<TString> boundaries;
    if (op.HasTopicPartitions()) {
        if (op.GetTopicPartitions() <= 0) {
            return {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, "Topic partitions count must be greater than 0")};
        }

        const auto& keyColumns = table->KeyColumnIds;
        const auto& columns = table->Columns;

        Y_ABORT_UNLESS(!keyColumns.empty());
        Y_ABORT_UNLESS(columns.contains(keyColumns.at(0)));
        const auto firstKeyColumnType = columns.at(keyColumns.at(0)).PType;

        if (!TSchemeShard::FillUniformPartitioning(boundaries, keyColumns.size(), firstKeyColumnType, op.GetTopicPartitions(), AppData()->TypeRegistry, errStr)) {
            return {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, errStr)};
        }
    } else {
        const auto& partitions = table->GetPartitions();
        boundaries.reserve(partitions.size() - 1);

        for (ui32 i = 0; i < partitions.size(); ++i) {
            const auto& partition = partitions.at(i);
            if (i != partitions.size() - 1) {
                boundaries.push_back(partition.EndOfRange);
            }
        }
    }

    TVector<ISubOperation::TPtr> result;

    if (initialScan) {
        auto outTx = TransactionTemplate(workingDirPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateLock);
        outTx.SetFailOnExist(false);
        outTx.SetInternal(true);
        outTx.MutableLockConfig()->SetName(tablePath.LeafName());

        result.push_back(CreateLock(NextPartId(opId, result), outTx));
    }

    if (workingDirPath.IsTableIndex()) {
        auto outTx = TransactionTemplate(workingDirPath.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpAlterTableIndex);
        outTx.MutableAlterTableIndex()->SetName(workingDirPath.LeafName());
        outTx.MutableAlterTableIndex()->SetState(NKikimrSchemeOp::EIndexState::EIndexStateReady);

        result.push_back(CreateAlterTableIndex(NextPartId(opId, result), outTx));
    }

    {
        auto outTx = TransactionTemplate(tablePath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStreamImpl);
        outTx.SetFailOnExist(!acceptExisted);
        outTx.MutableCreateCdcStream()->CopyFrom(op);

        if (initialScan) {
            outTx.MutableLockGuard()->SetOwnerTxId(ui64(opId.GetTxId()));
        }

        result.push_back(CreateNewCdcStreamImpl(NextPartId(opId, result), outTx));
    }

    {
        auto outTx = TransactionTemplate(workingDirPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStreamAtTable);
        outTx.SetFailOnExist(!acceptExisted);
        outTx.MutableCreateCdcStream()->CopyFrom(op);

        if (initialScan) {
            outTx.MutableLockGuard()->SetOwnerTxId(ui64(opId.GetTxId()));
        }

        result.push_back(CreateNewCdcStreamAtTable(NextPartId(opId, result), outTx, initialScan));
    }

    {
        auto outTx = TransactionTemplate(streamPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup);
        outTx.SetFailOnExist(!acceptExisted);

        auto& desc = *outTx.MutableCreatePersQueueGroup();
        desc.SetName("streamImpl");
        desc.SetTotalGroupCount(op.HasTopicPartitions() ? op.GetTopicPartitions() : table->GetPartitions().size());
        desc.SetPartitionPerTablet(2);

        auto& pqConfig = *desc.MutablePQTabletConfig();
        pqConfig.SetTopicName(streamName);
        pqConfig.SetTopicPath(streamPath.Child("streamImpl").PathString());
        pqConfig.SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS);

        auto& partitionConfig = *pqConfig.MutablePartitionConfig();
        partitionConfig.SetLifetimeSeconds(op.GetRetentionPeriodSeconds());
        partitionConfig.SetWriteSpeedInBytesPerSecond(1_MB); // TODO: configurable write speed
        partitionConfig.SetBurstSize(1_MB); // TODO: configurable burst
        partitionConfig.SetMaxCountInPartition(Max<i32>());

        for (const auto& tag : table->KeyColumnIds) {
            Y_ABORT_UNLESS(table->Columns.contains(tag));
            const auto& column = table->Columns.at(tag);

            auto& keyComponent = *pqConfig.AddPartitionKeySchema();
            keyComponent.SetName(column.Name);
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(column.PType, column.PTypeMod);
            keyComponent.SetTypeId(columnType.TypeId);
            if (columnType.TypeInfo) {
                *keyComponent.MutableTypeInfo() = *columnType.TypeInfo;
            }
        }

        for (const auto& serialized : boundaries) {
            TSerializedCellVec endKey(serialized);
            Y_ABORT_UNLESS(endKey.GetCells().size() <= table->KeyColumnIds.size());

            TString errStr;
            auto& boundary = *desc.AddPartitionBoundaries();
            for (ui32 ki = 0; ki < endKey.GetCells().size(); ++ki) {
                const auto& cell = endKey.GetCells()[ki];
                const auto tag = table->KeyColumnIds.at(ki);
                Y_ABORT_UNLESS(table->Columns.contains(tag));
                const auto typeId = table->Columns.at(tag).PType;
                const bool ok = NMiniKQL::CellToValue(typeId, cell, *boundary.AddTuple(), errStr);
                Y_ABORT_UNLESS(ok, "Failed to build key tuple at position %" PRIu32 " error: %s", ki, errStr.data());
            }
        }

        result.push_back(CreateNewPQ(NextPartId(opId, result), outTx));
    }

    return result;
}

}
