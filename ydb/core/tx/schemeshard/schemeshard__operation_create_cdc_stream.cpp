#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>

#define LOG_D(stream) LOG_DEBUG_S (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr {
namespace NSchemeShard {

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
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxCreateCdcStream);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const auto step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << "HandleReply TEvOperationPlan"
            << ": step# " << step);

        const auto* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxCreateCdcStream);
        const auto& pathId = txState->TargetPathId;

        Y_VERIFY(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_VERIFY(context.SS->CdcStreams.contains(pathId));
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

    static TTxState::ETxState NextState(TTxState::ETxState state) {
        switch (state) {
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) {
        switch (state) {
        case TTxState::Propose:
            return THolder(new TPropose(OperationId));
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
    explicit TNewCdcStream(TOperationId id, const TTxTransaction& tx)
        : OperationId(id)
        , Transaction(tx)
        , State(TTxState::Invalid)
    {
    }

    explicit TNewCdcStream(TOperationId id, TTxState::ETxState state)
        : OperationId(id)
        , State(state)
    {
        SetState(SelectStateFunc(state));
    }

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
                .IsCommonSensePath()
                .NotUnderDeleting();

            if (!checks) {
                TString explain = TStringBuilder() << "path checks failed, path: " << tablePath.PathString();
                const auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
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
                TString explain = TStringBuilder() << "path checks failed, path: " << streamPath.PathString();
                const auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
                if (streamPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(streamPath.Base()->CreateTxId));
                    result->SetPathId(streamPath.Base()->PathId.LocalPathId);
                }
                return result;
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
            result->SetError(NKikimrScheme::StatusInvalidParameter, TStringBuilder()
                << "Invalid stream mode: " << static_cast<ui32>(streamDesc.GetMode()));
            return result;
        }

        switch (streamDesc.GetFormat()) {
        case NKikimrSchemeOp::ECdcStreamFormatProto:
        case NKikimrSchemeOp::ECdcStreamFormatJson:
            break;
        default:
            result->SetError(NKikimrScheme::StatusInvalidParameter, TStringBuilder()
                << "Invalid stream format: " << static_cast<ui32>(streamDesc.GetFormat()));
            return result;
        }

        TString errStr;
        if (!context.SS->CheckLocks(tablePath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        auto stream = TCdcStreamInfo::Create(streamDesc);
        Y_VERIFY(stream);

        auto guard = context.DbGuard();
        const auto pathId = context.SS->AllocatePathId();
        context.MemChanges.GrabNewPath(context.SS, pathId);
        context.MemChanges.GrabPath(context.SS, tablePath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabDomain(context.SS, streamPath.GetPathIdForDomain());
        context.MemChanges.GrabNewCdcStream(context.SS, pathId);

        context.DbChanges.PersistPath(pathId);
        context.DbChanges.PersistPath(tablePath.Base()->PathId);
        context.DbChanges.PersistAlterCdcStream(pathId);
        context.DbChanges.PersistTxState(OperationId);

        streamPath.MaterializeLeaf(owner, pathId);
        result->SetPathId(pathId.LocalPathId);

        Y_VERIFY(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateCdcStream, streamPath.Base()->PathId);
        txState.State = TTxState::Propose;

        streamPath.Base()->PathState = NKikimrSchemeOp::EPathStateCreate;
        streamPath.Base()->CreateTxId = OperationId.GetTxId();
        streamPath.Base()->LastTxId = OperationId.GetTxId();
        streamPath.Base()->PathType = TPathElement::EPathType::EPathTypeCdcStream;

        context.SS->CdcStreams[pathId] = stream;
        context.SS->IncrementPathDbRefCount(pathId);

        streamPath.DomainInfo()->IncPathsInside();
        tablePath.Base()->IncAliveChildren();

        context.OnComplete.ActivateTx(OperationId);

        State = NextState();
        SetState(SelectStateFunc(State));

        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TNewCdcStream");
    }

    void AbortUnsafe(TTxId txId, TOperationContext& context) override {
        LOG_N("TNewCdcStream AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << txId);
        context.OnComplete.DoneOperation(OperationId);
    }

private:
    const TOperationId OperationId;
    const TTxTransaction Transaction;
    TTxState::ETxState State;

}; // TNewCdcStream

class TConfigurePartsAtTable: public NCdcStreamState::TConfigurePartsAtTable {
protected:
    void FillNotice(const TPathId& pathId, NKikimrTxDataShard::TFlatSchemeTransaction& tx, TOperationContext& context) const override {
        Y_VERIFY(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_VERIFY(context.SS->Tables.contains(pathId));
        auto table = context.SS->Tables.at(pathId);

        auto& notice = *tx.MutableCreateCdcStreamNotice();
        PathIdFromPathId(pathId, notice.MutablePathId());
        notice.SetTableSchemaVersion(table->AlterVersion + 1);

        bool found = false;
        for (const auto& [childName, childPathId] : path->GetChildren()) {
            Y_VERIFY(context.SS->PathsById.contains(childPathId));
            auto childPath = context.SS->PathsById.at(childPathId);

            if (!childPath->IsCdcStream() || childPath->Dropped()) {
                continue;
            }

            Y_VERIFY(context.SS->CdcStreams.contains(childPathId));
            auto stream = context.SS->CdcStreams.at(childPathId);

            if (stream->State != TCdcStreamInfo::EState::ECdcStreamStateInvalid) {
                continue;
            }

            Y_VERIFY_S(!found, "Too many cdc streams are planned to create"
                << ": found# " << PathIdFromPathId(notice.GetStreamDescription().GetPathId())
                << ", another# " << childPathId);
            found = true;

            Y_VERIFY(stream->AlterData);
            context.SS->DescribeCdcStream(childPathId, childName, stream->AlterData, *notice.MutableStreamDescription());
        }
    }

public:
    using NCdcStreamState::TConfigurePartsAtTable::TConfigurePartsAtTable;

}; // TConfigurePartsAtTable

class TNewCdcStreamAtTable: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::ConfigureParts;
    }

    static TTxState::ETxState NextState(TTxState::ETxState state) {
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

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            return THolder(new TConfigurePartsAtTable(OperationId));
        case TTxState::Propose:
            return THolder(new NCdcStreamState::TProposeAtTable(OperationId));
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
    explicit TNewCdcStreamAtTable(TOperationId id, const TTxTransaction& tx)
        : OperationId(id)
        , Transaction(tx)
        , State(TTxState::Invalid)
    {
    }

    explicit TNewCdcStreamAtTable(TOperationId id, TTxState::ETxState state)
        : OperationId(id)
        , State(state)
    {
        SetState(SelectStateFunc(state));
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
                .IsCommonSensePath()
                .IsLikeDirectory()
                .NotUnderDeleting();

            if (!checks) {
                TString explain = TStringBuilder() << "path checks failed, path: " << workingDirPath.PathString();
                const auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
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
                .IsCommonSensePath()
                .NotUnderDeleting()
                .NotUnderOperation();

            if (!checks) {
                TString explain = TStringBuilder() << "path checks failed, path: " << tablePath.PathString();
                const auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        context.DbChanges.PersistTxState(OperationId);

        Y_VERIFY(context.SS->Tables.contains(tablePath.Base()->PathId));
        auto table = context.SS->Tables.at(tablePath.Base()->PathId);

        Y_VERIFY(table->AlterVersion != 0);
        Y_VERIFY(!table->AlterData);

        Y_VERIFY(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateCdcStreamAtTable, tablePath.Base()->PathId);
        txState.State = TTxState::ConfigureParts;

        tablePath.Base()->PathState = NKikimrSchemeOp::EPathStateAlter;
        tablePath.Base()->LastTxId = OperationId.GetTxId();

        for (const auto& splitOpId : table->GetSplitOpsInFlight()) {
            context.OnComplete.Dependence(splitOpId.GetTxId(), OperationId.GetTxId());
        }

        context.OnComplete.ActivateTx(OperationId);

        State = NextState();
        SetState(SelectStateFunc(State));

        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TNewCdcStreamAtTable");
    }

    void AbortUnsafe(TTxId txId, TOperationContext& context) override {
        LOG_N("TNewCdcStreamAtTable AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << txId);
        context.OnComplete.DoneOperation(OperationId);
    }

private:
    const TOperationId OperationId;
    const TTxTransaction Transaction;
    TTxState::ETxState State;

}; // TNewCdcStreamAtTable

} // anonymous

ISubOperationBase::TPtr CreateNewCdcStreamImpl(TOperationId id, const TTxTransaction& tx) {
    return new TNewCdcStream(id, tx);
}

ISubOperationBase::TPtr CreateNewCdcStreamImpl(TOperationId id, TTxState::ETxState state) {
    return new TNewCdcStream(id, state);
}

ISubOperationBase::TPtr CreateNewCdcStreamAtTable(TOperationId id, const TTxTransaction& tx) {
    return new TNewCdcStreamAtTable(id, tx);
}

ISubOperationBase::TPtr CreateNewCdcStreamAtTable(TOperationId id, TTxState::ETxState state) {
    return new TNewCdcStreamAtTable(id, state);
}

TVector<ISubOperationBase::TPtr> CreateNewCdcStream(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_VERIFY(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStream);

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
            .IsCommonSensePath()
            .NotUnderDeleting()
            .NotUnderOperation();

        if (!checks) {
            TString explain = TStringBuilder() << "path checks failed, path: " << tablePath.PathString();
            const auto status = checks.GetStatus(&explain);
            return {CreateReject(opId, status, explain)};
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
            TString explain = TStringBuilder() << "path checks failed, path: " << streamPath.PathString();
            const auto status = checks.GetStatus(&explain);
            return {CreateReject(opId, status, explain)};
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

    const auto retentionPeriod = TDuration::Seconds(op.GetRetentionPeriodSeconds());
    if (retentionPeriod.Seconds() > TSchemeShard::MaxPQLifetimeSeconds) {
        return {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, TStringBuilder()
            << "Invalid retention period specified: " << retentionPeriod.Seconds()
            << ", limit: " << TSchemeShard::MaxPQLifetimeSeconds)};
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

    TVector<ISubOperationBase::TPtr> result;

    {
        auto outTx = TransactionTemplate(tablePath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStreamImpl);
        outTx.SetFailOnExist(!acceptExisted);
        outTx.MutableCreateCdcStream()->CopyFrom(op);

        result.push_back(CreateNewCdcStreamImpl(NextPartId(opId, result), outTx));
    }

    {
        auto outTx = TransactionTemplate(workingDirPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStreamAtTable);
        outTx.SetFailOnExist(!acceptExisted);
        outTx.MutableCreateCdcStream()->CopyFrom(op);

        result.push_back(CreateNewCdcStreamAtTable(NextPartId(opId, result), outTx));
    }

    {
        Y_VERIFY(context.SS->Tables.contains(tablePath.Base()->PathId));
        auto table = context.SS->Tables.at(tablePath.Base()->PathId);
        const auto& partitions = table->GetPartitions();

        auto outTx = TransactionTemplate(streamPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup);
        outTx.SetFailOnExist(!acceptExisted);

        auto& desc = *outTx.MutableCreatePersQueueGroup();
        desc.SetName("streamImpl");
        desc.SetTotalGroupCount(partitions.size());
        desc.SetPartitionPerTablet(2);

        auto& pqConfig = *desc.MutablePQTabletConfig();
        pqConfig.SetTopicName(streamName);
        pqConfig.SetTopicPath(streamPath.Child("streamImpl").PathString());
        pqConfig.SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_SERVERLESS);
        auto& partitionConfig = *pqConfig.MutablePartitionConfig();
        partitionConfig.SetLifetimeSeconds(retentionPeriod.Seconds());

        for (const auto& tag : table->KeyColumnIds) {
            Y_VERIFY(table->Columns.contains(tag));
            const auto& column = table->Columns.at(tag);

            auto& keyComponent = *pqConfig.AddPartitionKeySchema();
            keyComponent.SetName(column.Name);
            keyComponent.SetTypeId(column.PType);
        }

        auto& bootstrapConfig = *desc.MutableBootstrapConfig();
        for (ui32 i = 0; i < partitions.size(); ++i) {
            const auto& cur = partitions.at(i);

            Y_VERIFY(context.SS->ShardInfos.contains(cur.ShardIdx));
            const auto& shard = context.SS->ShardInfos.at(cur.ShardIdx);

            auto& mg = *bootstrapConfig.AddExplicitMessageGroups();
            mg.SetId(NPQ::NSourceIdEncoding::EncodeSimple(ToString(shard.TabletID)));

            if (i != partitions.size() - 1) {
                TSerializedCellVec endKey(cur.EndOfRange);
                Y_VERIFY(endKey.GetCells().size() <= table->KeyColumnIds.size());

                TString errStr;
                auto& boundary = *desc.AddPartitionBoundaries();
                for (ui32 ki = 0; ki < endKey.GetCells().size(); ++ki) {
                    const auto& cell = endKey.GetCells()[ki];
                    const auto tag = table->KeyColumnIds.at(ki);
                    Y_VERIFY(table->Columns.contains(tag));
                    const auto typeId = table->Columns.at(tag).PType;
                    const bool ok = NMiniKQL::CellToValue(typeId, cell, *boundary.AddTuple(), errStr);
                    Y_VERIFY(ok, "Failed to build key tuple at postition %" PRIu32 " error: %s", ki, errStr.data());
                }

                mg.MutableKeyRange()->SetToBound(cur.EndOfRange);
            }

            if (i) {
                const auto& prev = partitions.at(i - 1);
                mg.MutableKeyRange()->SetFromBound(prev.EndOfRange);
            }
        }

        result.push_back(CreateNewPQ(NextPartId(opId, result), outTx));
    }

    return result;
}

} // NSchemeShard
} // NKikimr
