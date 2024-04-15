#include "defs.h"

#include "datashard_write_operation.h"
#include "datashard_kqp.h"
#include "datashard_locks.h"
#include "datashard_impl.h"
#include "datashard_failpoints.h"

#include "key_conflicts.h"
#include "range_ops.h"

#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <ydb/library/actors/util/memory_track.h>

namespace NKikimr {
namespace NDataShard {

TValidatedWriteTx::TValidatedWriteTx(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx, ui64 globalTxId, TInstant receivedAt, const TRowVersion& readVersion, const TRowVersion& writeVersion, const NEvents::TDataEvents::TEvWrite::TPtr& ev)
    : Ev(ev)
    , UserDb(*self, txc.DB, globalTxId, readVersion, writeVersion, EngineHostCounters, TAppData::TimeProvider->Now())
    , KeyValidator(*self, txc.DB)
    , TabletId(self->TabletID())
    , Ctx(ctx)
    , ReceivedAt(receivedAt)
    , TxSize(0)
    , ErrCode(NKikimrTxDataShard::TError::OK)
    , IsReleased(false)
{
    ComputeTxSize();
    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Add(TxSize);

    if (LockTxId()) {
        UserDb.SetLockTxId(LockTxId());
        UserDb.SetLockNodeId(LockNodeId());
    }

    if (Immediate())
        UserDb.SetIsImmediateTx(true);

    NKikimrTxDataShard::TKqpTransaction::TDataTaskMeta meta;

    LOG_TRACE_S(Ctx, NKikimrServices::TX_DATASHARD, "Parsing write transaction for " << globalTxId << " at " << TabletId << ", record: " << GetRecord().ShortDebugString());

    if (!ParseRecord(self->TableInfos))
        return;

    SetTxKeys(RecordOperation().GetColumnIds());

    KqpSetTxLocksKeys(GetKqpLocks(), self->SysLocksTable(), KeyValidator);
    KeyValidator.GetInfo().SetLoaded();
}

TValidatedWriteTx::~TValidatedWriteTx() {
    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Sub(TxSize);
}

bool TValidatedWriteTx::ParseRecord(const TDataShard::TTableInfos& tableInfos) {
    if (GetRecord().GetOperations().size() != 1)
    {
        ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
        ErrStr = TStringBuilder() << "Only one operation is supported now.";
        return false;
    }

    const NKikimrDataEvents::TTableId& tableIdRecord = RecordOperation().GetTableId();

    auto tableInfoPtr = tableInfos.FindPtr(tableIdRecord.GetTableId());
    if (!tableInfoPtr) {
        ErrCode = NKikimrTxDataShard::TError::SCHEME_ERROR;
        ErrStr = TStringBuilder() << "Table '" << tableIdRecord.GetTableId() << "' doesn't exist.";
        return false;
    }
    TableInfo = tableInfoPtr->Get();
    Y_ABORT_UNLESS(TableInfo);

    if (TableInfo->GetTableSchemaVersion() != 0 && tableIdRecord.GetSchemaVersion() != TableInfo->GetTableSchemaVersion())
    {
        ErrCode = NKikimrTxDataShard::TError::SCHEME_CHANGED;
        ErrStr = TStringBuilder() << "Table '" << TableInfo->Path << "' scheme changed.";
        return false;
    }

    if (RecordOperation().GetPayloadFormat() != NKikimrDataEvents::FORMAT_CELLVEC)
    {
        ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
        ErrStr = TStringBuilder() << "Only FORMAT_CELLVEC is supported now. Got: " << RecordOperation().GetPayloadFormat();
        return false;
    }

    NEvWrite::TPayloadHelper<NEvents::TDataEvents::TEvWrite> payloadHelper(*Ev->Get());
    TString payload = payloadHelper.GetDataFromPayload(RecordOperation().GetPayloadIndex());

    if (!TSerializedCellMatrix::TryParse(payload,Matrix))
    {
        ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
        ErrStr = TStringBuilder() << "Can't parse TSerializedCellVec in payload";
        return false;
    }

    const auto& columnTags = RecordOperation().GetColumnIds();
    if ((size_t)columnTags.size() != Matrix.GetColCount())
    {
        ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
        ErrStr = TStringBuilder() << "Column count mismatch: got columnids " << columnTags.size() << ", got cells count " <<Matrix.GetColCount();
        return false;
    }

    if ((size_t)columnTags.size() < TableInfo->KeyColumnIds.size())
    {
        ErrCode = NKikimrTxDataShard::TError::SCHEME_ERROR;
        ErrStr = TStringBuilder() << "Column count mismatch: got " << columnTags.size() << ", expected greater or equal than key column count " << TableInfo->KeyColumnIds.size();
        return false;
    }

    for (size_t i = 0; i < TableInfo->KeyColumnIds.size(); ++i) {
        if (RecordOperation().columnids(i) != TableInfo->KeyColumnIds[i]) {
            ErrCode = NKikimrTxDataShard::TError::SCHEME_ERROR;
            ErrStr = TStringBuilder() << "Key column schema at position " << i;
            return false;
        }
    }

    for (ui32 columnTag : columnTags) {
        auto* col = TableInfo->Columns.FindPtr(columnTag);
        if (!col) {
            ErrCode = NKikimrTxDataShard::TError::SCHEME_ERROR;
            ErrStr = TStringBuilder() << "Missing column with id " << columnTag;
            return false;
        }
    }

    for (ui32 rowIdx = 0; rowIdx < Matrix.GetRowCount(); ++rowIdx)
    {
        ui64 keyBytes = 0;
        for (ui16 keyColIdx = 0; keyColIdx < TableInfo->KeyColumnIds.size(); ++keyColIdx) {
            const auto& cellType = TableInfo->KeyColumnTypes[keyColIdx];
            const TCell& cell = Matrix.GetCell(rowIdx, keyColIdx);
            if (cellType.GetTypeId() == NScheme::NTypeIds::Uint8 && !cell.IsNull() && cell.AsValue<ui8>() > 127) {
                ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
                ErrStr = TStringBuilder() << "Keys with Uint8 column values >127 are currently prohibited";
                return false;
            }
            keyBytes += cell.IsNull() ? 1 : cell.Size();
        }

        if (keyBytes > NLimits::MaxWriteKeySize) {
            ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
            ErrStr = TStringBuilder() << "Row key size of " << keyBytes << " bytes is larger than the allowed threshold " << NLimits::MaxWriteKeySize;
            return false;
        }

        for (ui16 valueColIdx = TableInfo->KeyColumnIds.size(); valueColIdx < Matrix.GetColCount(); ++valueColIdx) {
            const TCell& cell = Matrix.GetCell(rowIdx, valueColIdx);
            if (cell.Size() > NLimits::MaxWriteValueSize) {
                ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
                ErrStr = TStringBuilder() << "Row cell size of " << cell.Size() << " bytes is larger than the allowed threshold " << NLimits::MaxWriteValueSize;
                return false;
            }
        }
    }    

    TableId = TTableId(tableIdRecord.GetOwnerId(), tableIdRecord.GetTableId(), tableIdRecord.GetSchemaVersion());
    return true;
}

TVector<TKeyValidator::TColumnWriteMeta> GetColumnWrites(const ::google::protobuf::RepeatedField<::NProtoBuf::uint32>& columnTags) {
    TVector<TKeyValidator::TColumnWriteMeta> writeColumns;
    writeColumns.reserve(columnTags.size());
    for (ui32 columnTag : columnTags) {
        TKeyValidator::TColumnWriteMeta writeColumn;
        writeColumn.Column = NTable::TColumn("", columnTag, {}, {});

        writeColumns.push_back(std::move(writeColumn));
    }

    return writeColumns;
}

void TValidatedWriteTx::SetTxKeys(const ::google::protobuf::RepeatedField<::NProtoBuf::uint32>& columnTags)
{
    TVector<TCell> keyCells;
    for (ui32 rowIdx = 0; rowIdx <Matrix.GetRowCount(); ++rowIdx)
    {
        Matrix.GetSubmatrix(rowIdx, rowIdx, 0, TableInfo->KeyColumnIds.size() - 1, keyCells);

        LOG_TRACE_S(Ctx, NKikimrServices::TX_DATASHARD, "Table " << TableInfo->Path << ", shard: " << TabletId << ", "
                                                                 << "write point " << DebugPrintPoint(TableInfo->KeyColumnTypes, keyCells, *AppData()->TypeRegistry));
        TTableRange tableRange(keyCells);
        KeyValidator.AddWriteRange(TableId, tableRange, TableInfo->KeyColumnTypes, GetColumnWrites(columnTags), false);
    }
}

ui32 TValidatedWriteTx::ExtractKeys(bool allowErrors)
{
    SetTxKeys(RecordOperation().GetColumnIds());

    bool isValid = ReValidateKeys();
    Y_ABORT_UNLESS(allowErrors || isValid, "Validation errors: %s", ErrStr.data());

    return KeysCount();
}

bool TValidatedWriteTx::ReValidateKeys()
{
    using EResult = NMiniKQL::IEngineFlat::EResult;

    TKeyValidator::TValidateOptions options(UserDb);
    auto [result, error] = GetKeyValidator().ValidateKeys(options);
    if (result != EResult::Ok) {
        ErrStr = std::move(error);
        ErrCode = ConvertErrCode(result);
        return false;
    }

    return true;
}

bool TValidatedWriteTx::CanCancel() {
    return false;
}

bool TValidatedWriteTx::CheckCancelled() {
    return false;
}

void TValidatedWriteTx::ReleaseTxData() {
    IsReleased = true;

    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Sub(TxSize);
    ComputeTxSize();
    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Add(TxSize);
}

void TValidatedWriteTx::ComputeTxSize() {
    TxSize = sizeof(TValidatedWriteTx);
}

TWriteOperation* TWriteOperation::CastWriteOperation(TOperation::TPtr op)
{
    Y_ABORT_UNLESS(op->IsWriteTx());
    TWriteOperation* writeOp = dynamic_cast<TWriteOperation*>(op.Get());
    Y_ABORT_UNLESS(writeOp);
    return writeOp;
}

TWriteOperation::TWriteOperation(const TBasicOpInfo& op, NEvents::TDataEvents::TEvWrite::TPtr ev, TDataShard* self, TTransactionContext& txc, const TActorContext& ctx)
    : TOperation(op)
    , Ev(ev)
    , TabletId(self->TabletID())
    , Ctx(ctx)
    , ArtifactFlags(0)
    , TxCacheUsage(0)
    , ReleasedTxDataSize(0)
    , SchemeShardId(0)
    , SubDomainPathId(0)
{
    SetTarget(Ev->Sender);
    SetCookie(Ev->Cookie);
    Orbit = std::move(Ev->Get()->MoveOrbit());

    BuildWriteTx(self, txc, ctx);

    TrackMemory();
}

TWriteOperation::~TWriteOperation()
{
    UntrackMemory();
}

void TWriteOperation::FillTxData(TValidatedWriteTx::TPtr writeTx)
{
    Y_ABORT_UNLESS(!WriteTx);
    Y_ABORT_UNLESS(!Ev || HasVolatilePrepareFlag());

    Target = writeTx->GetSource();
    WriteTx = writeTx;
}

void TWriteOperation::FillTxData(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx, const TActorId& target, NEvents::TDataEvents::TEvWrite::TPtr&& ev, const TVector<TSysTables::TLocksTable::TLock>& locks, ui64 artifactFlags)
{
    UntrackMemory();

    Y_ABORT_UNLESS(!WriteTx);
    Y_ABORT_UNLESS(!Ev);

    Target = target;
    Ev = std::move(ev);
    if (locks.size()) {
        for (auto lock : locks)
            LocksCache().Locks[lock.LockId] = lock;
    }
    ArtifactFlags = artifactFlags;
    Y_ABORT_UNLESS(!WriteTx);
    BuildWriteTx(self, txc, ctx);
    Y_ABORT_UNLESS(WriteTx->Ready());

    TrackMemory();
}

void TWriteOperation::FillVolatileTxData(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx)
{
    UntrackMemory();

    Y_ABORT_UNLESS(!WriteTx);
    Y_ABORT_UNLESS(Ev);

    BuildWriteTx(self, txc, ctx);
    Y_ABORT_UNLESS(WriteTx->Ready());


    TrackMemory();
}

TValidatedWriteTx::TPtr TWriteOperation::BuildWriteTx(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx)
{
    if (!WriteTx) {
        Y_ABORT_UNLESS(Ev);
        auto [readVersion, writeVersion] = self->GetReadWriteVersions(this);
        WriteTx = std::make_shared<TValidatedWriteTx>(self, txc, ctx, GetGlobalTxId(), GetReceivedAt(), readVersion, writeVersion, Ev);
    }
    return WriteTx;
}

void TWriteOperation::ReleaseTxData(NTabletFlatExecutor::TTxMemoryProviderBase& provider, const TActorContext& ctx) {
    ReleasedTxDataSize = provider.GetMemoryLimit() + provider.GetRequestedMemory();

    if (!WriteTx || IsTxDataReleased())
        return;

    WriteTx->ReleaseTxData();
    // Immediate transactions have no body stored.
    if (!IsImmediate() && !HasVolatilePrepareFlag()) {
        UntrackMemory();
        Ev.Reset();
        TrackMemory();
    }

    //InReadSets.clear();
    OutReadSets().clear();
    LocksAccessLog().Locks.clear();
    LocksCache().Locks.clear();
    ArtifactFlags = 0;

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "tx " << GetTxId() << " released its data");
}

void TWriteOperation::DbStoreLocksAccessLog(NTable::TDatabase& txcDb)
{
    using Schema = TDataShard::Schema;

    NIceDb::TNiceDb db(txcDb);

    using TLocksVector = TVector<TSysTables::TLocksTable::TPersistentLock>;
    TLocksVector vec;
    vec.reserve(LocksAccessLog().Locks.size());
    for (auto& pr : LocksAccessLog().Locks)
        vec.emplace_back(pr.second);

    // Historically C++ column type was TVector<TLock>
    const char* vecDataStart = reinterpret_cast<const char*>(vec.data());
    size_t vecDataSize = vec.size() * sizeof(TLocksVector::value_type);
    TStringBuf vecData(vecDataStart, vecDataSize);
    db.Table<Schema::TxArtifacts>().Key(GetTxId()).Update(NIceDb::TUpdate<Schema::TxArtifacts::Locks>(vecData));

    LOG_TRACE_S(Ctx, NKikimrServices::TX_DATASHARD, "Storing " << vec.size() << " locks for txid=" << GetTxId() << " in " << TabletId);
}

void TWriteOperation::DbStoreArtifactFlags(NTable::TDatabase& txcDb)
{
    using Schema = TDataShard::Schema;

    NIceDb::TNiceDb db(txcDb);
    db.Table<Schema::TxArtifacts>().Key(GetTxId()).Update<Schema::TxArtifacts::Flags>(ArtifactFlags);

    LOG_TRACE_S(Ctx, NKikimrServices::TX_DATASHARD, "Storing artifactflags=" << ArtifactFlags << " for txid=" << GetTxId() << " in " << TabletId);
}

ui64 TWriteOperation::GetMemoryConsumption() const {
    ui64 res = 0;
    if (WriteTx) {
        res += WriteTx->GetTxSize();
    }
    if (Ev) {
        res += sizeof(NEvents::TDataEvents::TEvWrite);
    }

    return res;
}

ERestoreDataStatus TWriteOperation::RestoreTxData(
    TDataShard* self,
    TTransactionContext& txc,
    const TActorContext& ctx
)
{
    // TODO
    Y_UNUSED(self);
    Y_UNUSED(txc);
    Y_UNUSED(ctx);
    Y_ABORT();
    /*
    if (!WriteTx) {
        ReleasedTxDataSize = 0;
        return ERestoreDataStatus::Ok;
    }

    UntrackMemory();

    // For immediate transactions we should restore just
    // from the Ev. For planned transaction we should
    // restore from local database.

    TVector<TSysTables::TLocksTable::TLock> locks;
    if (!IsImmediate() && !HasVolatilePrepareFlag()) {
        NIceDb::TNiceDb db(txc.DB);
        bool ok = self->TransQueue.LoadTxDetails(db, GetTxId(), Target, Ev, locks, ArtifactFlags);
        if (!ok) {
            Ev.Reset();
            ArtifactFlags = 0;
            return ERestoreDataStatus::Restart;
        }
    } else {
        Y_ABORT_UNLESS(Ev);
    }

    TrackMemory();

    for (auto& lock : locks)
        LocksCache().Locks[lock.LockId] = lock;

    bool extractKeys = WriteTx->IsTxInfoLoaded();
    auto [readVersion, writeVersion] = self->GetReadWriteVersions(this);
    WriteTx = std::make_shared<TValidatedWriteTx>(self, txc, ctx, GetStepOrder(), GetReceivedAt(), readVersion, writeVersion, Ev);
    if (WriteTx->Ready() && extractKeys) {
        WriteTx->ExtractKeys();
    }

    if (!WriteTx->Ready()) {
        return ERestoreDataStatus::Error;
    }

    ReleasedTxDataSize = 0;
    */

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "tx " << GetTxId() << " at " << self->TabletID() << " restored its data");

    return ERestoreDataStatus::Ok;
}

void TWriteOperation::FinalizeWriteTxPlan()
{
    Y_ABORT_UNLESS(IsWriteTx());
    Y_ABORT_UNLESS(!IsImmediate());
    Y_ABORT_UNLESS(!IsKqpScanTransaction());

    TVector<EExecutionUnitKind> plan;

    plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
    if (IsKqpDataTransaction()) {
        plan.push_back(EExecutionUnitKind::BuildKqpDataTxOutRS);
        plan.push_back(EExecutionUnitKind::StoreAndSendOutRS);
        plan.push_back(EExecutionUnitKind::PrepareKqpDataTxInRS);
        plan.push_back(EExecutionUnitKind::LoadAndWaitInRS);
        plan.push_back(EExecutionUnitKind::ExecuteKqpDataTx);
    } else {
        plan.push_back(EExecutionUnitKind::BuildDataTxOutRS);
        plan.push_back(EExecutionUnitKind::StoreAndSendOutRS);
        plan.push_back(EExecutionUnitKind::PrepareDataTxInRS);
        plan.push_back(EExecutionUnitKind::LoadAndWaitInRS);
        plan.push_back(EExecutionUnitKind::ExecuteDataTx);
    }
    plan.push_back(EExecutionUnitKind::CompleteOperation);
    plan.push_back(EExecutionUnitKind::CompletedOperations);

    RewriteExecutionPlan(plan);
}

class TFinalizeWriteTxPlanUnit: public TExecutionUnit {
public:
    TFinalizeWriteTxPlanUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::FinalizeWriteTxPlan, false, dataShard, pipeline)
    {
    }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(txc);
        Y_UNUSED(ctx);

        TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);

        writeOp->FinalizeWriteTxPlan();

        return EExecutionStatus::Executed;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        Y_UNUSED(op);
        Y_UNUSED(ctx);
    }
};

THolder<TExecutionUnit> CreateFinalizeWriteTxPlanUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new TFinalizeWriteTxPlanUnit(dataShard, pipeline));
}

void TWriteOperation::OnCleanup(TDataShard& self, std::vector<std::unique_ptr<IEventHandle>>& replies) {
    if (!IsImmediate() && GetTarget() && !HasCompletedFlag()) {
        auto status = NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED;

        TString reason;
        if (self.State == TShardState::SplitSrcWaitForNoTxInFlight) {
            reason = TStringBuilder()
                << "DataShard " << self.TabletID() << " is splitting";
        } else if (self.Pipeline.HasWaitingSchemeOps()) {
            reason = TStringBuilder()
                << "DataShard " << self.TabletID() << " is blocked by a schema operation";
        } else {
            reason = "Transaction was cleaned up";
        }

        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(self.TabletID(), GetTxId(), status, reason);

        replies.push_back(std::make_unique<IEventHandle>(GetTarget(), self.SelfId(), result.release(), 0, GetCookie()));
    }
}

void TWriteOperation::TrackMemory() const {
    // TODO More accurate calc memory
    NActors::NMemory::TLabel<MemoryLabelActiveTransactionBody>::Add(GetRecord().SpaceUsed());
}

void TWriteOperation::UntrackMemory() const {
    NActors::NMemory::TLabel<MemoryLabelActiveTransactionBody>::Sub(GetRecord().SpaceUsed());
}

void TWriteOperation::SetError(const NKikimrDataEvents::TEvWriteResult::EStatus& status, const TString& errorMsg) {
    SetAbortedFlag();
    WriteResult = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletId, GetTxId(), status, errorMsg);
}

void TWriteOperation::SetWriteResult(std::unique_ptr<NEvents::TDataEvents::TEvWriteResult>&& writeResult) {
    WriteResult = std::move(writeResult);
}

void TWriteOperation::BuildExecutionPlan(bool loaded)
{
    Y_ABORT_UNLESS(GetExecutionPlan().empty());
    Y_ABORT_UNLESS(!loaded);

    TVector<EExecutionUnitKind> plan;

    //if (IsImmediate()) 
    {
        Y_ABORT_UNLESS(!loaded);
        plan.push_back(EExecutionUnitKind::CheckWrite);
        plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
        plan.push_back(EExecutionUnitKind::ExecuteWrite);
        plan.push_back(EExecutionUnitKind::FinishProposeWrite);
        plan.push_back(EExecutionUnitKind::CompletedOperations);
    } 
    /*
    else if (HasVolatilePrepareFlag()) {
        plan.push_back(EExecutionUnitKind::StoreDataTx);  // note: stores in memory
        plan.push_back(EExecutionUnitKind::FinishProposeWrite);
        Y_ABORT_UNLESS(!GetStep());
        plan.push_back(EExecutionUnitKind::WaitForPlan);
        plan.push_back(EExecutionUnitKind::PlanQueue);
        plan.push_back(EExecutionUnitKind::LoadTxDetails);  // note: reloads from memory
        plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
        plan.push_back(EExecutionUnitKind::ExecuteWrite);
        plan.push_back(EExecutionUnitKind::CompleteOperation);
        plan.push_back(EExecutionUnitKind::CompletedOperations);
    } else {
        if (!loaded) {
            plan.push_back(EExecutionUnitKind::CheckWrite);
            plan.push_back(EExecutionUnitKind::StoreDataTx);
            plan.push_back(EExecutionUnitKind::FinishProposeWrite);
        }
        if (!GetStep())
            plan.push_back(EExecutionUnitKind::WaitForPlan);
        plan.push_back(EExecutionUnitKind::PlanQueue);
        plan.push_back(EExecutionUnitKind::LoadTxDetails);
        plan.push_back(EExecutionUnitKind::FinalizeWriteTxPlan);
    } */
    RewriteExecutionPlan(plan);
}

}  // NDataShard
}  // NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::NDataShard::TWriteOperation, stream, tx) {
    stream << '[' << tx.GetStep() << ':' << tx.GetTxId() << ']';
}
