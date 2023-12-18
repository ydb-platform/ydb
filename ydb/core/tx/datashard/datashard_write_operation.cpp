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



TValidatedWriteTx::TValidatedWriteTx(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx, const TStepOrder& stepTxId, TInstant receivedAt, const NEvents::TDataEvents::TEvWrite::TPtr& ev)
    : StepTxId_(stepTxId)
    , TabletId_(self->TabletID())
    , Ev_(ev)
    , EngineBay(self, txc, ctx, stepTxId.ToPair())
    , ErrCode(NKikimrTxDataShard::TError::OK)
    , TxSize(0)
    , TxCacheUsage(0)
    , IsReleased(false)
    , ReceivedAt_(receivedAt)
{
    ComputeTxSize();
    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Add(TxSize);

    if (LockTxId())
        EngineBay.SetLockTxId(LockTxId(), LockNodeId());

    if (Immediate())
        EngineBay.SetIsImmediateTx();

    auto& typeRegistry = *AppData()->TypeRegistry;

    NKikimrTxDataShard::TKqpTransaction::TDataTaskMeta meta;

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "TxId: " << StepTxId_.TxId << ", shard " << TabletId() << ", meta: " << Record().ShortDebugString());

    if (!ParseRecord(self))
        return;

    SetTxKeys(RecordOperation().GetColumnIds(), typeRegistry, ctx);

    KqpSetTxLocksKeys(GetKqpLocks(), self->SysLocksTable(), EngineBay);
    EngineBay.MarkTxLoaded();
}

TValidatedWriteTx::~TValidatedWriteTx() {
    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Sub(TxSize);
}

bool TValidatedWriteTx::ParseRecord(TDataShard* self) {
    if (Record().GetOperations().size() != 1)
    {
        ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
        ErrStr = TStringBuilder() << "Only one operation is supported now.";
        return false;
    }

    const NKikimrDataEvents::TTableId& tableIdRecord = RecordOperation().GetTableId();

    auto tableInfoPtr = self->TableInfos.FindPtr(tableIdRecord.GetTableId());
    if (!tableInfoPtr) {
        ErrCode = NKikimrTxDataShard::TError::SCHEME_ERROR;
        ErrStr = TStringBuilder() << "Table '" << tableIdRecord.GetTableId() << "' doesn't exist";
        return false;
    }
    TableInfo_ = tableInfoPtr->Get();
    Y_ABORT_UNLESS(TableInfo_);

    if (TableInfo_->GetTableSchemaVersion() != 0 && tableIdRecord.GetSchemaVersion() != TableInfo_->GetTableSchemaVersion())
    {
        ErrCode = NKikimrTxDataShard::TError::SCHEME_CHANGED;
        ErrStr = TStringBuilder() << "Table '" << TableInfo_->Path << "' scheme changed.";
        return false;
    }

    if (RecordOperation().GetPayloadFormat() != NKikimrDataEvents::FORMAT_CELLVEC)
    {
        ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
        ErrStr = TStringBuilder() << "Only FORMAT_CELLVEC is supported now. Got: " << RecordOperation().GetPayloadFormat();
        return false;
    }

    NEvWrite::TPayloadHelper<NEvents::TDataEvents::TEvWrite> payloadHelper(*Ev_->Get());
    TString payload = payloadHelper.GetDataFromPayload(RecordOperation().GetPayloadIndex());

    if (!TSerializedCellMatrix::TryParse(payload, Matrix_))
    {
        ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
        ErrStr = TStringBuilder() << "Can't parse TSerializedCellVec in payload";
        return false;
    }

    const auto& columnTags = RecordOperation().GetColumnIds();
    if ((size_t)columnTags.size() != Matrix_.GetColCount())
    {
        ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
        ErrStr = TStringBuilder() << "Column count mismatch: got columnids " << columnTags.size() << ", got cells count " << Matrix_.GetColCount();
        return false;
    }

    if ((size_t)columnTags.size() < TableInfo_->KeyColumnIds.size())
    {
        ErrCode = NKikimrTxDataShard::TError::SCHEME_ERROR;
        ErrStr = TStringBuilder() << "Column count mismatch: got " << columnTags.size() << ", expected greater or equal than key column count " << TableInfo_->KeyColumnIds.size();
        return false;
    }

    for (size_t i = 0; i < TableInfo_->KeyColumnIds.size(); ++i) {
        if (RecordOperation().columnids(i) != TableInfo_->KeyColumnIds[i]) {
            ErrCode = NKikimrTxDataShard::TError::SCHEME_ERROR;
            ErrStr = TStringBuilder() << "Key column schema at position " << i;
            return false;
        }
    }

    for (ui32 columnTag : columnTags) {
        auto* col = TableInfo_->Columns.FindPtr(columnTag);
        if (!col) {
            ErrCode = NKikimrTxDataShard::TError::SCHEME_ERROR;
            ErrStr = TStringBuilder() << "Missing column with id " << columnTag;
            return false;
        }
    }

    TableId_ = TTableId(tableIdRecord.ownerid(), tableIdRecord.GetTableId(), tableIdRecord.GetSchemaVersion());
    return true;
}

TVector<TEngineBay::TColumnWriteMeta> GetColumnWrites(const ::google::protobuf::RepeatedField<::NProtoBuf::uint32>& columnTags) {
    TVector<TEngineBay::TColumnWriteMeta> writeColumns;
    writeColumns.reserve(columnTags.size());
    for (ui32 columnTag : columnTags) {
        TEngineBay::TColumnWriteMeta writeColumn;
        writeColumn.Column = NTable::TColumn("", columnTag, {}, {});

        writeColumns.push_back(std::move(writeColumn));
    }

    return writeColumns;
}

void TValidatedWriteTx::SetTxKeys(const ::google::protobuf::RepeatedField<::NProtoBuf::uint32>& columnTags, const NScheme::TTypeRegistry& typeRegistry, const TActorContext& ctx)
{
    TVector<TCell> keyCells;
    for (ui32 rowIdx = 0; rowIdx < Matrix_.GetRowCount(); ++rowIdx)
    {
        Matrix_.GetSubmatrix(rowIdx, rowIdx, 0, TableInfo_->KeyColumnIds.size() - 1, keyCells);

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Table " << TableInfo_->Path << ", shard: " << TabletId_ << ", "
                                                                 << "write point " << DebugPrintPoint(TableInfo_->KeyColumnTypes, keyCells, typeRegistry));
        TTableRange tableRange(keyCells);
        EngineBay.AddWriteRange(TableId_, tableRange, TableInfo_->KeyColumnTypes, GetColumnWrites(columnTags), false);
    }
}

ui32 TValidatedWriteTx::ExtractKeys(bool allowErrors)
{
    using EResult = NMiniKQL::IEngineFlat::EResult;

    EResult result = EngineBay.Validate();
    if (allowErrors) {
        if (result != EResult::Ok) {
            ErrStr = EngineBay.GetEngine()->GetErrors();
            ErrCode = ConvertErrCode(result);
            return 0;
        }
    } else {
        Y_ABORT_UNLESS(result == EResult::Ok, "Engine errors: %s", EngineBay.GetEngine()->GetErrors().data());
    }
    return KeysCount();
}

bool TValidatedWriteTx::ReValidateKeys()
{
    using EResult = NMiniKQL::IEngineFlat::EResult;


    auto [result, error] = EngineBay.GetKqpComputeCtx().ValidateKeys(EngineBay.TxInfo());
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
    EngineBay.DestroyEngine();
    IsReleased = true;

    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Sub(TxSize);
    ComputeTxSize();
    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Add(TxSize);
}

void TValidatedWriteTx::ComputeTxSize() {
    TxSize = sizeof(TValidatedWriteTx);
}

TWriteOperation::TWriteOperation(const TBasicOpInfo& op, NEvents::TDataEvents::TEvWrite::TPtr ev, TDataShard* self, TTransactionContext& txc, const TActorContext& ctx)
    : TOperation(op)
    , Ev_(ev)
    , ArtifactFlags(0)
    , TxCacheUsage(0)
    , ReleasedTxDataSize(0)
    , SchemeShardId(0)
    , SubDomainPathId(0)
{
    SetTarget(Ev_->Sender);
    SetCookie(Ev_->Cookie);
    Orbit = std::move(Ev_->Get()->MoveOrbit());

    BuildWriteTx(self, txc, ctx);

    // First time parsing, so we can fail
    Y_DEBUG_ABORT_UNLESS(WriteTx_->Ready());

    TrackMemory();
}

TWriteOperation::~TWriteOperation()
{
    UntrackMemory();
}

void TWriteOperation::FillTxData(TValidatedWriteTx::TPtr writeTx)
{
    Y_ABORT_UNLESS(!WriteTx_);
    Y_ABORT_UNLESS(!Ev_ || HasVolatilePrepareFlag());

    Target = writeTx->Source();
    WriteTx_ = writeTx;
}

void TWriteOperation::FillTxData(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx, const TActorId& target, NEvents::TDataEvents::TEvWrite::TPtr&& ev, const TVector<TSysTables::TLocksTable::TLock>& locks, ui64 artifactFlags)
{
    UntrackMemory();

    Y_ABORT_UNLESS(!WriteTx_);
    Y_ABORT_UNLESS(!Ev_);

    Target = target;
    Ev_ = std::move(ev);
    if (locks.size()) {
        for (auto lock : locks)
            LocksCache().Locks[lock.LockId] = lock;
    }
    ArtifactFlags = artifactFlags;
    Y_ABORT_UNLESS(!WriteTx_);
    BuildWriteTx(self, txc, ctx);
    Y_ABORT_UNLESS(WriteTx_->Ready());

    TrackMemory();
}

void TWriteOperation::FillVolatileTxData(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx)
{
    UntrackMemory();

    Y_ABORT_UNLESS(!WriteTx_);
    Y_ABORT_UNLESS(Ev_);

    BuildWriteTx(self, txc, ctx);
    Y_ABORT_UNLESS(WriteTx_->Ready());


    TrackMemory();
}

TValidatedWriteTx::TPtr TWriteOperation::BuildWriteTx(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx)
{
    if (!WriteTx_) {
        Y_ABORT_UNLESS(Ev_);
        WriteTx_ = std::make_shared<TValidatedWriteTx>(self, txc, ctx, GetStepOrder(), GetReceivedAt(), Ev_);
    }
    return WriteTx_;
}

void TWriteOperation::ReleaseTxData(NTabletFlatExecutor::TTxMemoryProviderBase& provider, const TActorContext& ctx) {
    ReleasedTxDataSize = provider.GetMemoryLimit() + provider.GetRequestedMemory();

    if (!WriteTx_ || WriteTx_->IsTxDataReleased())
        return;

    WriteTx_->ReleaseTxData();
    // Immediate transactions have no body stored.
    if (!IsImmediate() && !HasVolatilePrepareFlag()) {
        UntrackMemory();
        Ev_.Reset();
        TrackMemory();
    }

    //InReadSets.clear();
    OutReadSets().clear();
    LocksAccessLog().Locks.clear();
    LocksCache().Locks.clear();
    ArtifactFlags = 0;

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "tx " << GetTxId() << " released its data");
}

void TWriteOperation::DbStoreLocksAccessLog(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx)
{
    using Schema = TDataShard::Schema;

    NIceDb::TNiceDb db(txc.DB);

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

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Storing " << vec.size() << " locks for txid=" << GetTxId() << " in " << self->TabletID());
}

void TWriteOperation::DbStoreArtifactFlags(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx)
{
    using Schema = TDataShard::Schema;

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::TxArtifacts>().Key(GetTxId()).Update<Schema::TxArtifacts::Flags>(ArtifactFlags);

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Storing artifactflags=" << ArtifactFlags << " for txid=" << GetTxId() << " in " << self->TabletID());
}

ui64 TWriteOperation::GetMemoryConsumption() const {
    ui64 res = 0;
    if (WriteTx_) {
        res += WriteTx_->GetTxSize() + WriteTx_->GetMemoryAllocated();
    }
    if (Ev_) {
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
    if (!WriteTx_) {
        ReleasedTxDataSize = 0;
        return ERestoreDataStatus::Ok;
    }

    UntrackMemory();

    // For immediate transactions we should restore just
    // from the Ev_. For planned transaction we should
    // restore from local database.

    TVector<TSysTables::TLocksTable::TLock> locks;
    if (!IsImmediate() && !HasVolatilePrepareFlag()) {
        NIceDb::TNiceDb db(txc.DB);ExtractKeys
        bool ok = self->TransQueue.LoadTxDetails(db, GetTxId(), Target, Ev_, locks, ArtifactFlags);
        if (!ok) {
            Ev_.Reset();
            ArtifactFlags = 0;
            return ERestoreDataStatus::Restart;
        }
    } else {
        Y_ABORT_UNLESS(Ev_);
    }

    TrackMemory();

    for (auto& lock : locks)
        LocksCache().Locks[lock.LockId] = lock;

    bool extractKeys = WriteTx_->IsTxInfoLoaded();
    WriteTx_ = std::make_shared<TValidatedWriteTx>(self, txc, ctx, GetStepOrder(), GetReceivedAt(), Ev_);
    if (WriteTx_->Ready() && extractKeys) {
        WriteTx_->ExtractKeys(true);
    }

    if (!WriteTx_->Ready()) {
        return ERestoreDataStatus::Error;
    }

    ReleasedTxDataSize = 0;
    */

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "tx " << GetTxId() << " at " << self->TabletID() << " restored its data");

    return ERestoreDataStatus::Ok;
}

void TWriteOperation::FinalizeWriteTxPlan()
{
    Y_ABORT_UNLESS(IsDataTx());
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

        TWriteOperation* tx = dynamic_cast<TWriteOperation*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        tx->FinalizeWriteTxPlan();

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

void TWriteOperation::TrackMemory() const {
    // TODO More accurate calc memory
    NActors::NMemory::TLabel<MemoryLabelActiveTransactionBody>::Add(Record().SpaceUsed());
}

void TWriteOperation::UntrackMemory() const {
    NActors::NMemory::TLabel<MemoryLabelActiveTransactionBody>::Sub(Record().SpaceUsed());
}

void TWriteOperation::SetError(const NKikimrDataEvents::TEvWriteResult::EStatus& status, const TString& errorMsg) {
    SetAbortedFlag();
    WriteResult_ = NEvents::TDataEvents::TEvWriteResult::BuildError(WriteTx_->TabletId(), GetTxId(), status, errorMsg);
}

void TWriteOperation::SetWriteResult(std::unique_ptr<NEvents::TDataEvents::TEvWriteResult>&& writeResult) {
    WriteResult_ = std::move(writeResult);
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
        plan.push_back(EExecutionUnitKind::FinishPropose);
        plan.push_back(EExecutionUnitKind::CompletedOperations);
    } 
    /*
    else if (HasVolatilePrepareFlag()) {
        plan.push_back(EExecutionUnitKind::StoreDataTx);  // note: stores in memory
        plan.push_back(EExecutionUnitKind::FinishPropose);
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
            plan.push_back(EExecutionUnitKind::FinishPropose);
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
