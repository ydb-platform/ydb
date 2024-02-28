#include "defs.h"

#include "datashard_write_operation.h"
#include "datashard_kqp.h"
#include <ydb/core/tx/locks/locks.h>
#include "datashard_impl.h"
#include "datashard_failpoints.h"

#include "key_conflicts.h"
#include "range_ops.h"

#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <ydb/library/actors/util/memory_track.h>

#if defined LOG_T || \
    defined LOG_D || \
    defined LOG_I || \
    defined LOG_N || \
    defined LOG_W || \
    defined LOG_E || \
    defined LOG_C
    #error log macro redefinition
#endif

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)

namespace NKikimr {
namespace NDataShard {

TValidatedWriteTx::TValidatedWriteTx(TDataShard* self, TTransactionContext& txc, ui64 globalTxId, TInstant receivedAt, const NEvents::TDataEvents::TEvWrite& ev)
    : UserDb(*self, txc.DB, globalTxId, TRowVersion::Min(), TRowVersion::Max(), EngineHostCounters, TAppData::TimeProvider->Now())
    , KeyValidator(*self, txc.DB)
    , TabletId(self->TabletID())
    , ReceivedAt(receivedAt)
    , TxSize(0)
    , ErrCode(NKikimrTxDataShard::TError::OK)
    , IsReleased(false)
{
    ComputeTxSize();
    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Add(TxSize);

    UserDb.SetIsWriteTx(true);

    const NKikimrDataEvents::TEvWrite& record = ev.Record;

    if (record.GetLockTxId()) {
        UserDb.SetLockTxId(record.GetLockTxId());
        UserDb.SetLockNodeId(record.GetLockNodeId());
    }

    if (record.GetTxMode() == NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE)
        UserDb.SetIsImmediateTx(true);

    NKikimrTxDataShard::TKqpTransaction::TDataTaskMeta meta;

    LOG_T("Parsing write transaction for " << globalTxId << " at " << TabletId << ", record: " << record.ShortDebugString());

    if (record.operations().size() != 0) {
        Y_ABORT_UNLESS(record.operations().size() == 1, "Only one operation is supported now");
        Y_ABORT_UNLESS(record.operations(0).GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, "Only UPSERT operation is supported now");
        const NKikimrDataEvents::TEvWrite::TOperation& recordOperation = record.operations(0);

        ColumnIds = {recordOperation.GetColumnIds().begin(), recordOperation.GetColumnIds().end()};

        if (!ParseOperation(ev, recordOperation, self->TableInfos))
            return;
    }

    if (record.HasLocks()) {
        KqpLocks = record.GetLocks();
        KqpSetTxLocksKeys(record.GetLocks(), self->SysLocksTable(), KeyValidator);
    }
    KeyValidator.GetInfo().SetLoaded();
}

TValidatedWriteTx::~TValidatedWriteTx() {
    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Sub(TxSize);
}

bool TValidatedWriteTx::ParseOperation(const NEvents::TDataEvents::TEvWrite& ev, const NKikimrDataEvents::TEvWrite::TOperation& recordOperation, const TUserTable::TTableInfos& tableInfos) {
    const NKikimrDataEvents::TTableId& tableIdRecord = recordOperation.GetTableId();

    auto tableInfoPtr = tableInfos.FindPtr(tableIdRecord.GetTableId());
    if (!tableInfoPtr) {
        ErrCode = NKikimrTxDataShard::TError::SCHEME_ERROR;
        ErrStr = TStringBuilder() << "Table '" << tableIdRecord.GetTableId() << "' doesn't exist.";
        return false;
    }

    const TUserTable& tableInfo = *tableInfoPtr->Get();

    if (tableInfo.GetTableSchemaVersion() != 0 && tableIdRecord.GetSchemaVersion() != tableInfo.GetTableSchemaVersion())
    {
        ErrCode = NKikimrTxDataShard::TError::SCHEME_CHANGED;
        ErrStr = TStringBuilder() << "Table '" << tableInfo.Path << "' scheme changed.";
        return false;
    }

    if (recordOperation.GetPayloadFormat() != NKikimrDataEvents::FORMAT_CELLVEC)
    {
        ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
        ErrStr = TStringBuilder() << "Only FORMAT_CELLVEC is supported now. Got: " << recordOperation.GetPayloadFormat();
        return false;
    }

    ::NKikimr::NEvWrite::TPayloadReader<NEvents::TDataEvents::TEvWrite> payloadReader(ev);
    TString payload = payloadReader.GetDataFromPayload(recordOperation.GetPayloadIndex());

    if (!TSerializedCellMatrix::TryParse(payload, Matrix))
    {
        ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
        ErrStr = TStringBuilder() << "Can't parse TSerializedCellVec in payload";
        return false;
    }

    if ((size_t)ColumnIds.size() != Matrix.GetColCount())
    {
        ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
        ErrStr = TStringBuilder() << "Column count mismatch: got columnids " << ColumnIds.size() << ", got cells count " <<Matrix.GetColCount();
        return false;
    }

    if ((size_t)ColumnIds.size() < tableInfo.KeyColumnIds.size())
    {
        ErrCode = NKikimrTxDataShard::TError::SCHEME_ERROR;
        ErrStr = TStringBuilder() << "Column count mismatch: got " << ColumnIds.size() << ", expected greater or equal than key column count " << tableInfo.KeyColumnIds.size();
        return false;
    }

    for (size_t i = 0; i < tableInfo.KeyColumnIds.size(); ++i) {
        if (ColumnIds[i] != tableInfo.KeyColumnIds[i]) {
            ErrCode = NKikimrTxDataShard::TError::SCHEME_ERROR;
            ErrStr = TStringBuilder() << "Key column schema at position " << i;
            return false;
        }
    }

    for (ui32 columnTag : ColumnIds) {
        auto* col = tableInfo.Columns.FindPtr(columnTag);
        if (!col) {
            ErrCode = NKikimrTxDataShard::TError::SCHEME_ERROR;
            ErrStr = TStringBuilder() << "Missing column with id " << columnTag;
            return false;
        }
    }

    for (ui32 rowIdx = 0; rowIdx < Matrix.GetRowCount(); ++rowIdx)
    {
        ui64 keyBytes = 0;
        for (ui16 keyColIdx = 0; keyColIdx < tableInfo.KeyColumnIds.size(); ++keyColIdx) {
            const auto& cellType = tableInfo.KeyColumnTypes[keyColIdx];
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

        for (ui16 valueColIdx = tableInfo.KeyColumnIds.size(); valueColIdx < Matrix.GetColCount(); ++valueColIdx) {
            const TCell& cell = Matrix.GetCell(rowIdx, valueColIdx);
            if (cell.Size() > NLimits::MaxWriteValueSize) {
                ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
                ErrStr = TStringBuilder() << "Row cell size of " << cell.Size() << " bytes is larger than the allowed threshold " << NLimits::MaxWriteValueSize;
                return false;
            }
        }
    }    

    TableId = TTableId(tableIdRecord.GetOwnerId(), tableIdRecord.GetTableId(), tableIdRecord.GetSchemaVersion());

    SetTxKeys(tableInfo);

    return true;
}

TVector<TKeyValidator::TColumnWriteMeta> TValidatedWriteTx::GetColumnWrites() const {
    TVector<TKeyValidator::TColumnWriteMeta> writeColumns;
    writeColumns.reserve(ColumnIds.size());
    for (ui32 columnTag : ColumnIds) {
        TKeyValidator::TColumnWriteMeta writeColumn;
        writeColumn.Column = NTable::TColumn("", columnTag, {}, {});
        writeColumns.push_back(std::move(writeColumn));
    }

    return writeColumns;
}

void TValidatedWriteTx::SetTxKeys(const TUserTable& tableInfo)
{
    auto columnsWrites = GetColumnWrites();

    TVector<TCell> keyCells;
    for (ui32 rowIdx = 0; rowIdx < Matrix.GetRowCount(); ++rowIdx)
    {
        Matrix.GetSubmatrix(rowIdx, rowIdx, 0, tableInfo.KeyColumnIds.size() - 1, keyCells);

        LOG_T("Table " << tableInfo.Path << ", shard: " << TabletId << ", "
            << "write point " << DebugPrintPoint(tableInfo.KeyColumnTypes, keyCells, *AppData()->TypeRegistry));

        TTableRange tableRange(keyCells);
        KeyValidator.AddWriteRange(TableId, tableRange, tableInfo.KeyColumnTypes, columnsWrites, false);
    }
}

ui32 TValidatedWriteTx::ExtractKeys(bool allowErrors)
{
    if (!HasOperations())
        return 0;

    bool isValid = ReValidateKeys();
    if (allowErrors) {
        if (!isValid) {
            return 0;
        }
    } else {
        Y_ABORT_UNLESS(isValid, "Validation errors: %s", ErrStr.data());
    }
    
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
    Matrix.ReleaseBuffer();
    ColumnIds.clear();
    KqpLocks.reset();
    IsReleased = true;

    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Sub(TxSize);
    ComputeTxSize();
    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Add(TxSize);
}

void TValidatedWriteTx::ComputeTxSize() {
    TxSize = sizeof(TValidatedWriteTx);
    TxSize += Matrix.GetBuffer().size();
    TxSize += ColumnIds.size() * sizeof(ui32);

    if (KqpLocks)
        TxSize += KqpLocks->ByteSize();
}

TWriteOperation* TWriteOperation::CastWriteOperation(TOperation::TPtr op)
{
    Y_ABORT_UNLESS(op->IsWriteTx());
    TWriteOperation* writeOp = dynamic_cast<TWriteOperation*>(op.Get());
    Y_ABORT_UNLESS(writeOp);
    return writeOp;
}

TWriteOperation::TWriteOperation(const TBasicOpInfo& op, ui64 tabletId)
    : TOperation(op)
    , TabletId(tabletId)
    , ArtifactFlags(0)
    , TxCacheUsage(0)
    , ReleasedTxDataSize(0)
    , SchemeShardId(0)
    , SubDomainPathId(0)
{
    TrackMemory();
}

TWriteOperation::TWriteOperation(const TBasicOpInfo& op, NEvents::TDataEvents::TEvWrite::TPtr&& ev, TDataShard* self, TTransactionContext& txc)
    : TWriteOperation(op, self->TabletID())
{
    SetTarget(ev->Sender);
    SetCookie(ev->Cookie);

    TAutoPtr<TEventHandle<NEvents::TDataEvents::TEvWrite>> handle = ev.Release();
    TAutoPtr<NEvents::TDataEvents::TEvWrite> evPtr = handle->Release();

    Orbit = std::move(evPtr->MoveOrbit());
    WriteRequest.reset(evPtr.Release());

    BuildWriteTx(self, txc);

    TrackMemory();
}

TWriteOperation::~TWriteOperation()
{
    UntrackMemory();
}

void TWriteOperation::FillTxData(TValidatedWriteTx::TPtr writeTx)
{
    Y_ABORT_UNLESS(!WriteTx);
    Y_ABORT_UNLESS(!WriteRequest || HasVolatilePrepareFlag());

    Target = writeTx->GetSource();
    WriteTx = writeTx;
}

void TWriteOperation::FillTxData(TDataShard* self, TTransactionContext& txc, const TActorId& target, const TString& txBody, const TVector<TSysTables::TLocksTable::TLock>& locks, ui64 artifactFlags)
{
    UntrackMemory();

    Y_ABORT_UNLESS(!WriteTx);
    Y_ABORT_UNLESS(!WriteRequest);

    Target = target;
    SetTxBody(txBody);

    if (locks.size()) {
        for (auto lock : locks)
            LocksCache().Locks[lock.LockId] = lock;
    }
    ArtifactFlags = artifactFlags;
    Y_ABORT_UNLESS(!WriteTx);
    BuildWriteTx(self, txc);
    Y_ABORT_UNLESS(WriteTx->Ready());

    TrackMemory();
}

void TWriteOperation::FillVolatileTxData(TDataShard* self, TTransactionContext& txc)
{
    UntrackMemory();

    Y_ABORT_UNLESS(!WriteTx);
    Y_ABORT_UNLESS(WriteRequest);

    BuildWriteTx(self, txc);
    Y_ABORT_UNLESS(WriteTx->Ready());


    TrackMemory();
}

TString TWriteOperation::GetTxBody() const {
    Y_ABORT_UNLESS(WriteRequest);

    TAllocChunkSerializer serializer;
    bool success = WriteRequest->SerializeToArcadiaStream(&serializer);
    Y_ABORT_UNLESS(success);
    TEventSerializationInfo serializationInfo = WriteRequest->CreateSerializationInfo();

    NKikimrTxDataShard::TSerializedEvent proto;
    proto.SetIsExtendedFormat(serializationInfo.IsExtendedFormat);
    proto.SetEventData(serializer.Release(std::move(serializationInfo))->GetString());

    TString str;
    success = proto.SerializeToString(&str);
    Y_ABORT_UNLESS(success);
    return str;
}

void TWriteOperation::SetTxBody(const TString& txBody) {
    Y_ABORT_UNLESS(!WriteRequest);

    NKikimrTxDataShard::TSerializedEvent proto;
    const bool success = proto.ParseFromString(txBody);
    Y_ABORT_UNLESS(success);

    TEventSerializationInfo serializationInfo;
    serializationInfo.IsExtendedFormat = proto.GetIsExtendedFormat();

    TEventSerializedData buffer(proto.GetEventData(), std::move(serializationInfo));
    NKikimr::NEvents::TDataEvents::TEvWrite* writeRequest = static_cast<NKikimr::NEvents::TDataEvents::TEvWrite*>(NKikimr::NEvents::TDataEvents::TEvWrite::Load(&buffer));
    Y_ABORT_UNLESS(writeRequest);

    WriteRequest.reset(writeRequest);
}

void TWriteOperation::ClearTxBody() {
    UntrackMemory();
    WriteRequest.reset();
    TrackMemory();
}

TValidatedWriteTx::TPtr TWriteOperation::BuildWriteTx(TDataShard* self, TTransactionContext& txc)
{
    if (!WriteTx) {
        Y_ABORT_UNLESS(WriteRequest);
        WriteTx = std::make_shared<TValidatedWriteTx>(self, txc, GetGlobalTxId(), GetReceivedAt(), *WriteRequest);
    }
    return WriteTx;
}

void TWriteOperation::ReleaseTxData(NTabletFlatExecutor::TTxMemoryProviderBase& provider) {
    ReleasedTxDataSize = provider.GetMemoryLimit() + provider.GetRequestedMemory();

    if (!WriteTx || IsTxDataReleased())
        return;

    WriteTx->ReleaseTxData();
    // Immediate transactions have no body stored.
    if (!IsImmediate() && !HasVolatilePrepareFlag()) {
        ClearTxBody();
    }

    //InReadSets.clear();
    OutReadSets().clear();
    LocksAccessLog().Locks.clear();
    LocksCache().Locks.clear();
    ArtifactFlags = 0;

    LOG_D("tx " << GetTxId() << " released its data");
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

    LOG_T("Storing " << vec.size() << " locks for txid=" << GetTxId() << " in " << TabletId);
}

void TWriteOperation::DbStoreArtifactFlags(NTable::TDatabase& txcDb)
{
    using Schema = TDataShard::Schema;

    NIceDb::TNiceDb db(txcDb);
    db.Table<Schema::TxArtifacts>().Key(GetTxId()).Update<Schema::TxArtifacts::Flags>(ArtifactFlags);

    LOG_T("Storing artifactflags=" << ArtifactFlags << " for txid=" << GetTxId() << " in " << TabletId);
}

ui64 TWriteOperation::GetMemoryConsumption() const {
    ui64 res = 0;
    if (WriteTx) {
        res += WriteTx->GetTxSize();
    }
    if (WriteRequest) {
        res += WriteRequest->CalculateSerializedSize();
    }
    return res;
}

ERestoreDataStatus TWriteOperation::RestoreTxData(TDataShard* self, TTransactionContext& txc)
{
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

        TString txBody;
        bool ok = self->TransQueue.LoadTxDetails(db, GetTxId(), Target, txBody, locks, ArtifactFlags);
        if (!ok) {
            WriteRequest.reset();
            ArtifactFlags = 0;
            return ERestoreDataStatus::Restart;
        }

        SetTxBody(txBody);
    } else {
        Y_ABORT_UNLESS(WriteRequest);
    }

    TrackMemory();

    for (auto& lock : locks)
        LocksCache().Locks[lock.LockId] = lock;

    bool extractKeys = WriteTx->IsTxInfoLoaded();

    WriteTx = std::make_shared<TValidatedWriteTx>(self, txc, GetTxId(), GetReceivedAt(), *WriteRequest);
    if (WriteTx->Ready() && extractKeys) {
        WriteTx->ExtractKeys(true);
    }

    if (!WriteTx->Ready()) {
        return ERestoreDataStatus::Error;
    }

    ReleasedTxDataSize = 0;

    LOG_D("tx " << GetTxId() << " at " << self->TabletID() << " restored its data");

    return ERestoreDataStatus::Ok;
}

void TWriteOperation::BuildExecutionPlan(bool loaded)
{
    Y_ABORT_UNLESS(GetExecutionPlan().empty());

    TVector<EExecutionUnitKind> plan;

    if (IsImmediate()) 
    {
        Y_ABORT_UNLESS(!loaded);
        plan.push_back(EExecutionUnitKind::CheckWrite);
        plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
        plan.push_back(EExecutionUnitKind::ExecuteWrite);
        plan.push_back(EExecutionUnitKind::FinishProposeWrite);
        plan.push_back(EExecutionUnitKind::CompletedOperations);
    } else if (HasVolatilePrepareFlag()) {
        Y_ABORT_UNLESS(!loaded);
        plan.push_back(EExecutionUnitKind::CheckWrite);
        plan.push_back(EExecutionUnitKind::StoreWrite);  // note: stores in memory
        plan.push_back(EExecutionUnitKind::FinishProposeWrite);
        Y_ABORT_UNLESS(!GetStep());
        plan.push_back(EExecutionUnitKind::WaitForPlan);
        plan.push_back(EExecutionUnitKind::PlanQueue);
        plan.push_back(EExecutionUnitKind::LoadWriteDetails);  // note: reloads from memory
        plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
        // Note: execute will also prepare and send readsets
        plan.push_back(EExecutionUnitKind::ExecuteWrite);
        // Note: it is important that plan here is the same as regular
        // distributed tx, since normal tx may decide to commit in a
        // volatile manner with dependencies, to avoid waiting for
        // locked keys to resolve.
        plan.push_back(EExecutionUnitKind::CompleteWrite);
        plan.push_back(EExecutionUnitKind::CompletedOperations);
    } else {
        if (!loaded) {
            plan.push_back(EExecutionUnitKind::CheckWrite);
            plan.push_back(EExecutionUnitKind::StoreWrite);
            plan.push_back(EExecutionUnitKind::FinishProposeWrite);
        }
        if (!GetStep())
            plan.push_back(EExecutionUnitKind::WaitForPlan);
        plan.push_back(EExecutionUnitKind::PlanQueue);
        plan.push_back(EExecutionUnitKind::LoadWriteDetails);

        plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);

        plan.push_back(EExecutionUnitKind::BuildWriteOutRS);
        plan.push_back(EExecutionUnitKind::StoreAndSendWriteOutRS);
        plan.push_back(EExecutionUnitKind::PrepareWriteTxInRS);
        plan.push_back(EExecutionUnitKind::LoadAndWaitInRS);
        plan.push_back(EExecutionUnitKind::ExecuteWrite);

        plan.push_back(EExecutionUnitKind::CompleteWrite);
        plan.push_back(EExecutionUnitKind::CompletedOperations);
    }
    RewriteExecutionPlan(plan);
}

bool TWriteOperation::OnStopping(TDataShard& self, const TActorContext& ctx) {
    if (IsImmediate()) {
        // Send reject result immediately, because we cannot control when
        // a new datashard tablet may start and block us from commiting
        // anything new. The usual progress queue is too slow for that.
        if (!HasResultSentFlag() && !GetWriteResult()) {
            auto rejectStatus = NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED;
            TString rejectReason = TStringBuilder()
                                   << "Rejecting immediate write tx "
                                   << GetTxId()
                                   << " because datashard "
                                   << TabletId
                                   << " is restarting";

            auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletId, GetTxId(), rejectStatus, rejectReason);
            LOG_N(rejectReason);

            ctx.Send(GetTarget(), result.release(), 0, GetCookie());

            self.IncCounter(COUNTER_WRITE_OVERLOADED);
            self.IncCounter(COUNTER_WRITE_COMPLETE);
            SetResultSentFlag();
        }

        // Immediate ops become ready when stopping flag is set
        return true;
    } else {
        // Distributed operations send notification when proposed
        if (GetTarget() && !HasCompletedFlag()) {
            auto notify = MakeHolder<TEvDataShard::TEvProposeTransactionRestart>(self.TabletID(), GetTxId());
            ctx.Send(GetTarget(), notify.Release(), 0, GetCookie());
        }

        // Distributed ops avoid doing new work when stopping
        return false;
    }
}

void TWriteOperation::TrackMemory() const {
    NActors::NMemory::TLabel<MemoryLabelActiveTransactionBody>::Add(WriteRequest ? WriteRequest->CalculateSerializedSize() : 0);
}

void TWriteOperation::UntrackMemory() const {
    NActors::NMemory::TLabel<MemoryLabelActiveTransactionBody>::Sub(WriteRequest ? WriteRequest->CalculateSerializedSize() : 0);
}

void TWriteOperation::SetError(const NKikimrDataEvents::TEvWriteResult::EStatus& status, const TString& errorMsg) {
    SetAbortedFlag();
    WriteResult = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletId, GetTxId(), status, errorMsg);
}

void TWriteOperation::SetWriteResult(std::unique_ptr<NEvents::TDataEvents::TEvWriteResult>&& writeResult) {
    WriteResult = std::move(writeResult);
}

}  // NDataShard
}  // NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::NDataShard::TWriteOperation, stream, tx) {
    stream << '[' << tx.GetStep() << ':' << tx.GetTxId() << ']';
}
