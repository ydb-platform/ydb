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

TValidatedWriteTx::TValidatedWriteTx(TDataShard* self, ui64 globalTxId, TInstant receivedAt, const NEvents::TDataEvents::TEvWrite& ev,
        bool mvccSnapshotRead)
    : KeyValidator(*self)
    , TabletId(self->TabletID())
    , IsImmediate(ev.Record.GetTxMode() == NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE)
    , GlobalTxId(globalTxId)
    , ReceivedAt(receivedAt)
    , MvccSnapshotRead(mvccSnapshotRead)
    , TxSize(0)
    , ErrCode(NKikimrTxDataShard::TError::OK)
    , IsReleased(false)
{
    ComputeTxSize();
    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Add(TxSize);

    const NKikimrDataEvents::TEvWrite& record = ev.Record;

    if (record.GetLockTxId()) {
        LockTxId = record.GetLockTxId();
        LockNodeId = record.GetLockNodeId();
    }

    OverloadSubscribe = record.HasOverloadSubscribe() ? record.GetOverloadSubscribe() : std::optional<ui64>{};

    NKikimrTxDataShard::TKqpTransaction::TDataTaskMeta meta;

    LOG_T("Parsing write transaction for " << globalTxId << " at " << TabletId << ", record: " << record.ShortDebugString());

    Operations.reserve(record.operations().size());
    for (const auto& recordOperation : record.operations()) {
        TValidatedWriteTxOperation validatedOperation;

        auto [errCode, errStr] = validatedOperation.ParseOperation(ev, recordOperation, self->TableInfos, TabletId, KeyValidator);
        if (errCode != NKikimrTxDataShard::TError::OK) {
            ErrCode = errCode;
            ErrStr = std::move(errStr);
            return;
        }

        Operations.push_back(std::move(validatedOperation));
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

std::tuple<NKikimrTxDataShard::TError::EKind, TString> TValidatedWriteTxOperation::ParseOperation(const NEvents::TDataEvents::TEvWrite& ev, const NKikimrDataEvents::TEvWrite::TOperation& recordOperation, const TUserTable::TTableInfos& tableInfos, ui64 tabletId, TKeyValidator& keyValidator) {
    OperationType = recordOperation.GetType();
    switch (OperationType) {
        case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT:
        case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE:
        case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE:
        case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT:
        case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE:
            break;
        default:
            return {NKikimrTxDataShard::TError::BAD_ARGUMENT, TStringBuilder() << OperationType << " operation is not supported now"};
    }

    ColumnIds = {recordOperation.GetColumnIds().begin(), recordOperation.GetColumnIds().end()};

    const NKikimrDataEvents::TTableId& tableIdRecord = recordOperation.GetTableId();

    auto tableInfoPtr = tableInfos.FindPtr(tableIdRecord.GetTableId());
    if (!tableInfoPtr)
        return {NKikimrTxDataShard::TError::SCHEME_ERROR, TStringBuilder() << "Table '" << tableIdRecord.GetTableId() << "' doesn't exist."};

    const TUserTable& tableInfo = *tableInfoPtr->Get();

    if (tableInfo.GetTableSchemaVersion() != 0 && tableIdRecord.GetSchemaVersion() != tableInfo.GetTableSchemaVersion())
        return {NKikimrTxDataShard::TError::SCHEME_CHANGED, TStringBuilder() << "Table '" << tableInfo.Path << "' scheme changed."};

    if (recordOperation.GetPayloadFormat() != NKikimrDataEvents::FORMAT_CELLVEC)
        return {NKikimrTxDataShard::TError::BAD_ARGUMENT, TStringBuilder() << "Only FORMAT_CELLVEC is supported now. Got: " << recordOperation.GetPayloadFormat()};

    ::NKikimr::NEvWrite::TPayloadReader<NEvents::TDataEvents::TEvWrite> payloadReader(ev);
    TString payload = payloadReader.GetDataFromPayload(recordOperation.GetPayloadIndex());

    if (!TSerializedCellMatrix::TryParse(payload, Matrix))
        return {NKikimrTxDataShard::TError::BAD_ARGUMENT, TStringBuilder() << "Can't parse TSerializedCellVec in payload"};

    if ((size_t)ColumnIds.size() != Matrix.GetColCount())
        return {NKikimrTxDataShard::TError::BAD_ARGUMENT, TStringBuilder() << "Column count mismatch: got columnids " << ColumnIds.size() << ", got cells count " <<Matrix.GetColCount()};

    if ((size_t)ColumnIds.size() < tableInfo.KeyColumnIds.size())
        return {NKikimrTxDataShard::TError::SCHEME_ERROR, TStringBuilder() << "Column count mismatch: got " << ColumnIds.size() << ", expected greater or equal than key column count " << tableInfo.KeyColumnIds.size()};

    for (size_t i = 0; i < tableInfo.KeyColumnIds.size(); ++i) {
        if (ColumnIds[i] != tableInfo.KeyColumnIds[i])
            return {NKikimrTxDataShard::TError::SCHEME_ERROR, TStringBuilder() << "Key column schema at position " << i};
    }

    for (ui32 columnTag : ColumnIds) {
        auto* col = tableInfo.Columns.FindPtr(columnTag);
        if (!col)
            return {NKikimrTxDataShard::TError::SCHEME_ERROR, TStringBuilder() << "Missing column with id " << columnTag};
    }

    for (ui32 rowIdx = 0; rowIdx < Matrix.GetRowCount(); ++rowIdx)
    {
        ui64 keyBytes = 0;
        for (ui16 keyColIdx = 0; keyColIdx < tableInfo.KeyColumnIds.size(); ++keyColIdx) {
            const auto& cellType = tableInfo.KeyColumnTypes[keyColIdx];
            const TCell& cell = Matrix.GetCell(rowIdx, keyColIdx);
            if (cellType.GetTypeId() == NScheme::NTypeIds::Uint8 && !cell.IsNull() && cell.AsValue<ui8>() > 127)
                return {NKikimrTxDataShard::TError::BAD_ARGUMENT, TStringBuilder() << "Keys with Uint8 column values >127 are currently prohibited"};

            keyBytes += cell.IsNull() ? 1 : cell.Size();
        }

        if (keyBytes > NLimits::MaxWriteKeySize)
            return {NKikimrTxDataShard::TError::BAD_ARGUMENT, TStringBuilder() << "Row key size of " << keyBytes << " bytes is larger than the allowed threshold " << NLimits::MaxWriteKeySize};

        for (ui16 valueColIdx = tableInfo.KeyColumnIds.size(); valueColIdx < Matrix.GetColCount(); ++valueColIdx) {
            const TCell& cell = Matrix.GetCell(rowIdx, valueColIdx);
            if (cell.Size() > NLimits::MaxWriteValueSize)
                return {NKikimrTxDataShard::TError::BAD_ARGUMENT, TStringBuilder() << "Row cell size of " << cell.Size() << " bytes is larger than the allowed threshold " << NLimits::MaxWriteValueSize};
        }
    }

    TableId = TTableId(tableIdRecord.GetOwnerId(), tableIdRecord.GetTableId(), tableIdRecord.GetSchemaVersion());

    SetTxKeys(tableInfo, tabletId, keyValidator);

    return {NKikimrTxDataShard::TError::OK, {}};
}

TVector<TKeyValidator::TColumnWriteMeta> TValidatedWriteTxOperation::GetColumnWrites() const {
    TVector<TKeyValidator::TColumnWriteMeta> writeColumns;
    writeColumns.reserve(ColumnIds.size());
    for (ui32 columnTag : ColumnIds) {
        TKeyValidator::TColumnWriteMeta writeColumn;
        writeColumn.Column = NTable::TColumn("", columnTag, {}, {});
        writeColumns.push_back(std::move(writeColumn));
    }

    return writeColumns;
}

void TValidatedWriteTxOperation::SetTxKeys(const TUserTable& tableInfo, ui64 tabletId, TKeyValidator& keyValidator)
{
    auto columnsWrites = GetColumnWrites();

    TVector<TCell> keyCells;
    for (ui32 rowIdx = 0; rowIdx < Matrix.GetRowCount(); ++rowIdx)
    {
        Matrix.GetSubmatrix(rowIdx, rowIdx, 0, tableInfo.KeyColumnIds.size() - 1, keyCells);

        LOG_T("Table " << tableInfo.Path << ", shard: " << tabletId << ", "
            << "write point " << DebugPrintPoint(tableInfo.KeyColumnTypes, keyCells, *AppData()->TypeRegistry));

        TTableRange tableRange(keyCells);
        keyValidator.AddWriteRange(TableId, tableRange, tableInfo.KeyColumnTypes, columnsWrites, false);
    }
}

ui32 TValidatedWriteTx::ExtractKeys(const NTable::TScheme& scheme, bool allowErrors)
{
    if (!HasOperations())
        return 0;

    bool isValid = ReValidateKeys(scheme);
    if (allowErrors) {
        if (!isValid) {
            return 0;
        }
    } else {
        Y_ABORT_UNLESS(isValid, "Validation errors: %s", ErrStr.data());
    }
    
    return KeysCount();
}

bool TValidatedWriteTx::ReValidateKeys(const NTable::TScheme& scheme)
{
    using EResult = NMiniKQL::IEngineFlat::EResult;

    TKeyValidator::TValidateOptions options(LockTxId, LockNodeId, MvccSnapshotRead, IsImmediate, true, scheme);
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
    Operations.clear();
    KqpLocks.reset();
    IsReleased = true;

    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Sub(TxSize);
    ComputeTxSize();
    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Add(TxSize);
}

ui64 TValidatedWriteTxOperation::ComputeTxSize() const {
    ui64 txSize = sizeof(TValidatedWriteTxOperation);
    txSize += Matrix.GetBuffer().size();
    txSize += ColumnIds.size() * sizeof(ui32);
    return txSize;
}

void TValidatedWriteTx::ComputeTxSize() {
    TxSize = sizeof(TValidatedWriteTx);

    for(const auto& validatedOperation: Operations)
        TxSize += validatedOperation.ComputeTxSize();

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

TWriteOperation* TWriteOperation::TryCastWriteOperation(TOperation::TPtr op)
{
    if (!op->IsWriteTx())
        return nullptr;
    
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

TWriteOperation::TWriteOperation(const TBasicOpInfo& op, NEvents::TDataEvents::TEvWrite::TPtr&& ev, TDataShard* self)
    : TWriteOperation(op, self->TabletID())
{
    Recipient = ev->Recipient;
    SetTarget(ev->Sender);
    SetCookie(ev->Cookie);

    TAutoPtr<TEventHandle<NEvents::TDataEvents::TEvWrite>> handle = ev.Release();
    TAutoPtr<NEvents::TDataEvents::TEvWrite> evPtr = handle->Release();

    Orbit = std::move(evPtr->MoveOrbit());
    WriteRequest.reset(evPtr.Release());

    BuildWriteTx(self);

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

void TWriteOperation::FillTxData(TDataShard* self, const TActorId& target, const TString& txBody, const TVector<TSysTables::TLocksTable::TLock>& locks, ui64 artifactFlags)
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
    BuildWriteTx(self);
    Y_ABORT_UNLESS(WriteTx->Ready());

    TrackMemory();
}

void TWriteOperation::FillVolatileTxData(TDataShard* self)
{
    UntrackMemory();

    Y_ABORT_UNLESS(!WriteTx);
    Y_ABORT_UNLESS(WriteRequest);

    BuildWriteTx(self);
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

TValidatedWriteTx::TPtr TWriteOperation::BuildWriteTx(TDataShard* self)
{
    if (!WriteTx) {
        Y_ABORT_UNLESS(WriteRequest);
        WriteTx = std::make_shared<TValidatedWriteTx>(self, GetGlobalTxId(), GetReceivedAt(), *WriteRequest, IsMvccSnapshotRead());
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

    LOG_D("tx " << GetTxId() << " at " << TabletId << " released its data");
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

    LOG_T("Storing " << vec.size() << " locks for txid=" << GetTxId() << " at " << TabletId);
}

void TWriteOperation::DbStoreArtifactFlags(NTable::TDatabase& txcDb)
{
    using Schema = TDataShard::Schema;

    NIceDb::TNiceDb db(txcDb);
    db.Table<Schema::TxArtifacts>().Key(GetTxId()).Update<Schema::TxArtifacts::Flags>(ArtifactFlags);

    LOG_T("Storing artifactflags=" << ArtifactFlags << " for txid=" << GetTxId() << " at " << TabletId);
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

ERestoreDataStatus TWriteOperation::RestoreTxData(TDataShard* self, NTable::TDatabase& db)
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
        NIceDb::TNiceDb niceDb(db);

        TString txBody;
        bool ok = self->TransQueue.LoadTxDetails(niceDb, GetTxId(), Target, txBody, locks, ArtifactFlags);
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

    WriteTx = std::make_shared<TValidatedWriteTx>(self, GetTxId(), GetReceivedAt(), *WriteRequest, IsMvccSnapshotRead());
    if (WriteTx->Ready() && extractKeys) {
        WriteTx->ExtractKeys(db.GetScheme(), true);
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
    NActors::NMemory::TLabel<MemoryLabelActiveTransactionBody>::Add(WriteRequest ? WriteRequest->CalculateSerializedSize() : 0);
}

void TWriteOperation::UntrackMemory() const {
    NActors::NMemory::TLabel<MemoryLabelActiveTransactionBody>::Sub(WriteRequest ? WriteRequest->CalculateSerializedSize() : 0);
}

void TWriteOperation::SetError(const NKikimrDataEvents::TEvWriteResult::EStatus& status, const TString& errorMsg) {
    SetAbortedFlag();
    WriteResult = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletId, GetTxId(), status, errorMsg);
    LOG_I("Write transaction " << GetTxId() << " at " << TabletId << " has an error: " << errorMsg);
}

void TWriteOperation::SetWriteResult(std::unique_ptr<NEvents::TDataEvents::TEvWriteResult>&& writeResult) {
    WriteResult = std::move(writeResult);
}

}  // NDataShard
}  // NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::NDataShard::TWriteOperation, stream, tx) {
    stream << '[' << tx.GetStep() << ':' << tx.GetTxId() << ']';
}
