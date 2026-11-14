#include "events.h"
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/core/tx/data_events/common/error_codes.h>
#include <ydb/library/actors/core/log.h>
#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NKikimr::NEvents {

TDataEvents::TEvWrite::TEvWrite(const ui64 txId, NKikimrDataEvents::TEvWrite::ETxMode txMode) {
    Y_ABORT_UNLESS(txMode != NKikimrDataEvents::TEvWrite::MODE_UNSPECIFIED);
    Record.SetTxMode(txMode);
    Record.SetTxId(txId);
}

TDataEvents::TEvWrite::TEvWrite(NKikimrDataEvents::TEvWrite::ETxMode txMode) {
    Y_ABORT_UNLESS(txMode != NKikimrDataEvents::TEvWrite::MODE_UNSPECIFIED);
    Record.SetTxMode(txMode);
}

std::unique_ptr<TDataEvents::TEvWriteResult> TDataEvents::TEvWriteResult::BuildError(const ui64 origin, const ui64 txId, const NKikimrDataEvents::TEvWriteResult::EStatus& status, const TString& errorMsg) {
    auto result = std::make_unique<TEvWriteResult>();
    YDB_LOG_WARN_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "",
        {"event", "ev_write_error"},
        {"status", NKikimrDataEvents::TEvWriteResult::EStatus_Name(status)},
        {"details", errorMsg},
        {"txId", txId});
    result->Record.SetOrigin(origin);
    result->Record.SetTxId(txId);
    result->Record.SetStatus(status);
    NYql::TIssue issue(errorMsg);
    if (const auto statusConclusion = NKikimr::NEvWrite::NErrorCodes::TOperator::GetStatusInfo(status); statusConclusion.IsSuccess()) {
        NYql::SetIssueCode(statusConclusion->GetIssueCode(), issue);
    }
    NYql::IssueToMessage(issue, result->Record.AddIssues());
    return result;
}

std::unique_ptr<TDataEvents::TEvWriteResult> TDataEvents::TEvWriteResult::BuildCompleted(const ui64 origin) {
    auto result = std::make_unique<TEvWriteResult>();
    result->Record.SetOrigin(origin);
    result->Record.SetStatus(NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    return result;
}

std::unique_ptr<TDataEvents::TEvWriteResult> TDataEvents::TEvWriteResult::BuildCompleted(const ui64 origin, const ui64 txId) {
    auto result = std::make_unique<TEvWriteResult>();
    result->Record.SetOrigin(origin);
    result->Record.SetTxId(txId);
    result->Record.SetStatus(NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    return result;
}

std::unique_ptr<TDataEvents::TEvWriteResult> TDataEvents::TEvWriteResult::BuildCompleted(const ui64 origin, const ui64 txId, const NKikimrDataEvents::TLock& lock) {
    auto result = std::make_unique<TEvWriteResult>();
    result->Record.SetOrigin(origin);
    result->Record.SetTxId(txId);
    result->Record.SetStatus(NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    auto& lockResult = *result->Record.AddTxLocks();
    lockResult = lock;
    lockResult.SetHasWrites(true);
    return result;
}

std::unique_ptr<TDataEvents::TEvWriteResult> TDataEvents::TEvWriteResult::BuildPrepared(const ui64 origin, const ui64 txId, const TCoordinatorInfo& transactionInfo) {
    auto result = std::make_unique<TEvWriteResult>();
    result->Record.SetOrigin(origin);
    result->Record.SetTxId(txId);
    result->Record.SetStatus(NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);

    result->Record.SetMinStep(transactionInfo.GetMinStep());
    result->Record.SetMaxStep(transactionInfo.GetMaxStep());
    result->Record.MutableDomainCoordinators()->CopyFrom(transactionInfo.GetDomainCoordinators());
    return result;
}

void TDataEvents::TEvWriteResult::AddTxLock(ui64 lockId, ui64 shard, ui32 generation, ui64 counter, ui64 ssId, ui64 pathId, bool hasWrites,
    ui64 writerIndex, ui64 writeSeqNum)
{
    auto entry = Record.AddTxLocks();
    entry->SetLockId(lockId);
    entry->SetDataShard(shard);
    entry->SetGeneration(generation);
    entry->SetCounter(counter);
    entry->SetSchemeShard(ssId);
    entry->SetPathId(pathId);
    if (hasWrites) {
        entry->SetHasWrites(true);
    }
    if (writeSeqNum) {
        auto* entryWriteSeqNum = entry->AddWriteSeqNums();
        entryWriteSeqNum->SetWriterIndex(writerIndex);
        entryWriteSeqNum->SetWriteSeqNum(writeSeqNum);
    }
}

void TDataEvents::TEvLockRows::SetTableId(const TTableId& tableId) {
    auto* p = Record.MutableTableId();
    p->SetOwnerId(tableId.PathId.OwnerId);
    p->SetTableId(tableId.PathId.LocalPathId);
    if (tableId.SchemaVersion) {
        p->SetSchemaVersion(tableId.SchemaVersion);
    }
}

TSerializedCellMatrix TDataEvents::TEvLockRows::GetCellMatrix() const {
    TSerializedCellMatrix matrix;
    TString payload = GetPayload(Record.GetPayloadIndex()).ConvertToString();
    if (!TSerializedCellMatrix::TryParse(std::move(payload), matrix)) {
        throw yexception() << "Failed to parse TSerializedCellVec payload";
    }
    return matrix;
}

TDataEvents::TEvLockRowsResult::TEvLockRowsResult(ui64 tabletId, ui64 requestId, NKikimrDataEvents::TEvLockRowsResult::EStatus status) {
    Record.SetTabletId(tabletId);
    Record.SetRequestId(requestId);
    Record.SetStatus(status);
}

TDataEvents::TEvLockRowsResult::TEvLockRowsResult(ui64 tabletId, ui64 requestId, NKikimrDataEvents::TEvLockRowsResult::EStatus status, const TString& errorMsg)
    : TEvLockRowsResult(tabletId, requestId, status)
{
    NYql::TIssue issue(errorMsg);
    NYql::IssueToMessage(issue, Record.AddIssues());
}

void TDataEvents::TEvLockRowsResult::AddLock(ui64 lockId, ui64 shard, ui32 generation, ui64 counter, ui64 ssId, ui64 pathId, bool hasWrites) {
    auto* entry = Record.AddLocks();
    entry->SetLockId(lockId);
    entry->SetDataShard(shard);
    entry->SetGeneration(generation);
    entry->SetCounter(counter);
    entry->SetSchemeShard(ssId);
    entry->SetPathId(pathId);
    if (hasWrites) {
        entry->SetHasWrites(true);
    }
}

NKikimrDataEvents::TEvWrite::TOperation& TDataEvents::TEvWrite::AddOperation(NKikimrDataEvents::TEvWrite_TOperation::EOperationType operationType,
    const TTableId& tableId, const std::vector<ui32>& columnIds,
    ui64 payloadIndex, NKikimrDataEvents::EDataFormat payloadFormat,
    const ui32 defaultFilledColumnCount) {
    Y_ABORT_UNLESS(operationType != NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UNSPECIFIED);
    Y_ABORT_UNLESS(payloadFormat != NKikimrDataEvents::FORMAT_UNSPECIFIED);

    auto operation = Record.AddOperations();
    operation->SetType(operationType);
    operation->SetPayloadFormat(payloadFormat);
    operation->SetPayloadIndex(payloadIndex);
    operation->MutableTableId()->SetOwnerId(tableId.PathId.OwnerId);
    operation->MutableTableId()->SetTableId(tableId.PathId.LocalPathId);
    operation->MutableTableId()->SetSchemaVersion(tableId.SchemaVersion);
    operation->MutableColumnIds()->Assign(columnIds.begin(), columnIds.end());
    operation->SetDefaultFilledColumnCount(defaultFilledColumnCount);
    return *operation;
}

NKikimrDataEvents::TEvWrite::TOperation& TDataEvents::TEvWrite::AddUnsafeTruncateOperation(const TTableId& tableId) {
    auto operation = Record.AddOperations();
    operation->SetType(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UNSAFE_TRUNCATE);
    operation->MutableTableId()->SetOwnerId(tableId.PathId.OwnerId);
    operation->MutableTableId()->SetTableId(tableId.PathId.LocalPathId);
    operation->MutableTableId()->SetSchemaVersion(tableId.SchemaVersion);
    return *operation;
}

TTableId TDataEvents::TEvLockRows::GetTableId() const {
    const auto& p = Record.GetTableId();
    return TTableId(p.GetOwnerId(), p.GetTableId(), p.GetSchemaVersion());
}

TString TDataEvents::TEvWriteResult::GetError() const {
    return TStringBuilder() << "Status: " << Record.GetStatus() << " Issues: " << Record.GetIssues();
}

} // namespace NKikimr::NEvents
