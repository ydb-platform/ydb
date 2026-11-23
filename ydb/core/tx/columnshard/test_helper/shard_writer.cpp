#include "shard_writer.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>

namespace NKikimr::NTxUT {

NKikimrDataEvents::TEvWriteResult TShardWriter::StartCommitImpl(const ui64 txId) {
    auto evCommit = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
    evCommit->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
    auto* lock = evCommit->Record.MutableLocks()->AddLocks();
    lock->SetLockId(LockId);
    ForwardToTablet(Runtime, TTestTxConfig::TxTablet0, Sender, evCommit.release());

    TAutoPtr<NActors::IEventHandle> handle;
    auto event = Runtime.GrabEdgeEvent<NKikimr::NEvents::TDataEvents::TEvWriteResult>(handle);
    YDB_LOG_VERIFY(event);
    YDB_LOG_VERIFY(event->Record.GetTxId() == txId);
    return event->Record;
}

void TShardWriter::StartCommitFail(const ui64 txId) {
    auto event = StartCommitImpl(txId);
    YDB_LOG_VERIFY(event.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
}

TPlanStep TShardWriter::StartCommit(const ui64 txId) {
    const auto now = Runtime.GetTimeProvider()->Now();
    auto event = StartCommitImpl(txId);
    YDB_LOG_VERIFY(event.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
    YDB_LOG_VERIFY(now.MilliSeconds() <= event.GetMinStep());
    YDB_LOG_VERIFY(event.GetMinStep() <= event.GetMaxStep());
    YDB_LOG_VERIFY(event.GetMaxStep() < Max<ui64>());
    return TPlanStep{ event.GetMinStep() };
}

NKikimrDataEvents::TEvWriteResult::EStatus TShardWriter::StartCommitWithLock(const ui64 txId, const NKikimrDataEvents::TLock& lock) {
    auto evCommit = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(txId, NKikimrDataEvents::TEvWrite::MODE_PREPARE);
    auto& locks = *evCommit->Record.MutableLocks();
    locks.SetOp(NKikimrDataEvents::TKqpLocks::Commit);
    locks.AddSendingShards(TabletId);
    locks.AddReceivingShards(TabletId);
    locks.SetArbiterColumnShard(TabletId);
    *locks.AddLocks() = lock;
    ForwardToTablet(Runtime, TabletId, Sender, evCommit.release());

    TAutoPtr<NActors::IEventHandle> handle;
    auto event = Runtime.GrabEdgeEvent<NKikimr::NEvents::TDataEvents::TEvWriteResult>(handle);
    YDB_LOG_VERIFY(event);
    YDB_LOG_VERIFY(event->Record.GetTxId() == txId);
    return event->Record.GetStatus();
}

NKikimrDataEvents::TEvWriteResult::EStatus TShardWriter::Abort() {
    auto evCommit = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
    evCommit->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Rollback);
    auto* lock = evCommit->Record.MutableLocks()->AddLocks();
    lock->SetLockId(LockId);
    ForwardToTablet(Runtime, TTestTxConfig::TxTablet0, Sender, evCommit.release());

    TAutoPtr<NActors::IEventHandle> handle;
    auto event = Runtime.GrabEdgeEvent<NKikimr::NEvents::TDataEvents::TEvWriteResult>(handle);
    YDB_LOG_VERIFY(event);

    return event->Record.GetStatus();
}

void TShardWriter::SendWrite(const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<ui32>& columnIds, const ui64 txId) {
    TString blobData = NArrow::SerializeBatchNoCompression(batch);
    //    YDB_LOG_VERIFY(blobData.size() < NColumnShard::TLimits::GetMaxBlobSize());

    auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
    evWrite->SetTxId(txId);
    evWrite->SetLockId(LockId, LockNodeId);
    const ui64 payloadIndex = NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(std::move(blobData));
    evWrite->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE, { OwnerId, PathId, SchemaVersion }, columnIds,
        payloadIndex, NKikimrDataEvents::FORMAT_ARROW);

    ForwardToTablet(Runtime, TabletId, Sender, evWrite.release());
}

NKikimrDataEvents::TEvWriteResult TShardWriter::WaitWriteResult() {
    TAutoPtr<NActors::IEventHandle> handle;
    auto event = Runtime.GrabEdgeEvent<NKikimr::NEvents::TDataEvents::TEvWriteResult>(handle);
    YDB_LOG_VERIFY(event);

    YDB_LOG_VERIFY(event->Record.GetOrigin() == TabletId);
    YDB_LOG_VERIFY(event->Record.GetTxId() == LockId);

    return event->Record;
}

}   // namespace NKikimr::NTxUT
