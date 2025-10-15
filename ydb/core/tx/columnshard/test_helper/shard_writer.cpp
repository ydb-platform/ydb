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
    AFL_VERIFY(event);
    AFL_VERIFY(event->Record.GetTxId() == txId);
    return event->Record;
}

void TShardWriter::StartCommitFail(const ui64 txId) {
    auto event = StartCommitImpl(txId);
    AFL_VERIFY(event.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
}

TPlanStep TShardWriter::StartCommit(const ui64 txId) {
    const auto now = Runtime.GetTimeProvider()->Now();
    auto event = StartCommitImpl(txId);
    AFL_VERIFY(event.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
    AFL_VERIFY(now.MilliSeconds() <= event.GetMinStep());
    AFL_VERIFY(event.GetMinStep() <= event.GetMaxStep());
    AFL_VERIFY(event.GetMaxStep() < Max<ui64>());
    return TPlanStep{event.GetMinStep()};
}

NKikimrDataEvents::TEvWriteResult::EStatus TShardWriter::Abort() {
    auto evCommit = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
    evCommit->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Rollback);
    auto* lock = evCommit->Record.MutableLocks()->AddLocks();
    lock->SetLockId(LockId);
    ForwardToTablet(Runtime, TTestTxConfig::TxTablet0, Sender, evCommit.release());

    TAutoPtr<NActors::IEventHandle> handle;
    auto event = Runtime.GrabEdgeEvent<NKikimr::NEvents::TDataEvents::TEvWriteResult>(handle);
    AFL_VERIFY(event);

    return event->Record.GetStatus();
}

NKikimrDataEvents::TEvWriteResult::EStatus TShardWriter::Write(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<ui32>& columnIds, const ui64 txId) {
    TString blobData = NArrow::SerializeBatchNoCompression(batch);
//    AFL_VERIFY(blobData.size() < NColumnShard::TLimits::GetMaxBlobSize());

    auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
    evWrite->SetTxId(txId);
    evWrite->SetLockId(LockId, LockNodeId);
    const ui64 payloadIndex = NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(std::move(blobData));
    evWrite->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE, { OwnerId, PathId, SchemaVersion }, columnIds,
        payloadIndex, NKikimrDataEvents::FORMAT_ARROW);

    ForwardToTablet(Runtime, TabletId, Sender, evWrite.release());

    TAutoPtr<NActors::IEventHandle> handle;
    auto event = Runtime.GrabEdgeEvent<NKikimr::NEvents::TDataEvents::TEvWriteResult>(handle);
    AFL_VERIFY(event);

    AFL_VERIFY(event->Record.GetOrigin() == TabletId);
    AFL_VERIFY(event->Record.GetTxId() == LockId);

    return event->Record.GetStatus();
}

}   // namespace NKikimr::NTxUT
