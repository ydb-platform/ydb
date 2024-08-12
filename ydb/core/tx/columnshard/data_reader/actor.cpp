#include "actor.h"

namespace NKikimr::NOlap::NDataReader {

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev) {
    SwitchStage(EStage::WaitData, EStage::WaitData);
    auto data = ev->Get()->ArrowBatch;
    AFL_VERIFY(!!data || ev->Get()->Finished);
    if (data) {
        AFL_VERIFY(ScanActorId);
        const auto status = RestoreTask->OnDataChunk(data);
        if (status.IsSuccess()) {
            TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(FreeSpace, 1, 1));
        } else {
            SwitchStage(EStage::WaitData, EStage::Finished);
            TBase::Send(*ScanActorId, NKqp::TEvKqp::TEvAbortExecution::Aborted("task finished: " + status.GetErrorMessage()).Release());
        }
    } else {
        SwitchStage(EStage::WaitData, EStage::Finished);
        auto status = RestoreTask->OnFinished();
        if (status.IsFail()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "restore_task_finished_error")("reason", status.GetErrorMessage());
        } else {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "restore_task_finished")("reason", status.GetErrorMessage());
        }
    }
}

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
    SwitchStage(EStage::Initialization, EStage::WaitData);
    AFL_VERIFY(!ScanActorId);
    auto& msg = ev->Get()->Record;
    ScanActorId = ActorIdFromProto(msg.GetScanActorId());
    TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(FreeSpace, 1, 1));
}

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanError::TPtr& ev) {
    AFL_VERIFY(false)("error", NYql::IssuesFromMessageAsString(ev->Get()->Record.GetIssues()));
}

void TActor::Bootstrap(const TActorContext& /*ctx*/) {
    auto evStart = RestoreTask->BuildRequestInitiator();
    Send(RestoreTask->GetTabletActorId(), evStart.release());
    Become(&TActor::StateFunc);
}

}