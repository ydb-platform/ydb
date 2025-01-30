#include "actor.h"

namespace NKikimr::NOlap::NDataReader {

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_RESTORE)("event", "scan_data");
    LastAck = std::nullopt;
    if (!CheckActivity()) {
        TBase::Send(*ScanActorId, new NKqp::TEvKqp::TEvAbortExecution(NYql::NDqProto::StatusIds::ABORTED, "external task aborted"));
        return;
    }
    SwitchStage(EStage::WaitData, EStage::WaitData);
    auto data = ev->Get()->ArrowBatch;
    AFL_VERIFY(!!data || ev->Get()->Finished);
    if (data) {
        AFL_VERIFY(ScanActorId);
        const auto status = RestoreTask->OnDataChunk(data);
        if (status.IsSuccess()) {
            TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(FreeSpace, 1, 1), NActors::IEventHandle::FlagTrackDelivery);
            LastAck = TMonotonic::Now();
        } else {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_RESTORE)("event", "scan_data_restore_fail")("message", status.GetErrorMessage());
            SwitchStage(EStage::WaitData, EStage::Finished);
            TBase::Send(*ScanActorId, NKqp::TEvKqp::TEvAbortExecution::Aborted("task finished: " + status.GetErrorMessage()).Release());
            PassAway();
        }
    } else {
        SwitchStage(EStage::WaitData, EStage::Finished);
        auto status = RestoreTask->OnFinished();
        if (status.IsFail()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_RESTORE)("event", "restore_task_finished_error")("reason", status.GetErrorMessage());
        } else {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_RESTORE)("event", "restore_task_finished");
        }
        PassAway();
    }
}

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_RESTORE)("event", "init_actor");
    LastAck = std::nullopt;
    if (!CheckActivity()) {
        TBase::Send(*ScanActorId, new NKqp::TEvKqp::TEvAbortExecution(NYql::NDqProto::StatusIds::ABORTED, "external task aborted"));
        return;
    }
    SwitchStage(EStage::Initialization, EStage::WaitData);
    AFL_VERIFY(!ScanActorId);
    auto& msg = ev->Get()->Record;
    ScanActorId = ActorIdFromProto(msg.GetScanActorId());
    TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(FreeSpace, 1, 1), NActors::IEventHandle::FlagTrackDelivery);
    LastAck = TMonotonic::Now();
}

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanError::TPtr& ev) {
    SwitchStage(std::nullopt, EStage::Finished);
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_RESTORE)("event", "problem_on_restore_data")(
        "reason", NYql::IssuesFromMessageAsString(ev->Get()->Record.GetIssues()));
    RestoreTask->OnError(NYql::IssuesFromMessageAsString(ev->Get()->Record.GetIssues()));
    PassAway();
}

void TActor::HandleExecute(NActors::TEvents::TEvUndelivered::TPtr& ev) {
    SwitchStage(std::nullopt, EStage::Finished);
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_RESTORE)("event", "problem_on_event_undelivered")("reason", ev->Get()->Reason);
    RestoreTask->OnError("cannot delivery event: " + ::ToString(ev->Get()->Reason));
    PassAway();
}

void TActor::HandleExecute(NActors::TEvents::TEvWakeup::TPtr& /*ev*/) {
    if (!CheckActivity()) {
        TBase::Send(*ScanActorId, new NKqp::TEvKqp::TEvAbortExecution(NYql::NDqProto::StatusIds::ABORTED, "external task aborted"));
        return;
    }

    if (LastAck && TMonotonic::Now() - *LastAck > RestoreTask->GetTimeout()) {
        SwitchStage(std::nullopt, EStage::Finished);
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_RESTORE)("event", "problem_timeout");
        RestoreTask->OnError("timeout on restore data");
        TBase::Send(*ScanActorId, new NKqp::TEvKqp::TEvAbortExecution(NYql::NDqProto::StatusIds::ABORTED, "external task aborted"));
        PassAway();
        return;
    }
    Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());
}

void TActor::Bootstrap(const TActorContext& /*ctx*/) {
    if (!CheckActivity()) {
        return;
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_RESTORE)("event", "start_restore")("tablet_actor_id", RestoreTask->GetTabletActorId())(
        "this", (ui64)this);
    auto evStart = RestoreTask->BuildRequestInitiator();
    Send(RestoreTask->GetTabletActorId(), evStart.release(), NActors::IEventHandle::FlagTrackDelivery);
    LastAck = TMonotonic::Now();
    Become(&TActor::StateFunc);
    Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());
}

bool TActor::CheckActivity() {
    if (AbortedFlag) {
        return false;
    }
    if (RestoreTask->IsActive()) {
        return true;
    }
    AbortedFlag = true;
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "restoring_cancelled_from_operation");
    SwitchStage(std::nullopt, EStage::Finished);
    RestoreTask->OnError("restore task aborted through operation cancelled");
    PassAway();
    return false;
}

}   // namespace NKikimr::NOlap::NDataReader
