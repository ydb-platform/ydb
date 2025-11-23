#include "session.h"

#include <ydb/core/tx/columnshard/backup/import/import_actor.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/adapter.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap::NImport {

NKikimr::TConclusion<std::unique_ptr<NActors::IActor>> TSession::DoCreateActor(const NBackground::TStartContext& context) const {
    AFL_VERIFY(IsConfirmed());
    Status = EStatus::Started;
    return std::make_unique<TImportActor>(context.GetSessionSelfPtr(), context.GetAdapter());
}

void TSession::Finish() {
    AFL_VERIFY(Status == EStatus::Started);
    Status = EStatus::Finished;
}

const TInternalPathId TSession::GetInternalPathId() const {
    return Task->GetInternalPathId();
}

const TImportTask &TSession::GetTask() const { 
    return *Task; 
}

bool TSession::IsStarted() const { 
    return Status == EStatus::Started; 
}

void TSession::Abort() {
    AFL_VERIFY(Status != EStatus::Finished && Status != EStatus::Aborted);
    Status = EStatus::Aborted;
}

void TSession::Confirm() {
    AFL_VERIFY(IsDraft());
    Status = EStatus::Confirmed;
}

bool TSession::IsDraft() const { 
    return Status == EStatus::Draft; 
}

TString TSession::DebugString() const {
    return TStringBuilder() << "task=" << Task->DebugString()
                            << ";status=" << Status;
}

bool TSession::IsConfirmed() const { 
    return Status == EStatus::Confirmed; 
}

TSession::TSession(const std::shared_ptr<TImportTask> &task) : Task(task) {
    AFL_VERIFY(Task);
}

TString TSession::GetClassName() const { 
    return GetClassNameStatic(); 
}

bool TSession::IsReadyForRemoveOnFinished() const {
  return Status == EStatus::Aborted;
}

bool TSession::IsFinished() const { 
    return Status == EStatus::Finished; 
}

bool TSession::IsReadyForStart() const { 
    return Status == EStatus::Confirmed; 
}

std::optional<ui64> TSession::GetTxId() const { 
    return Task->GetTxId(); 
}

TSession::TProtoLogic TSession::DoSerializeToProto() const {
    TProtoLogic result;
    *result.MutableTask() = Task->SerializeToProto();
    return result;
}

TConclusionStatus TSession::DoDeserializeFromProto(const TProtoLogic &proto) {
    Task = std::make_shared<TImportTask>();
    return Task->DeserializeFromProto(proto.GetTask());
}

TSession::TProtoState TSession::DoSerializeStateToProto() const {
    TProtoState result;
    result.SetStatus(::ToString(Status));
    return result;
}

TConclusionStatus TSession::DoDeserializeStateFromProto(const TProtoState &proto) {
    if (!TryFromString(proto.GetStatus(), Status)) {
    return TConclusionStatus::Fail("cannot read status from proto: " +
                                    proto.GetStatus());
    }
    return TConclusionStatus::Success();
}

TSession::TProtoProgress TSession::DoSerializeProgressToProto() const {
    return NProtoBuf::Empty{};
}

TConclusionStatus TSession::DoDeserializeProgressFromProto(const TProtoProgress & /* proto */) {
    return TConclusionStatus::Success();
}

TString TSession::GetClassNameStatic() { 
    return "CS::EXPORT"; 
}

} // namespace NKikimr::NOlap::NImport
