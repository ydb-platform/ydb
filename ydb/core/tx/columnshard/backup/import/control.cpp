#include "control.h"
#include "session.h"

namespace NKikimr::NOlap::NImport {

NKikimr::TConclusionStatus TConfirmSessionControl::DoApply(const std::shared_ptr<NBackground::ISessionLogic>& session) const {
    auto exportSession = dynamic_pointer_cast<TSession>(session);
    AFL_VERIFY(exportSession);
    exportSession->Confirm();
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TAbortSessionControl::DoApply(const std::shared_ptr<NBackground::ISessionLogic>& session) const {
    auto exportSession = dynamic_pointer_cast<TSession>(session);
    AFL_VERIFY(exportSession);
    if (exportSession->IsFinished() || exportSession->IsReadyForRemoveOnFinished()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "abort_control_skipped_terminal_session")("is_finished", exportSession->IsFinished())(
            "is_aborted", exportSession->IsReadyForRemoveOnFinished());
        return TConclusionStatus::Success();
    }
    exportSession->Abort("Aborted by user");
    return TConclusionStatus::Success();
}

TString TConfirmSessionControl::GetClassNameStatic() {
    return "CS::IMPORT::CONFIRM";
}

TString TConfirmSessionControl::GetClassName() const {
    return GetClassNameStatic();
}

TString TAbortSessionControl::GetClassName() const {
    return GetClassNameStatic();
}

TString TAbortSessionControl::GetClassNameStatic() {
    return "CS::IMPORT::ABORT";
}

TConclusionStatus TConfirmSessionControl::DoDeserializeFromProto(const NKikimrColumnShardExportProto::TSessionControlContainer& /*proto*/) {
    return TConclusionStatus::Success();
}

NKikimrColumnShardExportProto::TSessionControlContainer TConfirmSessionControl::DoSerializeToProto() const {
    NKikimrColumnShardExportProto::TSessionControlContainer result;
    return result;
}

TConclusionStatus TAbortSessionControl::DoDeserializeFromProto(const NKikimrColumnShardExportProto::TSessionControlContainer& /*proto*/) {
    return TConclusionStatus::Success();
}

NKikimrColumnShardExportProto::TSessionControlContainer TAbortSessionControl::DoSerializeToProto() const {
    NKikimrColumnShardExportProto::TSessionControlContainer result;
    return result;
}

}   // namespace NKikimr::NOlap::NImport
