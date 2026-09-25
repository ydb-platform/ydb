#include "control.h"
#include "session.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap::NExport {

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
        YDB_LOG_WARN("",
            {"event", "abort_control_skipped_terminal_session"},
            {"isFinished", exportSession->IsFinished()},
            {"isAborted", exportSession->IsReadyForRemoveOnFinished()});
        return TConclusionStatus::Success();
    }
    exportSession->Abort("Aborted by user");
    return TConclusionStatus::Success();
}

TString TAbortSessionControl::GetClassName() const {
    return GetClassNameStatic();
}

NKikimrColumnShardExportProto::TSessionControlContainer TAbortSessionControl::DoSerializeToProto() const {
    NKikimrColumnShardExportProto::TSessionControlContainer result;
    return result;
}

TString TAbortSessionControl::GetClassNameStatic() {
    return "CS::EXPORT::ABORT";
}

TString TConfirmSessionControl::GetClassName() const {
    return GetClassNameStatic();
}

NKikimrColumnShardExportProto::TSessionControlContainer TConfirmSessionControl::DoSerializeToProto() const {
    NKikimrColumnShardExportProto::TSessionControlContainer result;
    return result;
}

TConclusionStatus TConfirmSessionControl::DoDeserializeFromProto(const NKikimrColumnShardExportProto::TSessionControlContainer& /*proto*/) {
    return TConclusionStatus::Success();
}

TString TConfirmSessionControl::GetClassNameStatic() {
    return "CS::EXPORT::CONFIRM";
}

TConclusionStatus TAbortSessionControl::DoDeserializeFromProto(const NKikimrColumnShardExportProto::TSessionControlContainer& /*proto*/) {
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NOlap::NExport
