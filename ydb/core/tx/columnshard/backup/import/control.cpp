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
    exportSession->Abort();
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

TConclusionStatus TConfirmSessionControl::DoDeserializeFromProto(const NKikimrColumnShardExportProto::TSessionControlContainer & /*proto*/) {
    return TConclusionStatus::Success();
}

NKikimrColumnShardExportProto::TSessionControlContainer TConfirmSessionControl::DoSerializeToProto() const {
    NKikimrColumnShardExportProto::TSessionControlContainer result;
    return result;
}

TConclusionStatus TAbortSessionControl::DoDeserializeFromProto(const NKikimrColumnShardExportProto::TSessionControlContainer & /*proto*/) {
    return TConclusionStatus::Success();
}

NKikimrColumnShardExportProto::TSessionControlContainer TAbortSessionControl::DoSerializeToProto() const {
    NKikimrColumnShardExportProto::TSessionControlContainer result;
    return result;
}

} // namespace NKikimr::NOlap::NImport
