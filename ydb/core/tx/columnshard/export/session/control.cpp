#include "control.h"
#include "session.h"

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
    exportSession->Abort();
    return TConclusionStatus::Success();
}

}
