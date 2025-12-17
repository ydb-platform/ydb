#include "error.h"

namespace NKikimr::NSQS {

void MakeError(NSQS::TError* error, const TErrorClass& errorClass, const TString& message) {
    error->SetErrorCode(errorClass.ErrorCode);
    error->SetStatus(errorClass.HttpStatusCode);
    if (!message.empty()) {
        error->SetMessage(message);
    } else {
        error->SetMessage(errorClass.DefaultMessage);
    }
}

} // namespace NKikimr::NSQS
