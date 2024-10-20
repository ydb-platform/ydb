#include "status.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr {

void TConclusionStatusImplBase::AbortOnValidationProblem(const TString& errorMessage, const TString& processInfo) const {
    if (processInfo) {
        AFL_VERIFY(false)("problem", errorMessage)("process_info", processInfo);
    } else {
        AFL_VERIFY(false)("problem", errorMessage);
    }
}

}   // namespace NKikimr
