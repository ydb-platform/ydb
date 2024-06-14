#include "status.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr {

void TConclusionStatus::Validate(const TString& processInfo) const {
    if (processInfo) {
        AFL_VERIFY(Ok())("problem", GetErrorMessage())("process_info", processInfo);
    } else {
        AFL_VERIFY(Ok())("problem", GetErrorMessage());
    }
}

}
