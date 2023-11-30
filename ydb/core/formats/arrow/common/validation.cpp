#include "validation.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NArrow {

void TStatusValidator::Validate(const arrow::Status& status) {
    AFL_VERIFY(status.ok())("problem", status.ToString().c_str());
}

}
