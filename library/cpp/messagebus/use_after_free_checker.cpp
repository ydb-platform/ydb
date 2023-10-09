#include "use_after_free_checker.h"

#include <util/system/yassert.h>

namespace {
    const ui64 VALID = (ui64)0xAABBCCDDEEFF0011LL;
    const ui64 INVALID = (ui64)0x1122334455667788LL;
}

TUseAfterFreeChecker::TUseAfterFreeChecker()
    : Magic(VALID)
{
}

TUseAfterFreeChecker::~TUseAfterFreeChecker() {
    Y_ABORT_UNLESS(Magic == VALID, "Corrupted");
    Magic = INVALID;
}

void TUseAfterFreeChecker::CheckNotFreed() const {
    Y_ABORT_UNLESS(Magic == VALID, "Freed or corrupted");
}
