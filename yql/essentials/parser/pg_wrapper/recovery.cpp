#include "pg_compat.h"

extern "C" {
#include "access/xlog.h"
#include "utils/inval.h"
}

bool RecoveryInProgress() {
    return false;
}

void AcceptInvalidationMessages() {
}
