#include "fixed_port.h"

#include <util/system/env.h>

#include <stdlib.h>

bool NBus::NTest::IsFixedPortTestAllowed() {
    // TODO: report skipped tests to test
    return !GetEnv("MB_TESTS_SKIP_FIXED_PORT");
}
