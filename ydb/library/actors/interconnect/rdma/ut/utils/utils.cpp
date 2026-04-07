#include "utils.h"

#include <util/system/env.h>

namespace NRdmaTest {

const char* RdmaTestEnvSwitchName = "TEST_ICRDMA";

bool IsRdmaTestDisabled() {
    return GetEnv(RdmaTestEnvSwitchName).empty();
}

}
