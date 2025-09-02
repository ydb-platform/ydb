#pragma once

#include <util/system/getpid.h>

namespace NYql {

bool IsProcessAlive(TProcessId pid);

}
