#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/opcode_ids.h>
#else
#error "No <opcode_ids.h> in Python2"
#endif
