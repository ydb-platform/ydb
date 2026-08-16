#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_opcode_metadata.h>
#else
#error "No <internal/pycore_opcode_metadata.h> in Python2"
#endif
