#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_instruction_sequence.h>
#else
#error "No <internal/pycore_instruction_sequence.h> in Python2"
#endif
