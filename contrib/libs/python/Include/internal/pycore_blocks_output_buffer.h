#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_blocks_output_buffer.h>
#else
#error "No <internal/pycore_blocks_output_buffer.h> in Python2"
#endif
