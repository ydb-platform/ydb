#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_cell.h>
#else
#error "No <internal/pycore_cell.h> in Python2"
#endif
