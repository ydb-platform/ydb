#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_brc.h>
#else
#error "No <internal/pycore_brc.h> in Python2"
#endif
