#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_uop_ids.h>
#else
#error "No <internal/pycore_uop_ids.h> in Python2"
#endif
