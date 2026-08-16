#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_parking_lot.h>
#else
#error "No <internal/pycore_parking_lot.h> in Python2"
#endif
