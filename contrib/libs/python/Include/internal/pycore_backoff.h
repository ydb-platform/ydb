#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_backoff.h>
#else
#error "No <internal/pycore_backoff.h> in Python2"
#endif
