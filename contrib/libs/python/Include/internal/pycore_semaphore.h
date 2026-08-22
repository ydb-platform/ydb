#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_semaphore.h>
#else
#error "No <internal/pycore_semaphore.h> in Python2"
#endif
