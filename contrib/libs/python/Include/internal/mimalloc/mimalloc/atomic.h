#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/mimalloc/mimalloc/atomic.h>
#else
#error "No <internal/mimalloc/mimalloc/atomic.h> in Python2"
#endif
