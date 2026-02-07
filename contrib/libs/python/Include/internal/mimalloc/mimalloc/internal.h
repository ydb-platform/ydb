#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/mimalloc/mimalloc/internal.h>
#else
#error "No <internal/mimalloc/mimalloc/internal.h> in Python2"
#endif
