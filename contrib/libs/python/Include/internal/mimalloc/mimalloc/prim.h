#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/mimalloc/mimalloc/prim.h>
#else
#error "No <internal/mimalloc/mimalloc/prim.h> in Python2"
#endif
