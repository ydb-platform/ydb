#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/mimalloc/mimalloc.h>
#else
#error "No <internal/mimalloc/mimalloc.h> in Python2"
#endif
