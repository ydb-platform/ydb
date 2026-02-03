#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/mimalloc/mimalloc/track.h>
#else
#error "No <internal/mimalloc/mimalloc/track.h> in Python2"
#endif
