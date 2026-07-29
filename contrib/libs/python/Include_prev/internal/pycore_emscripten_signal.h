#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/internal/pycore_emscripten_signal.h>
#else
#error "No <internal/pycore_emscripten_signal.h> in Python2"
#endif
