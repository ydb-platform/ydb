#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_emscripten_signal.h>
#else
#error "No <internal/pycore_emscripten_signal.h> in Python2"
#endif
