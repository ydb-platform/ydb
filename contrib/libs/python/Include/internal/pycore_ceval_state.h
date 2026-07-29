#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_ceval_state.h>
#else
#error "No <internal/pycore_ceval_state.h> in Python2"
#endif
