#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_token.h>
#else
#error "No <internal/pycore_token.h> in Python2"
#endif
