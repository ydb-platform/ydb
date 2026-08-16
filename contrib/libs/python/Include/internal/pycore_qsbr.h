#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_qsbr.h>
#else
#error "No <internal/pycore_qsbr.h> in Python2"
#endif
