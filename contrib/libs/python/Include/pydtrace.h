#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/pydtrace.h>
#else
#error "No <pydtrace.h> in Python2"
#endif
