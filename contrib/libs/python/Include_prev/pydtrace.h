#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/pydtrace.h>
#else
#error "No <pydtrace.h> in Python2"
#endif
