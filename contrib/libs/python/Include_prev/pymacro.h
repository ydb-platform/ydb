#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/pymacro.h>
#else
#error "No <pymacro.h> in Python2"
#endif
