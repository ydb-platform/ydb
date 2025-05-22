#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/pymacro.h>
#else
#error "No <pymacro.h> in Python2"
#endif
