#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/pyframe.h>
#else
#error "No <pyframe.h> in Python2"
#endif
