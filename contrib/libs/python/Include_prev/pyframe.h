#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/pyframe.h>
#else
#error "No <pyframe.h> in Python2"
#endif
