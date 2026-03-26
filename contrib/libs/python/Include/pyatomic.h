#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/pyatomic.h>
#else
#error "No <pyatomic.h> in Python2"
#endif
