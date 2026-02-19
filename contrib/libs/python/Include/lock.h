#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/lock.h>
#else
#error "No <lock.h> in Python2"
#endif
