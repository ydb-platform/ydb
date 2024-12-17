#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/pystats.h>
#else
#error "No <pystats.h> in Python2"
#endif
