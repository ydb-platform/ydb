#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/pystats.h>
#else
#error "No <pystats.h> in Python2"
#endif
