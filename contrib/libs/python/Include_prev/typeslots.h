#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/typeslots.h>
#else
#error "No <typeslots.h> in Python2"
#endif
