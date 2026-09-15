#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/interpreteridobject.h>
#else
#error "No <interpreteridobject.h> in Python2"
#endif
