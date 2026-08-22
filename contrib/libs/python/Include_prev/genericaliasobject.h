#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/genericaliasobject.h>
#else
#error "No <genericaliasobject.h> in Python2"
#endif
