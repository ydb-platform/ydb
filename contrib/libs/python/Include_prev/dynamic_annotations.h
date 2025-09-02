#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/dynamic_annotations.h>
#else
#error "No <dynamic_annotations.h> in Python2"
#endif
