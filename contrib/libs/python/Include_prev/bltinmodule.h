#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/bltinmodule.h>
#else
#error "No <bltinmodule.h> in Python2"
#endif
