#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/bltinmodule.h>
#else
#error "No <bltinmodule.h> in Python2"
#endif
