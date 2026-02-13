#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/monitoring.h>
#else
#error "No <monitoring.h> in Python2"
#endif
