#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/critical_section.h>
#else
#error "No <critical_section.h> in Python2"
#endif
