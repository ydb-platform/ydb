#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_pyatomic_ft_wrappers.h>
#else
#error "No <internal/pycore_pyatomic_ft_wrappers.h> in Python2"
#endif
