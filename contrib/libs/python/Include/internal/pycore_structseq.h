#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_structseq.h>
#else
#error "No <internal/pycore_structseq.h> in Python2"
#endif
