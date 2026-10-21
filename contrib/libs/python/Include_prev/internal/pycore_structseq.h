#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/internal/pycore_structseq.h>
#else
#error "No <internal/pycore_structseq.h> in Python2"
#endif
