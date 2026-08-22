#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/fileutils.h>
#else
#error "No <fileutils.h> in Python2"
#endif
