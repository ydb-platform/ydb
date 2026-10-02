#pragma once

#ifdef USE_PYTHON3_PREV
#error "No <Python-ast.h> in Python3"
#else
#include <contrib/tools/python/src/Include/Python-ast.h>
#endif
