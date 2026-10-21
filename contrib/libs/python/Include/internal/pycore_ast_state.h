#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_ast_state.h>
#else
#error "No <internal/pycore_ast_state.h> in Python2"
#endif
