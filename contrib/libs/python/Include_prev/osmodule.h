#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/osmodule.h>
#else
#error "No <osmodule.h> in Python2"
#endif
