#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/osmodule.h>
#else
#error "No <osmodule.h> in Python2"
#endif
