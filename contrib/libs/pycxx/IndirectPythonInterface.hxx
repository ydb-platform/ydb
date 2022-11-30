#include "WrapPython.h"

#if PY_MAJOR_VERSION == 2
#include "Python2/IndirectPythonInterface.hxx"
#else
#include "Python3/IndirectPythonInterface.hxx"
#endif
