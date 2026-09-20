#ifdef USE_PYTHON3
#include <contrib/python/numpy/py3/numpy/_core/src/common/gil_utils.h>
#else
#error #include <contrib/python/numpy/py2/numpy/core/src/common/gil_utils.h>
#endif
