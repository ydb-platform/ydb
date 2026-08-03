#ifdef USE_PYTHON3
#include <contrib/python/numpy/py3/numpy/_core/src/highway/hwy/contrib/thread_pool/futex.h>
#else
#error #include <contrib/python/numpy/py2/numpy/core/src/highway/hwy/contrib/thread_pool/futex.h>
#endif
