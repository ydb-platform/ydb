// Override _WIN32_WINNT
#undef _WIN32_WINNT
#define _WIN32_WINNT Py_WINVER

#define Py_NO_ENABLE_SHARED

/* Define to 1 if you want to build _blake2 module with libb2 */
#define HAVE_LIBB2 1

#if !defined(NDEBUG) && !defined(Py_LIMITED_API) && !defined(DISABLE_PYDEBUG)
#define Py_DEBUG
#define GC_NDEBUG
#define ABIFLAGS "d"
#else
#define ABIFLAGS ""
#endif

#include "../PC/pyconfig.h.in"
