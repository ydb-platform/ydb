// Override _WIN32_WINNT
#undef _WIN32_WINNT
#define _WIN32_WINNT Py_WINVER

#define Py_NO_ENABLE_SHARED

/* Define to 1 if you want to build _blake2 module with libb2 */
#define HAVE_LIBB2 1

#include "../PC/pyconfig.h.in"
