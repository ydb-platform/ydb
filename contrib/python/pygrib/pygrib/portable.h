#ifndef _PYGRIB_PORTABLE_H_
#define _PYGRIB_PORTABLE_H_

#ifdef _WIN32

#include <io.h>
#define wrap_dup _dup
#define HAS_FMEMOPEN 0
static inline FILE *fmemopen(void *buf, size_t size, char *mode) { return NULL; }

#else

#include <unistd.h>
#define wrap_dup dup
#define HAS_FMEMOPEN 1

#endif  /* _WIN32 */

#endif  /* _PYGRIB_PORTABLE_H_ */
