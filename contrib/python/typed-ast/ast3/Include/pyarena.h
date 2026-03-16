/* An arena-like memory interface for the compiler.
 */

#ifndef Ta3_PYARENA_H
#define Ta3_PYARENA_H

#if PY_MINOR_VERSION >= 10
#include "../Include/pycore_pyarena.h"

#define PyArena_New _PyArena_New
#define PyArena_Free _PyArena_Free
#define PyArena_Malloc _PyArena_Malloc
#define PyArena_AddPyObject _PyArena_AddPyObject
#endif

#endif /* !Ta3_PYARENA_H */
