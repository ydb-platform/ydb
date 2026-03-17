
#ifndef Ta27_COMPILE_H
#define Ta27_COMPILE_H

#include "Python.h"

#ifdef __cplusplus
extern "C" {
#endif

#if PY_VERSION_HEX < 0x030d0000
/* Public interface */
PyAPI_FUNC(PyFutureFeatures *) PyFuture_FromAST(struct _mod *, const char *);
#endif

#ifdef __cplusplus
}
#endif
#endif /* !Ta27_COMPILE_H */
