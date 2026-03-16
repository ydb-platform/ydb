#ifndef DUMMY_Py_PGENHEADERS_H
#define DUMMY_Py_PGENHEADERS_H

/* pgenheaders.h is included by a bunch of files but nothing in it is
 * used except for the Python.h import, and it was removed in Python
 * 3.8. Since some of those files are generated we provide a dummy
 * pgenheaders.h. */
#include "Python.h"

#endif /* !DUMMY_Py_PGENHEADERS_H */
