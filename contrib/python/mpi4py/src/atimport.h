/* Author:  Lisandro Dalcin   */
/* Contact: dalcinl@gmail.com */

/* ------------------------------------------------------------------------- */

#include "Python.h"
#include "mpi.h"

/* ------------------------------------------------------------------------- */

#include "lib-mpi/config.h"
#include "lib-mpi/missing.h"
#include "lib-mpi/fallback.h"
#include "lib-mpi/compat.h"

#include "pympivendor.h"
#include "pympistatus.h"
#include "pympicommctx.h"

/* ------------------------------------------------------------------------- */

#include "pycompat.h"

#ifdef PYPY_VERSION
  #define PyMPI_RUNTIME_PYPY    1
  #define PyMPI_RUNTIME_CPYTHON 0
#else
  #define PyMPI_RUNTIME_PYPY    0
  #define PyMPI_RUNTIME_CPYTHON 1
#endif

/* ------------------------------------------------------------------------- */

#if !defined(PyMPI_USE_MATCHED_RECV)
  #if defined(PyMPI_HAVE_MPI_Mprobe) && \
      defined(PyMPI_HAVE_MPI_Mrecv)  && \
      MPI_VERSION >= 3
    #define PyMPI_USE_MATCHED_RECV 1
  #else
    #define PyMPI_USE_MATCHED_RECV 0
  #endif
#endif

/* ------------------------------------------------------------------------- */

/*
  Local variables:
  c-basic-offset: 2
  indent-tabs-mode: nil
  End:
*/
