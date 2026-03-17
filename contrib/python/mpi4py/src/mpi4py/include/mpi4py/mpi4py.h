/* Author:  Lisandro Dalcin   */
/* Contact: dalcinl@gmail.com */

#ifndef MPI4PY_H
#define MPI4PY_H

#include "mpi.h"

#if defined(MSMPI_VER) && !defined(PyMPI_HAVE_MPI_Message)
#  if defined(MPI_MESSAGE_NULL)
#    define PyMPI_HAVE_MPI_Message 1
#  endif
#endif

#if (MPI_VERSION < 3) && !defined(PyMPI_HAVE_MPI_Message)
typedef void *PyMPI_MPI_Message;
#define MPI_Message PyMPI_MPI_Message
#endif

#include "mpi4py.MPI_api.h"

static int import_mpi4py(void) {
  if (import_mpi4py__MPI() < 0) goto bad;
  return 0;
 bad:
  return -1;
}

#endif /* MPI4PY_H */
