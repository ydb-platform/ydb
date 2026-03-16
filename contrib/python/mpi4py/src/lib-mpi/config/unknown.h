#ifndef PyMPI_CONFIG_UNKNOWN_H
#define PyMPI_CONFIG_UNKNOWN_H

#include "mpi-11.h"
#include "mpi-12.h"
#include "mpi-20.h"
#include "mpi-22.h"
#include "mpi-30.h"
#include "mpi-31.h"
#include "mpi-40.h"

/* These types are difficult to implement portably */
#undef PyMPI_HAVE_MPI_INTEGER16
#undef PyMPI_HAVE_MPI_REAL2
#undef PyMPI_HAVE_MPI_COMPLEX4

#endif /* !PyMPI_CONFIG_UNKNOWN_H */
